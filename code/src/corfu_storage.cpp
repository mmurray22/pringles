#include "corfu_storage.h"
#include "structs.h"
#include "spdlog/spdlog.h"
#include <chrono>
#include <algorithm>
#include <cstring>

// const uint64_t MAX_WAIT_TIME = 100;

CorfuStorage::CorfuStorage(uint64_t ssid, std::string input_file)
    : ssid(ssid), s_epoch(0), mark(0), terminate(false)
{
    YAML::Node config = YAML::LoadFile(input_file);

    this->cli_ips = get_cli_ip(config);
    this->max_num_threads = get_num_client_threads(config);

    std::string multicast_ip = "";
    this->stor = StorageType(0); // NOTE: this is hardcoded to be the KV store!!

    net = std::make_shared<Network>(
        std::to_string(get_send_port(config)), 
        get_stor_recv_port(config),
		get_socket_type(config),
        get_log_level(config),
		get_batch_size(config),
		get_batch_on(config),
		get_batch_timeout(config),
	    get_interface(config),
		get_self_ip(config),
		multicast_ip,
		false,
		false);

    this->send_port = get_recv_port(config);
    this->max_duration = get_experiment_duration(config);

    terminate = false;
    storage_thread = std::thread(&CorfuStorage::server, this);
}

CorfuStorage::~CorfuStorage() {
    // terminate = true;
    // if (storage_thread.joinable()) {
    //     storage_thread.join();
    // }
    // spdlog::debug("Storage thread with ssid {} joined!", ssid);
}

void CorfuStorage::error_sealed(int cid, int thread_id, std::string req_type) {
    auto err_packet = corfu_storage_serialize_str_entry("", CORFU_SEALED_PROTO_TYPE, 0);
    uint64_t allocated_packet_size = err_packet->length() + 1;
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    memcpy(packet.get(), err_packet->c_str(), allocated_packet_size);
    packet[err_packet->length()] = '\0';

    spdlog::info("storage {} returning err_sealed from cid {}'s {} req", ssid, cid, req_type);

    net->send_client_udp_packet(
        std::move(packet), 
        allocated_packet_size, 
        cli_ips[cid],
        std::to_string(send_port + cid * max_num_threads + thread_id)
    );
}

void CorfuStorage::error_deleted(int cid, int thread_id, std::string req_type) {
    auto err_packet = corfu_storage_serialize_str_entry("", CORFU_DELETED_PROTO_TYPE, 0);
    uint64_t allocated_packet_size = err_packet->length() + 1;
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    memcpy(packet.get(), err_packet->c_str(), allocated_packet_size);
    packet[err_packet->length()] = '\0';

    spdlog::info("storage {} returning err_deleted from cid {}'s {} req", ssid, cid, req_type);
    net->send_client_udp_packet(
        std::move(packet), 
        allocated_packet_size, 
        cli_ips[cid],
        std::to_string(send_port + cid * max_num_threads + thread_id)
    );
}

void CorfuStorage::send_ack(int cid, int thread_id, std::string req_type) {
    auto ack_packet = corfu_storage_serialize_str_entry("", CORFU_ACK_PROTO_TYPE, 0);
    uint64_t allocated_packet_size = ack_packet->length() + 1;
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    memcpy(packet.get(), ack_packet->c_str(), allocated_packet_size);
    packet[ack_packet->length()] = '\0';

    spdlog::info("storage {} returning ack from cid {}'s {} req", ssid, cid, req_type);
    net->send_client_udp_packet(
        std::move(packet), 
        allocated_packet_size, 
        cli_ips[cid],
        std::to_string(send_port + cid * max_num_threads + thread_id)
    );
}


bool CorfuStorage::store(uint64_t idx, std::string entry) {
    switch (stor)
    {
	case StorageType::MEM_KV: 
	{
        std::pair<std::string, bool> to_insert = std::pair<std::string, bool>(entry, false);
	    std::unique_lock<std::mutex> lock(kv_store_lock);
	    kv_store.insert(std::pair<uint64_t, std::pair<std::string, bool>>(idx, to_insert));
	    return true;
	};	    
	default:
	{
	    spdlog::critical("Storage type is not supported!");
	};
    }
    return false;
}

std::pair<std::string, bool> CorfuStorage::get_entry(uint64_t idx) {
    std::pair<std::string, bool> default_pair = std::pair<std::string, bool>("", true);
    switch (stor)
    {
	case StorageType::MEM_KV: 
	{
	    return kv_store[idx]; // TODO error checking
	};	    
	default:
	{
	    spdlog::critical("Storage type is not supported!");
	};
    }
    return default_pair;
}

void CorfuStorage::write(corfuclient::Payload msg) {
    // if epoch != s_epoch, respond <err_sealed>
    int curr_epoch = msg.append().currepoch();
    int cid = msg.clientid();
    int thread_id = msg.threadid();
    if (curr_epoch != s_epoch) {
        error_sealed(cid, thread_id, "append");
        return;
    }
    
    // otherwise check the address status
    // check if deleted:
    uint64_t idx = msg.append().idx();
    auto it = kv_store.find(idx);

    if (it != kv_store.end()) { // is the entry already in the map?
        if (it->second.second) { // has it been marked deleted?
            // send <err_deleted>
            error_deleted(cid, thread_id, "append");
            return;
        } else { // if not marked deleted, then it must be written to already so we send back err + written contents
            // send <err_written>
            auto err_packet = corfu_storage_serialize_str_entry(it->second.first, CORFU_WRITTEN_PROTO_TYPE, 0);
            uint64_t allocated_packet_size = err_packet->length() + 1;
            std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
            memcpy(packet.get(), err_packet->c_str(), allocated_packet_size);
            packet[err_packet->length()] = '\0';

            spdlog::info("storage {} returning err_written from cid {}'s append req", ssid, cid);
            net->send_client_udp_packet(
                std::move(packet), 
                allocated_packet_size, 
                cli_ips[cid],
                std::to_string(send_port + cid * max_num_threads + thread_id)
            );
        }
        return;
    }
    // otherwise, the address is available so we write the content to the local store
    std::string entry = msg.append().entry();
    store(idx, entry);

    mark = std::max(mark, idx);

    // reply with ack
    send_ack(cid, thread_id, "append");
}

void CorfuStorage::read(corfuclient::Payload msg) {
    // if epoch != s_epoch, respond <err_sealed>
    int curr_epoch = msg.read().currepoch();
    int cid = msg.clientid();
    int thread_id = msg.threadid();
    if (curr_epoch != s_epoch) {
        error_sealed(cid, thread_id, "read");
        return;
    }

    // otherwise check the address status
    uint64_t idx = msg.read().idx();
    auto it = kv_store.find(idx);

    // if unwritten, respond <err_unwritten> (entry is not in the map)
    if (it == kv_store.end()) { // is the entry not already in the map?
        auto err_packet = corfu_storage_serialize_str_entry("", CORFU_UNWRITTEN_PROTO_TYPE, 0);
        uint64_t allocated_packet_size = err_packet->length() + 1;
        std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
        memcpy(packet.get(), err_packet->c_str(), allocated_packet_size);
        packet[err_packet->length()] = '\0';

        spdlog::info("storage {} returning err_unwritten from cid {}'s read req", ssid, cid);
        net->send_client_udp_packet(
            std::move(packet), 
            allocated_packet_size, 
            cli_ips[cid],
            std::to_string(send_port + cid * max_num_threads + thread_id)
        );
        return;
    }

    // if written, respond <pg_content>
    std::pair<std::string, bool> entry = get_entry(idx);
    if (entry.second) {
        // send <err_deleted>
        error_deleted(cid, thread_id, "read");
        return;
    }
    
    auto read_packet = corfu_storage_serialize_str_entry(entry.first, CORFU_STORE_READ_PROTO_TYPE, 0);
    uint64_t allocated_packet_size = read_packet->length() + 1;
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    memcpy(packet.get(), read_packet->c_str(), allocated_packet_size);
    packet[read_packet->length()] = '\0';

    spdlog::info("storage {} returning contents from cid {}'s read req at idx {}", ssid, cid, idx);
    net->send_client_udp_packet(
        std::move(packet), 
        allocated_packet_size, 
        cli_ips[cid],
        std::to_string(send_port + cid * max_num_threads + thread_id)
    );
}

void CorfuStorage::storage_delete(corfuclient::Payload msg) {
    int cid = msg.clientid();
    int thread_id = msg.threadid();
    
    uint64_t idx = msg.trim().idx();
    auto it = kv_store.find(idx);

    if (it != kv_store.end()) { // is the entry already in the map?
        it->second.second = true;
        send_ack(cid, thread_id, "trim");
        return;
    }

    // Corfu Spec does not add anything for error checking this function
}

void CorfuStorage::seal(corfuclient::Payload msg) {
    // begin seal with setting new epoch value
    if (msg.seal().currepoch() > s_epoch) {
        s_epoch = msg.seal().currepoch();
    }

    // // respond <sealed, highaddr> with highaddr = highest locally stored page address
    // auto sealed_packet = corfu_storage_serialize_str_entry("", CORFU_STORE_SEAL_PROTO_TYPE, mark);
    // uint64_t allocated_packet_size = sealed_packet->length() + 1;
    // std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    // memcpy(packet.get(), sealed_packet->c_str(), allocated_packet_size);
    // packet[sealed_packet->length()] = '\0';
    // int cid = msg.clientid();

    // spdlog::info("storage {} returning sealed from cid {}'s seal req", ssid, cid);
    // net->send_client_udp_packet(
    //     std::move(packet), 
    //     allocated_packet_size, 
    //     static_cast<int>(StoragePacketType::store_seal),
    //     ETH_CLI_SEQ, 
    //     cli_ips[cid],
    //     std::to_string(send_port + cid)
    // );
}

void CorfuStorage::server() {
    spdlog::info("CorfuStorage server running (ssid={})", ssid);

    // uint64_t wait_time = 10;
    std::unique_ptr<std::string> rcv_str;

    while (!terminate.load()) {
        char* recv_ptr = net->recv_packet();
        if (!recv_ptr) {
            continue;
        }

        auto rcv_str = std::make_unique<std::string>(recv_ptr);
        corfuclient::Payload msg = corfu_client_deserialize_str_entry(std::move(rcv_str));
        int cid = msg.clientid();
        int thread_id = msg.threadid();

        switch (msg.packet_type()) {
            case CORFU_APPEND_PROTO_TYPE:
                spdlog::info("storage {} got an append req from client {}:{}", ssid, cid, thread_id);
                write(msg);
                break;
            case CORFU_READ_PROTO_TYPE:
                spdlog::info("storage {} got a read req from client {}:{}", ssid, cid, thread_id);
                read(msg);
                break;
            case CORFU_TRIM_PROTO_TYPE:
                spdlog::info("storage {} got a trim req from client {}:{}", ssid, cid, thread_id);
                storage_delete(msg);
                break;
            case CORFU_SEAL_PROTO_TYPE:
                spdlog::info("storage {} got a seal req from client {}:{}", ssid, cid, thread_id);
                seal(msg);
                break;
            default:
                spdlog::warn("Unknown packet type received");
        }
    }
}

std::string CorfuStorage::get(uint64_t /*idx*/) {
    return "";
}

void CorfuStorage::wait_to_finish() {
    std::chrono::seconds sleep_duration(max_duration);
    std::this_thread::sleep_for(sleep_duration);
    end_thread = true;
}