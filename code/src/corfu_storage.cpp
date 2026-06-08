#include "corfu_storage.h"
#include "structs.h"
#include "spdlog/spdlog.h"
#include <chrono>
#include <algorithm>
#include <cstring>

// const uint64_t MAX_WAIT_TIME = 100;

//overall TODO: remove redundancies (ex. cid, error checking dupes, etc.)

CorfuStorage::CorfuStorage(uint64_t ssid, std::string input_file)
    : ssid(ssid), s_epoch(0), mark(0), storage_map(), terminate(false)
{
    YAML::Node config = YAML::LoadFile(input_file);
    this->num_pkt_types = get_num_pkt_types(config);

    this->cli_ips = get_cli_ips(config);

    bool run_threads = false;
    net = std::make_shared<Network>(
        std::to_string(get_send_port(config)), 
        std::to_string(get_recv_port(config)),
        get_socket_type(config),
        get_log_level(config),
        get_batch_size(config),
        get_batch_on(config),
        get_batch_timeout(config),
        get_interface(config),
        get_self_ip(config),
        num_pkt_types,
        run_threads
    );

    this->send_port = std::to_string(get_send_port(config));

    terminate = false;
    storage_thread = std::thread(&CorfuStorage::server, this);
}

CorfuStorage::~CorfuStorage() {
    terminate = true;
    if (storage_thread.joinable()) {
        storage_thread.join();
    }
    spdlog::debug("Storage thread with ssid {} joined!", ssid);
}

void CorfuStorage::write(corfuclient::Payload msg) {
    // if epoch != s_epoch, respond <err_sealed>
    int curr_epoch = msg.append().currepoch();
    if (curr_epoch != s_epoch) {
        auto err_packet = corfu_storage_serialize_str_entry("", CORFU_SEALED_PROTO_TYPE, 0);
        uint64_t allocated_packet_size = err_packet->length() + 1;
        std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
        memcpy(packet.get(), err_packet->c_str(), allocated_packet_size);
        packet[err_packet->length()] = '\0';

        int cid = msg.clientid();

        spdlog::info("storage {} returning err_sealed from cid {}'s append req", ssid, cid);
        net->send_client_udp_packet(
            std::move(packet), 
            allocated_packet_size, 
            static_cast<int>(StorageType::err_sealed),
            ETH_CLI_SEQ, 
            cli_ips[cid],
            send_port
        );
        return;
    }
    
    // otherwise check the address status
    // check if deleted:
    uint64_t idx = msg.append().idx();
    auto it = storage_map.find(idx);

    if (it != storage_map.end()) { // is the entry already in the map?
        if (it->second.deleted) { // has it been marked deleted?
            // send <err_deleted>
            auto err_packet = corfu_storage_serialize_str_entry("", CORFU_DELETED_PROTO_TYPE, 0);
            uint64_t allocated_packet_size = err_packet->length() + 1;
            std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
            memcpy(packet.get(), err_packet->c_str(), allocated_packet_size);
            packet[err_packet->length()] = '\0';

            int cid = msg.clientid();

            spdlog::info("storage {} returning err_deleted from cid {}'s append req", ssid, cid);
            net->send_client_udp_packet(
                std::move(packet), 
                allocated_packet_size, 
                static_cast<int>(StorageType::err_deleted),
                ETH_CLI_SEQ, 
                cli_ips[cid],
                send_port
            );
        } else { // if not marked deleted, then it must be written to already so we send back err + written contents
            // send <err_written>
            auto err_packet = corfu_storage_serialize_str_entry(it->second.contents, CORFU_WRITTEN_PROTO_TYPE, 0);
            uint64_t allocated_packet_size = err_packet->length() + 1;
            std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
            memcpy(packet.get(), err_packet->c_str(), allocated_packet_size);
            packet[err_packet->length()] = '\0';

            int cid = msg.clientid();

            spdlog::info("storage {} returning err_written from cid {}'s append req", ssid, cid);
            net->send_client_udp_packet(
                std::move(packet), 
                allocated_packet_size, 
                static_cast<int>(StorageType::err_written),
                ETH_CLI_SEQ, 
                cli_ips[cid],
                send_port
            );
        }
        return;
    }
    // otherwise, the address is available so we write the content to the local store
    storage_map[idx] = {false, msg.append().entry()};
    mark = std::max(mark, idx);

    // reply with ack
    auto ack_packet = corfu_storage_serialize_str_entry("", CORFU_ACK_PROTO_TYPE, 0);
    uint64_t allocated_packet_size = ack_packet->length() + 1;
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    memcpy(packet.get(), ack_packet->c_str(), allocated_packet_size);
    packet[ack_packet->length()] = '\0';
    int cid = msg.clientid();

    spdlog::info("storage {} returning ack from cid {}'s append req", ssid, cid);
    net->send_client_udp_packet(
        std::move(packet), 
        allocated_packet_size, 
        static_cast<int>(StorageType::ack),
        ETH_CLI_SEQ, 
        cli_ips[cid],
        send_port
    );
}

void CorfuStorage::read(corfuclient::Payload msg) {
    // if epoch != s_epoch, respond <err_sealed>
    int curr_epoch = msg.read().currepoch();
    if (curr_epoch != s_epoch) {
        auto err_packet = corfu_storage_serialize_str_entry("", CORFU_SEALED_PROTO_TYPE, 0);
        uint64_t allocated_packet_size = err_packet->length() + 1;
        std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
        memcpy(packet.get(), err_packet->c_str(), allocated_packet_size);
        packet[err_packet->length()] = '\0';

        int cid = msg.clientid();

        spdlog::info("storage {} returning err_sealed from cid {}'s read req", ssid, cid);
        net->send_client_udp_packet(
            std::move(packet), 
            allocated_packet_size, 
            static_cast<int>(StorageType::err_sealed),
            ETH_CLI_SEQ, 
            cli_ips[cid],
            send_port
        );
        return;
    }

    // otherwise check the address status
    uint64_t idx = msg.read().idx();
    auto it = storage_map.find(idx);

    // if unwritten, respond <err_unwritten> (entry is not in the map)
    if (it == storage_map.end()) { // is the entry not already in the map?
        auto err_packet = corfu_storage_serialize_str_entry("", CORFU_UNWRITTEN_PROTO_TYPE, 0);
        uint64_t allocated_packet_size = err_packet->length() + 1;
        std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
        memcpy(packet.get(), err_packet->c_str(), allocated_packet_size);
        packet[err_packet->length()] = '\0';

        int cid = msg.clientid();

        spdlog::info("storage {} returning err_unwritten from cid {}'s read req", ssid, cid);
        net->send_client_udp_packet(
            std::move(packet), 
            allocated_packet_size, 
            static_cast<int>(StorageType::err_unwritten),
            ETH_CLI_SEQ, 
            cli_ips[cid],
            send_port
        );
        return;
    }

    // if deleted, respond <err_deleted> (deleted bit in map set)
    if (it->second.deleted) {
        // send <err_deleted>
        auto err_packet = corfu_storage_serialize_str_entry("", CORFU_DELETED_PROTO_TYPE, 0);
        uint64_t allocated_packet_size = err_packet->length() + 1;
        std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
        memcpy(packet.get(), err_packet->c_str(), allocated_packet_size);
        packet[err_packet->length()] = '\0';

        int cid = msg.clientid();
        
        spdlog::info("storage {} returning err_deleted from cid {}'s read req", ssid, cid);
        net->send_client_udp_packet(
            std::move(packet), 
            allocated_packet_size, 
            static_cast<int>(StorageType::err_deleted),
            ETH_CLI_SEQ, 
            cli_ips[cid],
            send_port
        );
        return;
    }

    // if written, respond <pg_content>
    auto read_packet = corfu_storage_serialize_str_entry(it->second.contents, CORFU_STORE_READ_PROTO_TYPE, 0);
    uint64_t allocated_packet_size = read_packet->length() + 1;
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    memcpy(packet.get(), read_packet->c_str(), allocated_packet_size);
    packet[read_packet->length()] = '\0';

    int cid = msg.clientid();

    spdlog::info("storage {} returning contents from cid {}'s read req", ssid, cid);
    net->send_client_udp_packet(
        std::move(packet), 
        allocated_packet_size, 
        static_cast<int>(StorageType::store_read),
        ETH_CLI_SEQ, 
        cli_ips[cid],
        send_port
    );
}

void CorfuStorage::storage_delete(corfuclient::Payload msg) {
    // mark addr deleted
    uint64_t idx = msg.trim().idx();
    auto &entry = storage_map[idx];
    entry.deleted = true;
    entry.contents.clear();

    // reply with ack
    auto ack_packet = corfu_storage_serialize_str_entry("", CORFU_ACK_PROTO_TYPE, 0);
    uint64_t allocated_packet_size = ack_packet->length() + 1;
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    memcpy(packet.get(), ack_packet->c_str(), allocated_packet_size);
    packet[ack_packet->length()] = '\0';
    int cid = msg.clientid();

    spdlog::info("storage {} returning ack from cid {}'s trim req", ssid, cid);
    net->send_client_udp_packet(
        std::move(packet), 
        allocated_packet_size, 
        static_cast<int>(StorageType::ack),
        ETH_CLI_SEQ, 
        cli_ips[cid],
        send_port
    );
}

void CorfuStorage::seal(corfuclient::Payload msg) {
    // begin seal with setting new epoch value
    if (msg.seal().currepoch() > s_epoch) {
        s_epoch = msg.seal().currepoch();
    }

    // respond <sealed, highaddr> with highaddr = highest locally stored page address
    auto sealed_packet = corfu_storage_serialize_str_entry("", CORFU_STORE_SEAL_PROTO_TYPE, mark);
    uint64_t allocated_packet_size = sealed_packet->length() + 1;
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    memcpy(packet.get(), sealed_packet->c_str(), allocated_packet_size);
    packet[sealed_packet->length()] = '\0';
    int cid = msg.clientid();

    spdlog::info("storage {} returning sealed from cid {}'s seal req", ssid, cid);
    net->send_client_udp_packet(
        std::move(packet), 
        allocated_packet_size, 
        static_cast<int>(StorageType::store_seal),
        ETH_CLI_SEQ, 
        cli_ips[cid],
        send_port
    );
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

        switch (msg.packet_type()) {
            case CORFU_APPEND_PROTO_TYPE:
                spdlog::info("storage {} got an append req from client {}", ssid, cid);
                write(msg);
                break;
            case CORFU_READ_PROTO_TYPE:
                read(msg);
                break;
            case CORFU_TRIM_PROTO_TYPE:
                storage_delete(msg);
                break;
            case CORFU_SEAL_PROTO_TYPE:
                seal(msg);
                break;
            default:
                spdlog::warn("Unknown packet type received");
        }
    }
}

bool CorfuStorage::store(uint64_t /*idx*/, std::string /*entry*/) {
    return true;
}

std::string CorfuStorage::get(uint64_t /*idx*/) {
    return "";
}