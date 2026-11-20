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

    this->send_port = get_recv_port(config);
    this->max_duration = get_experiment_duration(config);
    this->just_storage = get_just_storage(config);

    curr_idx.store(0);

    terminate = false;

    int append_req_port = 60008;
    this->num_append_req_threads = get_store_append_req_threads(config);
    spdlog::debug("append req threads: {}", num_append_req_threads);
    for (uint64_t i = 0; i < num_append_req_threads; i++) {
        std::unique_ptr<Network> append_net = std::make_unique<Network>( 
            get_socket_type(config),
            get_log_level(config),
            get_batch_size(config), 
            get_batch_on(config),
            get_batch_timeout(config),
            get_interface(config),
            get_self_ip(config));
        int random_socket = append_net->create_random_port_socket();
        if (random_socket < 0) {
            append_net->stop_batch_threads();
            return;
        }
	    append_socket.push_back(random_socket);
        append_req_threads.emplace_back(std::thread(&CorfuStorage::server, this, append_req_port, std::move(append_net), i));
    }
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
    if (stor == StorageType::MEM_KV) {
        KVMap::accessor acc;
        if (just_storage) {
            idx = curr_idx.load();
            curr_idx.fetch_add(1);
            kv_store.insert(acc, idx);
            acc->second = std::make_pair(entry, false);
            return true;
        } else {
            if (kv_store.insert(acc, std::make_pair(idx, std::make_pair(entry, false)))) {
                return true;
            }
        }
    } else {
        spdlog::critical("Storage type is not supported!");
    }
    return false;
}

std::pair<std::string, bool> CorfuStorage::get_entry(uint64_t idx) {
    KVMap::const_accessor acc;
    if (kv_store.find(acc, idx)) {
        return acc->second;
    }
    return std::make_pair("", true);
}

void CorfuStorage::write() {
    // if (curr_epoch != s_epoch) {
    //     error_sealed(cid, thread_id, "append");
    //     return;
    // }

    // KVMap::const_accessor read_acc;

    // if (!just_storage) {
    //     bool exists = false;
    //     {
    //     // Safely check if the key already exists without locking the entire map
    //         if (kv_store.find(read_acc, idx)) {
    //             if (read_acc->second.second) { // Marked deleted
    //                 // error_deleted(cid, thread_id, "append");
    //             } else { // Already written
    //                 // auto err_packet = corfu_storage_serialize_str_entry(read_acc->second.first, CORFU_WRITTEN_PROTO_TYPE, 0);
    //                 // uint64_t allocated_packet_size = err_packet->length() + 1;
    //                 // std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    //                 // memcpy(packet.get(), err_packet->c_str(), allocated_packet_size);
    //                 // packet[err_packet->length()] = '\0';

    //                 // net->send_client_udp_packet(
    //                 //     std::move(packet), 
    //                 //     allocated_packet_size, 
    //                 //     cli_ips[cid],
    //                 //     std::to_string(send_port + cid * max_num_threads + thread_id)
    //                 // );
    //             }
    //         }
    //     }
    //     if (exists) return;
    // }

    // // Address is available; insert the payload
    // if (store(idx, entry)) {
    //     // Atomic max update for the mark tracking high address
    //     uint64_t current_mark = mark.load();
    //     while (idx > current_mark && !mark.compare_exchange_weak(current_mark, idx)) {}

    //     send_ack(cid, thread_id, "append");
    // }
}

void CorfuStorage::read(corfuclient::Payload msg) {
    int curr_epoch = msg.read().currepoch();
    int cid = msg.clientid();
    int thread_id = msg.threadid();

    if (curr_epoch != s_epoch) {
        error_sealed(cid, thread_id, "read");
        return;
    }

    uint64_t idx = msg.read().idx();
    KVMap::const_accessor acc;

    // Concurrent lookup
    if (!kv_store.find(acc, idx)) {
        auto err_packet = corfu_storage_serialize_str_entry("", CORFU_UNWRITTEN_PROTO_TYPE, 0);
        uint64_t allocated_packet_size = err_packet->length() + 1;
        std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
        memcpy(packet.get(), err_packet->c_str(), allocated_packet_size);
        packet[err_packet->length()] = '\0';

        net->send_client_udp_packet(
            std::move(packet), 
            allocated_packet_size, 
            cli_ips[cid],
            std::to_string(send_port + cid * max_num_threads + thread_id)
        );
        return;
    }

    if (acc->second.second) { // Check deleted flag
        error_deleted(cid, thread_id, "read");
        return;
    }

    auto read_packet = corfu_storage_serialize_str_entry(acc->second.first, CORFU_STORE_READ_PROTO_TYPE, 0);
    uint64_t allocated_packet_size = read_packet->length() + 1;
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    memcpy(packet.get(), read_packet->c_str(), allocated_packet_size);
    packet[read_packet->length()] = '\0';

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

    KVMap::accessor acc;
    if (kv_store.find(acc, idx)) {
        acc->second.second = true; // Safely mutate the entry in-place
        send_ack(cid, thread_id, "trim");
    }
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

void CorfuStorage::server(int append_port, std::unique_ptr<Network> append_net, uint64_t thread_id) {
    spdlog::info("CorfuStorage server running (ssid={}, tid={})", ssid, thread_id);
    int perf_fd;
    perf_fd = setup_perf(gettid());
    if (perf_fd < 0) {
        perror("Error opening perf event");
        exit(EXIT_FAILURE);
    }

    pin_current_thread_linux(thread_id);

    // uint64_t wait_time = 10;

    int comms_socket = append_net->setup_batch_socket(append_port);
    if (comms_socket < 0) {
        append_net->stop_batch_threads();
	    return;
    }
    int send_socket = append_socket[thread_id];
    spdlog::debug("Created communication socket {}!", comms_socket);
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    int num_received = 0;

    while (!terminate.load()) {
        char* buf = append_net->recv_packet(comms_socket);
        if (!buf) {
            continue;
        }
        spdlog::debug("got a packet");

        struct corfu_cli_header* recv_header = (struct corfu_cli_header*)buf;
        uint16_t recv_port = ntohs(recv_header->recv_port);
        uint32_t client_ip = recv_header->client_ip;
        
        switch (ntohs(recv_header->proto_type)) {
            case CORFU_APPEND_PROTO_TYPE: {
                spdlog::info("storage {} got an append req from client", ssid);
                
                struct corfu_append* append_entry = (struct corfu_append*)(buf + sizeof(struct corfu_cli_header));
 	            uint32_t log_idx = ntohl(append_entry->log_idx);
                // uint32_t curr_epoch = ntohl(append_entry->curr_epoch);
                // uint32_t entry_size = ntohl(append_entry->entry_size);
                char* entry = (char*)(buf + sizeof(struct corfu_cli_header) + sizeof(struct corfu_append));
                std::string string_to_store(entry);
                // write(log_idx, string_to_store);

                if (store(log_idx, entry)) {
                    // Atomic max update for the mark tracking high address
                    uint64_t current_mark = mark.load();
                    while (log_idx > current_mark && !mark.compare_exchange_weak(current_mark, log_idx)) {}

                    // send_ack(cid, thread_id, "append");

                    size_t size_of_hdr = get_corfu_storage_header_size();
                    uint64_t allocated_packet_size = size_of_hdr + 1;

                    // Create packet buffer which will be sent  
                    struct corfu_storage_header* hdr = (struct corfu_storage_header*)(buf);
                    hdr->proto_type = htons(CORFU_ACK_PROTO_TYPE);

                    server_addr.sin_port = htons(recv_port);
                    server_addr.sin_addr.s_addr = client_ip;
                    sendto(send_socket, buf, allocated_packet_size, 0, (struct sockaddr*)&server_addr, sizeof(server_addr));
                }
                num_received += 1;
                break;
            }
            case CORFU_READ_PROTO_TYPE: {
                spdlog::info("storage {} got a read req from client", ssid);
                struct corfu_append* read_entry = (struct corfu_append*)(buf + sizeof(struct corfu_cli_header)); // Reusing format
                uint32_t log_idx = ntohl(read_entry->log_idx);
                
                KVMap::const_accessor acc;
                size_t size_of_hdr = get_corfu_storage_header_size();
                struct corfu_storage_header* hdr = (struct corfu_storage_header*)(buf);

                if (!kv_store.find(acc, log_idx)) {
                    hdr->proto_type = htons(CORFU_UNWRITTEN_PROTO_TYPE);
                    server_addr.sin_port = htons(recv_port);
                    server_addr.sin_addr.s_addr = client_ip;
                    sendto(send_socket, buf, size_of_hdr + 1, 0, (struct sockaddr*)&server_addr, sizeof(server_addr));
                } else if (acc->second.second) { // deleted
                    hdr->proto_type = htons(CORFU_DELETED_PROTO_TYPE);
                    server_addr.sin_port = htons(recv_port);
                    server_addr.sin_addr.s_addr = client_ip;
                    sendto(send_socket, buf, size_of_hdr + 1, 0, (struct sockaddr*)&server_addr, sizeof(server_addr));
                } else {
                    hdr->proto_type = htons(CORFU_STORE_READ_PROTO_TYPE);
                    std::string content = acc->second.first;
                    
                    uint64_t allocated_packet_size = size_of_hdr + content.size() + 1;
                    std::unique_ptr<char[]> out_packet = std::make_unique<char[]>(allocated_packet_size);
                    memcpy(out_packet.get(), hdr, size_of_hdr);
                    memcpy(out_packet.get() + size_of_hdr, content.c_str(), content.size() + 1);

                    server_addr.sin_port = htons(recv_port);
                    server_addr.sin_addr.s_addr = client_ip;
                    sendto(send_socket, out_packet.get(), allocated_packet_size, 0, (struct sockaddr*)&server_addr, sizeof(server_addr));
                }
                break;
            }
            case CORFU_TRIM_PROTO_TYPE: {
                spdlog::info("storage {} got a trim req from client", ssid);
                struct corfu_append* trim_entry = (struct corfu_append*)(buf + sizeof(struct corfu_cli_header));
                uint32_t log_idx = ntohl(trim_entry->log_idx);
                
                KVMap::accessor acc;
                if (kv_store.find(acc, log_idx)) {
                    acc->second.second = true; 
                }

                size_t size_of_hdr = get_corfu_storage_header_size();
                struct corfu_storage_header* hdr = (struct corfu_storage_header*)(buf);
                hdr->proto_type = htons(CORFU_ACK_PROTO_TYPE);
                
                server_addr.sin_port = htons(recv_port);
                server_addr.sin_addr.s_addr = client_ip;
                sendto(send_socket, buf, size_of_hdr + 1, 0, (struct sockaddr*)&server_addr, sizeof(server_addr));
                break;
            }
            case CORFU_SEAL_PROTO_TYPE: {
                spdlog::info("storage {} got a seal req from client", ssid);
                struct corfu_seal* seal_entry = (struct corfu_seal*)(buf + sizeof(struct corfu_cli_header));
                uint32_t new_epoch = ntohl(seal_entry->curr_epoch);
                
                if (new_epoch > this->s_epoch) {
                    this->s_epoch = new_epoch; // this node is now sealed against older epochs
                }

                size_t size_of_hdr = get_corfu_storage_header_size();
                struct corfu_storage_header* hdr = (struct corfu_storage_header*)(buf);
                hdr->proto_type = htons(CORFU_ACK_PROTO_TYPE);
                
                server_addr.sin_port = htons(recv_port);
                server_addr.sin_addr.s_addr = client_ip;
                sendto(send_socket, buf, size_of_hdr + 1, 0, (struct sockaddr*)&server_addr, sizeof(server_addr));
                break;
            }
            default: {
                spdlog::warn("Unknown packet type received");
                break;
            }
        }
    }

    append_net->stop_batch_threads();
    close(comms_socket);
    spdlog::critical("Total num received: {}", num_received);
}

std::string CorfuStorage::get(uint64_t /*idx*/) {
    return "";
}

void CorfuStorage::wait_to_finish() {
    std::chrono::seconds sleep_duration(max_duration);
    std::this_thread::sleep_for(sleep_duration);
    end_thread = true;
}