#include "corfu_storage.h"
#include "structs.h"
#include "spdlog/spdlog.h"
#include <chrono>
#include <algorithm>
#include <cstring>

// const uint64_t MAX_WAIT_TIME = 100;

CorfuStorage::CorfuStorage(uint64_t ssid, std::shared_ptr<Network> net)
    : ssid(ssid), s_epoch(0), mark(0), storage_map(), net(std::move(net)), terminate(false)
{
    spdlog::info("Starting CorfuStorage server thread (ssid={})", ssid);
    server_thread = std::thread(&CorfuStorage::server, this);
}

CorfuStorage::~CorfuStorage() {
    spdlog::info("Stopping CorfuStorage server thread (ssid={})", ssid);
    terminate.store(true);
    if (server_thread.joinable()) {
        server_thread.join();
    }
}

void CorfuStorage::write(corfuclient::Payload msg) {
    // // if epoch != s_epoch, respond <err_sealed>
    // if (msg.append().currepoch() != s_epoch) {
    //     auto err_packet = Trace<std::string>::corfu_storage_serialize_str_entry("", CORFU_SEALED_PROTO_TYPE, 0);

    //     net->add_to_send_queue(std::move(err_packet), msg.clientid());
    //     return;
    // }
    
    // // otherwise check the address status
    // // check if deleted:
    // uint64_t idx = msg.append().idx();
    // auto it = storage_map.find(idx);

    // if (it != storage_map.end()) { // is the entry already in the map?
    //     if (it->second.deleted) { // has it been marked deleted?
    //         // send <err_deleted>
    //         auto err_packet = Trace<std::string>::corfu_storage_serialize_str_entry("", CORFU_DELETED_PROTO_TYPE, 0);
    //         net->add_to_send_queue(std::move(err_packet), msg.clientid());
    //     } else { // if not marked deleted, then it must be written to already so we send back err + written contents
    //         // send <err_written>
    //         auto err_packet = Trace<std::string>::corfu_storage_serialize_str_entry(it->second.contents, CORFU_WRITTEN_PROTO_TYPE, 0);
    //         net->add_to_send_queue(std::move(err_packet), msg.clientid());
    //     }
    //     return;
    // }
    // // otherwise, the address is available so we write the content to the local store
    // storage_map[idx] = {false, msg.append().entry()};
    // mark = std::max(mark, idx);

    // // reply with ack
    // auto ack_packet = Trace<std::string>::corfu_storage_serialize_str_entry("", CORFU_ACK_PROTO_TYPE, 0);
    // net->add_to_send_queue(std::move(ack_packet), msg.clientid());
    (void) msg;
}

void CorfuStorage::read(corfuclient::Payload msg) {
//     // if epoch != s_epoch, respond <err_sealed>
//     if (msg.read().currepoch() != s_epoch) {
//         auto err_packet = Trace<std::string>::corfu_storage_serialize_str_entry("", CORFU_SEALED_PROTO_TYPE, 0);

//         net->add_to_send_queue(std::move(err_packet), msg.clientid());
//         return;
//     }

//     // otherwise check the address status
//     uint64_t idx = msg.read().idx();
//     auto it = storage_map.find(idx);

//     // if unwritten, respond <err_unwritten> (entry is not in the map)
//     if (it == storage_map.end()) { // is the entry not already in the map?
//         auto err_packet = Trace<std::string>::corfu_storage_serialize_str_entry("", CORFU_UNWRITTEN_PROTO_TYPE, 0);
//         net->add_to_send_queue(std::move(err_packet), msg.clientid());
//         return;
//     }

//     // if deleted, respond <err_deleted> (deleted bit in map set)
//     if (it->second.deleted) {
//         // send <err_deleted>
//         auto err_packet = Trace<std::string>::corfu_storage_serialize_str_entry("", CORFU_DELETED_PROTO_TYPE, 0);
//         net->add_to_send_queue(std::move(err_packet), msg.clientid());
//         return;
//     }

//     // if written, respond <pg_content>
//     auto read_packet = Trace<std::string>::corfu_storage_serialize_str_entry(it->second.contents, CORFU_STORE_READ_PROTO_TYPE, 0);
//     net->add_to_send_queue(std::move(read_packet), msg.clientid());
        (void) msg;
}

void CorfuStorage::storage_delete(corfuclient::Payload msg) {
//     // mark addr deleted
//     uint64_t idx = msg.trim().idx();
//     auto &entry = storage_map[idx];
//     entry.deleted = true;
//     entry.contents.clear();

//     // reply with ack
//     auto ack_packet = Trace<std::string>::corfu_storage_serialize_str_entry("", CORFU_ACK_PROTO_TYPE, 0);
//     net->add_to_send_queue(std::move(ack_packet), msg.clientid());
        (void) msg;
}

void CorfuStorage::seal(corfuclient::Payload msg) {
    // // begin seal with setting new epoch value
    // if (msg.seal().currepoch() > s_epoch) {
    //     s_epoch = msg.seal().currepoch();
    // }

    // // respond <sealed, highaddr> with highaddr = highest locally stored page address
    // auto payload = Trace<std::string>::corfu_storage_serialize_str_entry("", CORFU_STORE_SEAL_PROTO_TYPE, mark);
    // net->add_to_send_queue(std::move(payload), msg.clientid());
        (void) msg;
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

            switch (msg.packet_type()) {
                case CORFU_APPEND_PROTO_TYPE:
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