#include "corfu_storage.h"
#include "trace.h"
// #include "network.h"
#include "spdlog/spdlog.h"

// #include <algorithm>

const uint64_t MAX_WAIT_TIME = 100;

CorfuStorage::CorfuStorage(uint64_t ssid, std::shared_ptr<Network> net)
    : ssid(ssid), 
      s_epoch(0), 
      mark(0),
      storage_map(),
      net(std::move(net)),
      server_thread(),
      terminate(false)
{
    spdlog::info("Starting CorfuStorage server thread (ssid={})", ssid);
    server_thread = std::thread(&CorfuStorage::server, this);
}

CorfuStorage::~CorfuStorage() {
    spdlog::info("Stopping CorfuStorage server thread (ssid={})", ssid);
    terminate.store(true);
    server_thread.join();
}

void CorfuStorage::write(corfuclient::Payload msg) {
    // if epoch != s_epoch, respond <err_sealed>
    if (msg.append().currEpoch() != s_epoch) {
        auto err_packet = corfu_storage_serialize_str_entry("", CORFU_SEALED_PROTO_TYPE, 0);

        net->add_to_send_queue(std::move(err_packet), msg.client_id());
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
            net->add_to_send_queue(std::move(err_packet), msg.clientID());
        } else { // if not marked deleted, then it must be written to already so we send back err + written contents
            // send <err_written>
            auto err_packet = corfu_storage_serialize_str_entry(it->second.contents, CORFU_WRITTEN_PROTO_TYPE, 0);
            net->add_to_send_queue(std::move(err_packet), msg.clientID());
        }
        return;
    }
    // otherwise, the address is available so we write the content to the local store
    storage_map[idx] = {false, msg.append().entry()};
    mark = std::max(mark, idx);

    // reply with ack
    auto ack_packet = corfu_storage_serialize_str_entry("", CORFU_ACK_PROTO_TYPE, 0);
    net->add_to_send_queue(std::move(ack_packet), msg.clientID());
}

void CorfuStorage::read(corfuclient::Payload msg) {
    // if epoch != s_epoch, respond <err_sealed>
    if (msg.read().currEpoch() != s_epoch) {
        auto err_packet = corfu_storage_serialize_str_entry("", CORFU_SEALED_PROTO_TYPE, 0);

        net->add_to_send_queue(std::move(err_packet), msg.client_id());
        return;
    }

    // otherwise check the address status
    uint64_t idx = msg.read().idx();
    auto it = storage_map.find(idx);

    // if unwritten, respond <err_unwritten> (entry is not in the map)
    if (it == storage_map.end()) { // is the entry not already in the map?
        auto err_packet = corfu_storage_serialize_str_entry("", CORFU_UNWRITTEN_PROTO_TYPE, 0);
        net->add_to_send_queue(std::move(err_packet), msg.clientID());
        return;
    }

    // if deleted, respond <err_deleted> (deleted bit in map set)
    if (it->second.deleted) {
        // send <err_deleted>
        auto err_packet = corfu_storage_serialize_str_entry("", CORFU_DELETED_PROTO_TYPE, 0);
        net->add_to_send_queue(std::move(err_packet), msg.clientID());
        return;
    }

    // if written, respond <pg_content>
    auto read_packet = corfu_storage_serialize_str_entry(it->second.contents, CORFU_STORE_READ_PROTO_TYPE, 0);
    net->add_to_send_queue(std::move(read_packet), msg.clientID());
}

void CorfuStorage::storage_delete(corfuclient::Payload msg) {
    // mark addr deleted
    uint64_t idx = msg.trim().idx();
    auto &entry = storage_map[idx];
    entry.deleted = true;
    entry.contents.clear();

    // reply with ack
    auto ack_packet = corfu_storage_serialize_str_entry("", CORFU_ACK_PROTO_TYPE, 0);
    net->add_to_send_queue(std::move(ack_packet), msg.clientID());
}

void CorfuStorage::seal(corfuclient::Payload msg) {
    // begin seal with setting new epoch value
    if (msg.seal().currEpoch() > s_epoch) {
        s_epoch = msg.seal().currEpoch();
    }

    // respond <sealed, highaddr> with highaddr = highest locally stored page address
    auto payload = corfu_storage_serialize_str_entry("", CORFU_STORE_SEAL_PROTO_TYPE, mark);
    net->add_to_send_queue(std::move(payload), msg.clientID());
}

void CorfuStorage::server() {
    spdlog::info("CorfuStorage server running (ssid={})", ssid);

    uint64_t wait_time = 10;
    std::unique_ptr<std::string> rcv_str;

    while (!terminate.load()) {
        rcv_str = net->read_from_recv_queue();
        if (!rcv_str) {
            std::this_thread::sleep_for(std::chrono::milliseconds(wait_time));
            wait_time = std::min(wait_time + 10, MAX_WAIT_TIME);
            continue;
        }

        wait_time = 10; // reset wait time on message receive

        corfuclient::Payload msg = Trace::corfu_client_deserialize_str_entry(*rcv_str);

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
                spdlog::warn("Unknown packet type received: {}", msg.packet_type());
                break;
        }
    }

    spdlog::info("CorfuStorage server stopped (ssid={})", ssid);
}