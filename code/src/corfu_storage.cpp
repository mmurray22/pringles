#include "corfu_storage.h"

const uint64_t MAX_WAIT_TIME = 100;

#define WRITE 0
#define READ 1
#define DELETE 2
#define SEAL 3

CorfuStorage::CorfuStorage(uint64_t ssid) {
    server_thread = std::thread(&CorfuServer::server, this);
}

CorfuStorage::~CorfuStorage() {
    terminate = true;
    server_thread.join();
}

void CorfuStorage::write(std::string msg) {
    // if epoch != s_epoch, respond <err_sealed>
    if (msg.append().currEpoch() != s_epoch) {
        std::unique_ptr<std::string> err_packet = corfu_storage_serialize_str_entry("", CORFU_SEALED_PROTO_TYPE, 0);
        std::string client_id = msg.clientID(); // each packet comes with a client ip, but better solution should be found

        net->add_to_send_queue(err_packet, client_id);
        return;
    }
    
    // otherwise check the address status
    // check if deleted:
    if (storage_map.count(msg.append().idx()) > 0) { // is the entry already in the map?
        if (storage_map[msg.append().idx()].deleted) { // has it been marked deleted?
            // send <err_deleted>
            std::unique_ptr<std::string> err_packet = corfu_storage_serialize_str_entry("", CORFU_DELETED_PROTO_TYPE, 0);
            std::string client_id = msg.clientID(); // each packet comes with a client ip, but better solution should be found

            net->add_to_send_queue(err_packet, client_id);
            return;
        } else { // if not marked deleted, then it must be written to already so we send back err + written contents
            // send <err_written>
            std::unique_ptr<std::string> err_packet = corfu_storage_serialize_str_entry(storage_map[msg.append().idx()].contents, CORFU_WRITTEN_PROTO_TYPE, 0);
            std::string client_id = msg.clientID(); // each packet comes with a client ip, but better solution should be found

            net->add_to_send_queue(err_packet, client_id);
            return;
        }
    }
    // otherwise, the address is available so we write the content to the local store
    storage_map[msg.append().idx()] = {false, msg.append().entry()};

    // reply with ack
    std::unique_ptr<std::string> ack_packet = corfu_storage_serialize_str_entry("", CORFU_ACK_PROTO_TYPE, 0);
    std::string client_id = msg.clientID(); // each packet comes with a client ip, but better solution should be found

    net->add_to_send_queue(ack_packet, client_id);
}

void CorfuStorage::read(std::string msg) {
    // if epoch != s_epoch, respond <err_sealed>
    if (msg.read().currEpoch() != s_epoch) {
        std::unique_ptr<std::string> err_packet = corfu_storage_serialize_str_entry("", CORFU_SEALED_PROTO_TYPE, 0);
        std::string client_id = msg.clientID(); // each packet comes with a client ip, but better solution should be found

        net->add_to_send_queue(err_packet, client_id);
        return;
    }

    // otherwise check the address status
    // if unwritten, respond <err_unwritten> (entry is not in the map)
    if (storage_map.count(msg.read().idx()) == 0) { // is the entry not already in the map?
        std::unique_ptr<std::string> err_packet = corfu_storage_serialize_str_entry("", CORFU_UNWRITTEN_PROTO_TYPE, 0);
        std::string client_id = msg.clientID(); // each packet comes with a client ip, but better solution should be found

        net->add_to_send_queue(err_packet, client_id);
        return;
    }

    // if deleted, respond <err_deleted> (deleted bit in map set)
    if (storage_map[msg.read().idx()].deleted) {
            // send <err_deleted>
            std::unique_ptr<std::string> err_packet = corfu_storage_serialize_str_entry("", CORFU_DELETED_PROTO_TYPE, 0);
            std::string client_id = msg.clientID(); // each packet comes with a client ip, but better solution should be found

            net->add_to_send_queue(err_packet, client_id);
            return;
    }

    // if written, respond <pg_content>
    std::unique_ptr<std::string> read_packet = corfu_storage_serialize_str_entry(storage_map[msg.read().idx()].contents, CORFU_STORE_READ_PROTO_TYPE, 0);
    std::string client_id = msg.clientID(); // each packet comes with a client ip, but better solution should be found

    net->add_to_send_queue(err_packet, client_id);
}

void CorfuStorage::storage_delete(std::string msg) {
    // mark addr deleted
    storage_map[msg.trim().idx()]->deleted = true;

    // reply with ack
    std::unique_ptr<std::string> ack_packet = corfu_storage_serialize_str_entry("", CORFU_ACK_PROTO_TYPE, 0);
    std::string client_id = msg.clientID(); // each packet comes with a client ip, but better solution should be found

    net->add_to_send_queue(ack_packet, client_id);
}

void CorfuStorage::seal(std::string msg) {
    // begin seal with setting new epoch value
    if (msg.seal().currEpoch() > s_epoch) {
        s_epoch = msg.seal().currEpoch();
    }

    // respond <sealed, highaddr> with highaddr = highest locally stored page address
    std::unique_ptr<std::string> payload = corfu_storage_serialize_str_entry("", CORFU_STORE_SEAL_PROTO_TYPE, this->mark);
    net->add_to_send_queue(payload, msg.clientID());
}

void CorfuStorage::server(std::shared_ptr<Network> net) {
    spdlog::info("Corfu Server Starting");
    std::unique_ptr<std::string> rcv_str = NULL;
    uint64_t wait_time = 10;
    while (true) {
        if (wait_time >= MAX_WAIT_TIME) {
            spdlog::debug("!!!!!!!!!!!!!!No more packets to receive.");
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(wait_time));
        wait_time += 10;
        rcv_str = net->read_from_recv_queue();
        if (rcv_str == NULL) {
            continue;
        }

        // deserialize string, send to other functions
        msg = Trace::corfu_client_deserialize_str_entry(recv_str);

        if (msg.packet_type() == CORFU_APPEND_PROTO_TYPE) {
            write(msg);
        } else if (msg.packet_type() == CORFU_READ_PROTO_TYPE) {
            read(msg);
        } else if (msg.packet_type() == CORFU_TRIM_PROTO_TYPE) {
            storage_delete(msg);
        } else if (msg.packet_type() == CORFU_SEAL_PROTO_TYPE) {
            seal(msg);
        }
        wait_time = 10;
    }
}