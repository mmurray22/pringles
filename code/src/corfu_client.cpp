#include "corfu_client.h"

CorfuClient::CorfuClient(YAML::Node config) {
    // Create network
    net = std::make_shared<Network>(get_threads(config), 
                                    get_seq_ip(config),
                                    get_storage_ips(config), 
                                    get_send_port(config), 
                                    get_recv_port(config),
                                    get_protocol(config), 
                                    get_log_level(config),
                                    get_batch_size(config),
                                    get_interface(config));

    // Create trace
    trace = std::make_shared<Trace<std::string>>(get_trace_file(config));

    // Get Client type
    cli_type = fromStringToClientType(get_type(config));
    pending_appends_updated = false;

    // Timeouts
    wait_for_read_acks = get_read_timeout(config);
    wait_for_write_acks = get_write_timeout(config);

    // Create recv thread
    recv = std::thread(&CorfuClient::recv_pkt, this); // CTODO: make this function for CorfuClient
}

CorfuClient::~CorfuClient() {
    terminate = true;
    net->done();
    recv.join();
}

void reconfigure(uint64_t log_idx, CorfuStorage failing_unit) {
    return;
}

uint64_t CorfuClient::append(std::unique_ptr<std::string> entry) {
    // we use the network object to send a request to the sequencer for the next log position
    std::unique_ptr<std::string> sequencing_packet = corfu_client_serialize_str_entry("", CORFU_GETTOKEN_PROTO_TYPE, client_id, 0, 0);
    net->add_to_send_queue(sequencing_packet, sequencer.IP); // request a log position

    std::string msg;

    // start timer
    auto start_time = std::chrono::high_resolution_clock::now();
    auto read_time = std::chrono::high_resolution_clock::duration::zero();
    while (read_time < TIMEOUT && msg.empty()) {
        msg = net->read_from_recv_queue(sequencer.IP);
        read_time = std::chrono::high_resolution_clock::now() - start_time;
    }

    if (msg.empty() && read_time >= TIMEOUT) {
        spdlog::critical("We did not receive anything from server before timeout");
        return 1; // failure
    }

    // recv packet from the sequencer
    std::string packet_contents = corfu_sequencer_deserialize_str_entry(msg);

    uint64_t log_idx = packet_contents.send_token().token();

    // loop through all of the replicas that have this log position
    std::vector<CorfuStorage> send_machines;
    // use current epoch to find corresponding send machines
    for (const auto& config_kv : this->auxiliary[this->curr_epoch]) {
        std::pair<uint64_t, uint64_t> curr_range = config_kv.first;
        std::vector<CorfuStorage> server_range = config_kv.second;

        if (log_idx >= curr_range.first && log_idx < curr_range.second) {
            send_machines = server_range;
            break;
        }
    }

    if (send_machines.empty()) {
        spdlog::critical("Append: Unable to find send machines");
        return 1; // return 1 indicates there was a failure and we should try again
    }

    for (CorfuStorage sm : send_machines) {
        std::unique_ptr<std::string> write_packet = corfu_client_serialize_str_entry(entry, CORFU_APPEND_PROTO_TYPE, client_id, log_idx, curr_epoch);
        net->add_to_send_queue(write_packet, sm.IP);

        std::string msg;

        // start timer
        auto start_time = std::chrono::high_resolution_clock::now();
        auto read_time = std::chrono::high_resolution_clock::duration::zero();
        while (read_time < TIMEOUT && msg.empty()) {
            msg = net->read_from_recv_queue(sm.IP);
            read_time = std::chrono::high_resolution_clock::now() - start_time;
        }

        // must reconfigure if there's no response
        if (msg.empty() && read_time >= TIMEOUT) {
            spdlog::info("Append: Must reconfigure because there was no response");
            CorfuStorage failing_unit = auxiliary[curr_epoch][log_idx][0];
            reconfigure(log_idx, failing_unit);
            spdlog::info("Just reconfigured, you should attempt to append again");
            return 1; // return error
        }

        std::string packet_contents = corfu_storage_deserialize_str_entry(msg);

        if (packet_contents.packet_type() == CORFU_SEALED_PROTO_TYPE) {
            spdlog::info("Must reconfigure because the current epoch was sealed");
            CorfuStorage failing_unit = auxiliary[curr_epoch][log_idx][0];
            reconfigure(log_idx, failing_unit);
            spdlog::info("Just reconfigured, you should attempt to append again");
            return 1; // return error
        } else if (packet_contents.packet_type() == CORFU_DELETED_PROTO_TYPE) {
            spdlog::critical("We got an err_deleted and now we're returning the error code");
            return 1;
        } else if (packet_contents.packet_type() == CORFU_UNWRITTEN_PROTO_TYPE) {
            spdlog::critical("We got an err_unwritten and now we're returning the error code");
            return 1;
        }

        // check to make sure that we've received an ack
        if (packet_contents.packet_type() == CORFU_ACK_PROTO_TYPE) {
            spdlog::info("Append: received an ack, continuing to write to next server");
            continue;
        } else {
            spdlog::critical("Append: We did not receive an ack :(");
            return 1; // return error
        }
    }
    return log_idx;
}

std::unique_ptr<std::string> SimpleClient::read(uint64_t idx) {
    return nullptr;
}

uint64_t CorfuClient::fill(uint64_t idx) {
    return 0;
}

bool CorfuClient::trim(uint64_t log_idx) {
    // loop through all of the replicas that have this log position
    std::vector<CorfuStorage> send_machines;
    // use current epoch to find corresponding send machines
    for (const auto& config_kv : this->auxiliary[this->curr_epoch]) {
        std::pair<uint64_t, uint64_t> curr_range = config_kv.first;
        std::vector<CorfuStorage> server_range = config_kv.second;

        if (log_idx >= curr_range.first && log_idx < curr_range.second) {
            send_machines = server_range;
            break;
        }
    }

    if (send_machines.empty()) {
        spdlog::critical("Unable to find send machines");
        return 1; // return 1 indicates there was a failure and we should try again
    }

    for (CorfuStorage sm : send_machines) {
        std::unique_ptr<std::string> delete_packet = corfu_client_serialize_str_entry("", CORFU_TRIM_PROTO_TYPE, client_id, log_idx, 0);
        net->add_to_send_queue(delete_packet, sm.IP);

        std::unique_ptr<std::string> msg;
        // start timer
        auto start_time = std::chrono::high_resolution_clock::now();
        auto read_time = std::chrono::high_resolution_clock::duration::zero();
        while (read_time < TIMEOUT && msg.empty()) {
            msg = net->read_from_recv_queue(sm.IP);
            read_time = std::chrono::high_resolution_clock::now() - start_time;
        }

        // never received ack
        if (msg.empty() && read_time >= TIMEOUT) {
            spdlog::critical("Trim: We did not receive an ack before timeout");
            return 1; // failure
        }

        std::string packet_contents = corfu_storage_deserialize_str_entry(msg);
        if (packet_contents.packet_type() == CORFU_ACK_PROTO_TYPE) {
            // Note: corfu paper does not say to trim anything from local log representation,
            // so this is a possible optimization to add later :)
            continue;
        } else {
            spdlog::critical("Trim: We did not receive an ack :(");
            return 1; // we did not receive an ack, so we must return a failure
        }

    }
    return 0; // success
}
