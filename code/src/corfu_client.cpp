#include "corfu_client.h"
#include "spdlog/spdlog.h"

#include "corfuclient.pb.h"
#include "corfustorage.pb.h"
#include "corfusequencer.pb.h"

CorfuClient::CorfuClient(YAML::Node config) : sequencer(config){
    // Create network
    bool run_threads = false;
    net = std::make_unique<Network>(get_threads(config),
                    std::to_string(get_send_port(config)), 
                    std::to_string(get_recv_port(config)),
                    get_socket_type(config),
                                    get_log_level(config),
                    get_batch_size(config),
                    get_batch_on(config),
                    get_interface(config),
                    get_self_ip(config),
                    get_num_pkt_types(config),
                    run_threads);
        cid = get_cli_id(config);
        set_spdlog_level(get_log_level(config));
        spdlog::info("Corfu Client YAML parsed");


    // Create trace
    trace = std::make_shared<Trace<std::string>>(get_trace_file(config));

    // Get Client type
    pending_appends_updated = false;
    terminate = false;

    // Timeouts
    wait_for_read_acks = get_read_timeout(config);
    wait_for_write_acks = get_write_timeout(config);

    // Create recv thread
    recv_thread = std::thread(&CorfuClient::recv_pkt, this); // CTODO: make this function for CorfuClient
}

CorfuClient::~CorfuClient() {
    terminate = true;
    if (recv_thread.joinable()) {
        recv_thread.join();
    }
}

void CorfuClient::reconfigure(uint64_t /*log_idx*/, CorfuStorage& /*failing_unit*/) {
    return;
}

uint64_t CorfuClient::append(std::unique_ptr<std::string> entry) {
    // we use the network object to send a request to the sequencer for the next log position
    std::unique_ptr<std::string> sequencing_packet = Trace<std::string>::corfu_client_serialize_str_entry("", CORFU_GETTOKEN_PROTO_TYPE, cid, 0, 0);
    net->add_to_send_queue(std::move(sequencing_packet), sequencer.IP); // request a log position

    auto msg = net->read_from_recv_queue();

    // start timer
    auto start_time = std::chrono::high_resolution_clock::now();
    auto read_time = std::chrono::high_resolution_clock::duration::zero();
    while (read_time < TIMEOUT && !msg) {
        msg = net->read_from_recv_queue();
        read_time = std::chrono::high_resolution_clock::now() - start_time;
    }

    if (!msg && read_time >= TIMEOUT) {
        spdlog::critical("We did not receive anything from server before timeout");
        return 1; // failure
    }

    // recv packet from the sequencer
    corfusequencer::Payload packet_contents = Trace<std::string>::corfu_sequencer_deserialize_str_entry(std::move(msg));

    uint64_t log_idx = packet_contents.send_token().token();

    // loop through all of the replicas that have this log position
    std::vector<std::shared_ptr<CorfuStorage>> send_machines;

    // We use curr_epoch to grab the inner map of ranges for this specific epoch
    for (const auto& config_kv : this->auxiliary[this->curr_epoch]) {
        
        std::pair<uint64_t, uint64_t> curr_range = config_kv.first;
        const std::vector<std::shared_ptr<CorfuStorage>>& server_range = config_kv.second;

        // Check if our log_idx falls inside {range_start, range_end}
        if (log_idx >= curr_range.first && log_idx < curr_range.second) {
            send_machines = server_range;
            break;
        }
    }

    if (send_machines.empty()) {
        spdlog::critical("Append: Unable to find send machines");
        return 1; 
    }

    for (auto& sm : send_machines) {
        std::unique_ptr<std::string> write_packet = Trace<std::string>::corfu_client_serialize_str_entry(*entry, CORFU_APPEND_PROTO_TYPE, cid, log_idx, curr_epoch);
        net->add_to_send_queue(std::move(write_packet), std::to_string(sm->ssid));
        // CHECK HOW SHOULD I BE GETTING THE IPs OF SEND MACHINES???

        std::unique_ptr<std::string> msg;

        // start timer
        auto start_time = std::chrono::high_resolution_clock::now();
        auto read_time = std::chrono::high_resolution_clock::duration::zero();
        while (read_time < TIMEOUT && !msg) {
            msg = net->read_from_recv_queue();
            read_time = std::chrono::high_resolution_clock::now() - start_time;
        }

        // must reconfigure if there's no response
        if (!msg && read_time >= TIMEOUT) {
            spdlog::info("Append: Must reconfigure because there was no response");
            std::shared_ptr<CorfuStorage> failing_unit = send_machines[0];
            reconfigure(log_idx, *failing_unit);
            spdlog::info("Just reconfigured, you should attempt to append again");
            return 1; // return error
        }

        corfustorage::Payload packet_contents = Trace<std::string>::corfu_storage_deserialize_str_entry(std::move(msg));

        if (packet_contents.packet_type() == CORFU_SEALED_PROTO_TYPE) {
            spdlog::info("Must reconfigure because the current epoch was sealed");
            std::shared_ptr<CorfuStorage> failing_unit = send_machines[0];
            reconfigure(log_idx, *failing_unit);
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

std::unique_ptr<std::string> CorfuClient::read(uint64_t /*idx*/) {
    return nullptr;
}

uint64_t CorfuClient::fill(uint64_t /*idx*/) {
    return 0;
}

bool CorfuClient::trim(uint64_t log_idx) {
    // loop through all of the replicas that have this log position
    std::vector<std::shared_ptr<CorfuStorage>> send_machines;

    // We use curr_epoch to grab the inner map of ranges for this specific epoch
    for (const auto& config_kv : this->auxiliary[this->curr_epoch]) {
        
        std::pair<uint64_t, uint64_t> curr_range = config_kv.first;
        const std::vector<std::shared_ptr<CorfuStorage>>& server_range = config_kv.second;

        // Check if our log_idx falls inside {range_start, range_end}
        if (log_idx >= curr_range.first && log_idx < curr_range.second) {
            send_machines = server_range;
            break;
        }
    }

    if (send_machines.empty()) {
        spdlog::critical("Append: Unable to find send machines");
        return false; 
    }

    for (auto& sm : send_machines) {
        std::unique_ptr<std::string> delete_packet = Trace<std::string>::corfu_client_serialize_str_entry("", CORFU_TRIM_PROTO_TYPE, cid, log_idx, 0);
        net->add_to_send_queue(std::move(delete_packet), std::to_string(sm->ssid));

        std::unique_ptr<std::string> msg;
        // start timer
        auto start_time = std::chrono::high_resolution_clock::now();
        auto read_time = std::chrono::high_resolution_clock::duration::zero();
        while (read_time < TIMEOUT && !msg) {
            msg = net->read_from_recv_queue();
            read_time = std::chrono::high_resolution_clock::now() - start_time;
        }

        // never received ack
        if (!msg && read_time >= TIMEOUT) {
            spdlog::critical("Trim: We did not receive an ack before timeout");
            return false; // failure
        }

        corfustorage::Payload packet_contents = Trace<std::string>::corfu_storage_deserialize_str_entry(std::move(msg));
        if (packet_contents.packet_type() == CORFU_ACK_PROTO_TYPE) {
            // Note: corfu paper does not say to trim anything from local log representation,
            // so this is a possible optimization to add later :)
            continue;
        } else {
            spdlog::critical("Trim: We did not receive an ack :(");
            return false; // we did not receive an ack, so we must return a failure
        }

    }
    return true; // success
}
