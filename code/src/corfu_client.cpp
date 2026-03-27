#include <thread>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/if_ether.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <net/if.h>
#include <net/ethernet.h>
#include <sys/ioctl.h>
#include <linux/ip.h>
#include <linux/if_packet.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdexcept>
#include <cstring>
#include <atomic>

#include "corfu_client.h"
#include "utils.h"
#include "spdlog/spdlog.h"
#include "yaml-cpp/yaml.h"
#define RECEIVE_PORT 3149

#include "corfuclient.pb.h"
#include "corfustorage.pb.h"
#include "corfusequencer.pb.h"

CorfuClient::CorfuClient(std::string input_file, uint64_t thread_id, CorfuSequencer& sequencer) {
   // Get packet types for sending/receiving
   this->sequencer = &sequencer;
   this->cid = thread_id;
   YAML::Node config = YAML::LoadFile(input_file);
   num_pkt_types = get_num_pkt_types(config);
   std::vector<std::array<uint8_t, 6>> mac_addrs = get_dst_mac_addrs(config);
   if (mac_addrs.size() < 1) {
       spdlog::critical("Unable to parse mac address!");
       throw;
   }
  
   this->num_work_threads = get_num_client_threads(config);
   // Create network
   bool run_threads = false;
   net = std::make_unique<Network>(
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
    spdlog::info("Corfu Client: net init");
    
    // Start receiving thread 
    for (size_t i = 0; i < num_pkt_types; i++) {
        this->pkt_q.insert(std::pair<PacketType, std::queue<std::unique_ptr<char[]>>>(PacketType(i), std::queue<std::unique_ptr<char[]>>()));
    }

    // Protocol types
    this->seq = SequencerType(get_sequencer_type(config));

    // Updating the log 
    this->started_append = false; 

    this->payload_size = get_payload_size(config);
    this->batch_size = num_work_threads * payload_size;
    spdlog::debug("Batch size: {}", batch_size);
    // this->stat = std::make_unique<Stats>(get_batch_size(config), get_batch_on(config), get_json_name(config), thread_id);
    this->max_duration = get_experiment_duration(config);
    this->warm_up = get_warm_up(config);
    this->cool_down = get_cool_down(config);
    this->global_thread_id = thread_id;
   
    this->switch_mac = get_switch_mac(config);
    this->switch_ip = get_switch_ip(config); 

    this->execution_thread = std::thread(&CorfuClient::execute, this, thread_id);
    pthread_t native_handle = this->execution_thread.native_handle();

    // Create a CPU set and add the desired core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(thread_id, &cpuset); // Pin to core 'i'

    // Set thread affinity
    int result = pthread_setaffinity_np(native_handle, sizeof(cpu_set_t), &cpuset);
    if (result != 0) {
        std::cerr << "Error setting thread affinity for thread " << this->execution_thread.get_id() << ": " << result << std::endl;
    }

    collect_stats = false;
}

CorfuClient::~CorfuClient() {
    spdlog::debug("Append thread joined!"); 
    spdlog::debug("Received thread joined!");
    //TODO subscribe_thread.join();
    /*for (uint64_t i = 0; i < num_work_threads; i++) {
        recv_threads[i].join();	
    }*/
    //duration_thread.join(); 
    execution_thread.join();
    spdlog::debug("Joined the client threads!");
    net->done();
}

void CorfuClient::execute(uint64_t thread_id) {
    spdlog::debug("At the beginning of execution here!");	
    spdlog::critical("Execute thread starting with TID = {}", gettid());
    std::string payload(payload_size, 'X');
    uint64_t cnt = 0;
    while (experiment_status()) {	    
 	uint32_t idx = append(std::make_unique<std::string>(payload)); // dummy(payload); //append(payload);
        spdlog::debug("The entry was given index: {}", idx);
	cnt += 1;
    }
    spdlog::critical("Total number of sent appends (NOT necessarily successful): {} from thread {}", cnt, thread_id);
}

void CorfuClient::reconfigure(uint64_t /*log_idx*/, CorfuStorage& /*failing_unit*/) {
    return;
}

uint64_t CorfuClient::append(std::unique_ptr<std::string> entry) {
    // we use the network object to send a request to the sequencer for the next log position
    std::unique_ptr<std::string> sequencing_packet = corfu_client_serialize_str_entry("", CORFU_GETTOKEN_PROTO_TYPE, cid, 0, 0);
    uint64_t allocated_packet_size = sequencing_packet->length() + 1;
    spdlog::debug("gettoken packet is of size: {}", allocated_packet_size);
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    memcpy(packet.get(), sequencing_packet->c_str(), allocated_packet_size);
    packet[allocated_packet_size - 1] = '\0';
    net->send_packet(std::move(packet), allocated_packet_size, static_cast<int>(PacketType::gettoken), get_pkt_eth_types()[PacketType::gettoken], switch_mac, inet_addr(switch_ip.c_str())); // request a log position

    char* msg = net->recv_packet();

    // start timer
    auto start_time = std::chrono::high_resolution_clock::now();
    auto read_time = std::chrono::high_resolution_clock::duration::zero();
    while (read_time < TIMEOUT && !msg) {
        msg = net->recv_packet();
        read_time = std::chrono::high_resolution_clock::now() - start_time;
    }

    if (!msg && read_time >= TIMEOUT) {
        spdlog::critical("We did not receive anything from server before timeout");
        return 1; // failure
    }

    // recv packet from the sequencer
    corfusequencer::Payload packet_contents = corfu_sequencer_deserialize_str_entry(std::make_unique<std::string>(msg));

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
        (void) sm; // CHANGE THIS
        std::unique_ptr<std::string> write_packet = corfu_client_serialize_str_entry(*entry, CORFU_APPEND_PROTO_TYPE, cid, log_idx, curr_epoch);
        uint64_t w_allocated_packet_size = write_packet->length() + 1;
        spdlog::debug("append packet is of size: {}", w_allocated_packet_size);
        std::unique_ptr<char[]> w_packet = std::make_unique<char[]>(w_allocated_packet_size);
        memcpy(w_packet.get(), write_packet->c_str(), w_allocated_packet_size);
        w_packet[w_allocated_packet_size - 1] = '\0';
        net->send_packet(std::move(w_packet), w_allocated_packet_size, static_cast<int>(PacketType::append), get_pkt_eth_types()[PacketType::append], switch_mac, inet_addr(switch_ip.c_str()));
        // CHECK HOW SHOULD I BE GETTING THE IPs OF SEND MACHINES???

        msg = net->recv_packet();

        // start timer
        auto start_time = std::chrono::high_resolution_clock::now();
        auto read_time = std::chrono::high_resolution_clock::duration::zero();
        while (read_time < TIMEOUT && !msg) {
            msg = net->recv_packet();
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

        corfustorage::Payload packet_contents = corfu_storage_deserialize_str_entry(std::make_unique<std::string>(msg));

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

std::string CorfuClient::read(uint64_t /*log_idx*/) {
    return nullptr;
}

uint64_t CorfuClient::fill(uint64_t /*idx*/) {
    return 0;
}

bool CorfuClient::trim(uint64_t /*log_idx*/) {
    // // loop through all of the replicas that have this log position
    // std::vector<std::shared_ptr<CorfuStorage>> send_machines;

    // // We use curr_epoch to grab the inner map of ranges for this specific epoch
    // for (const auto& config_kv : this->auxiliary[this->curr_epoch]) {
        
    //     std::pair<uint64_t, uint64_t> curr_range = config_kv.first;
    //     const std::vector<std::shared_ptr<CorfuStorage>>& server_range = config_kv.second;

    //     // Check if our log_idx falls inside {range_start, range_end}
    //     if (log_idx >= curr_range.first && log_idx < curr_range.second) {
    //         send_machines = server_range;
    //         break;
    //     }
    // }

    // if (send_machines.empty()) {
    //     spdlog::critical("Append: Unable to find send machines");
    //     return false; 
    // }

    // for (auto& sm : send_machines) {
    //     std::unique_ptr<std::string> delete_packet = Trace<std::string>::corfu_client_serialize_str_entry("", CORFU_TRIM_PROTO_TYPE, cid, log_idx, 0);
    //     net->add_to_send_queue(std::move(delete_packet), std::to_string(sm->ssid));

    //     std::unique_ptr<std::string> msg;
    //     // start timer
    //     auto start_time = std::chrono::high_resolution_clock::now();
    //     auto read_time = std::chrono::high_resolution_clock::duration::zero();
    //     while (read_time < TIMEOUT && !msg) {
    //         msg = net->read_from_recv_queue();
    //         read_time = std::chrono::high_resolution_clock::now() - start_time;
    //     }

    //     // never received ack
    //     if (!msg && read_time >= TIMEOUT) {
    //         spdlog::critical("Trim: We did not receive an ack before timeout");
    //         return false; // failure
    //     }

    //     corfustorage::Payload packet_contents = Trace<std::string>::corfu_storage_deserialize_str_entry(std::move(msg));
    //     if (packet_contents.packet_type() == CORFU_ACK_PROTO_TYPE) {
    //         // Note: corfu paper does not say to trim anything from local log representation,
    //         // so this is a possible optimization to add later :)
    //         continue;
    //     } else {
    //         spdlog::critical("Trim: We did not receive an ack :(");
    //         return false; // we did not receive an ack, so we must return a failure
    //     }

    // }
    // return true; // success
    return false;
}

/* Experiment Logistics */
void CorfuClient::wait_to_finish() {
    collect_stats = true;
    std::chrono::seconds sleep_duration(max_duration);
    std::this_thread::sleep_for(sleep_duration);
    spdlog::debug("Collecting statistics!");
    // stat->getAvgLatency();
    // stat->getThroughput(max_duration);
    // stat->getTotalOps();
    // stat->exportResultsToJson();
    collect_stats = false;
} 

void CorfuClient::wait_to_warmup() {
    std::chrono::seconds sleep_duration(warm_up);
    std::this_thread::sleep_for(sleep_duration);
}

void CorfuClient::wait_to_cooldown() {
    std::chrono::seconds sleep_duration(cool_down);
    std::this_thread::sleep_for(sleep_duration);
    spdlog::debug("End thread: {}", end_thread);
    end_thread = true;
} 

bool CorfuClient::experiment_status() {
    return !end_thread;
}

/* Header functions */
std::vector<int> CorfuClient::get_pkt_eth_types() {
    std::vector<int> eth_types = {};
    for(uint64_t i = 0; i < num_pkt_types; i++) {
        eth_types.push_back(get_eth_type(i));
    }
    return eth_types;
}

int CorfuClient::get_eth_type(uint64_t pkt_type) {
    if (PacketType(pkt_type) == PacketType::append) {
	spdlog::debug("Eth type is ETH_APPEND_REQ.");
        return ETH_APPEND_REQ;
    }
    spdlog::critical("No ethernet type found!");
    return -1; // no ethernet type found
}
