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

#include "corfuclient.pb.h"
#include "corfustorage.pb.h"
#include "corfusequencer.pb.h"

#define base_storage_port 5000
#define seq_port "4951"

CorfuClient::CorfuClient(std::string input_file, uint64_t thread_id) {
   // Get packet types for sending/receiving
   this->cid = thread_id;
   YAML::Node config = YAML::LoadFile(input_file);
   num_pkt_types = get_num_pkt_types(config);
//    std::vector<std::array<uint8_t, 6>> mac_addrs = get_dst_mac_addrs(config);
//    if (mac_addrs.size() < 1) {
//        spdlog::critical("Unable to parse mac address!");
//        throw;
//    }

    uint64_t base_send_port = get_send_port(config);
    uint64_t base_recv_port = get_recv_port(config);

    std::string unique_send_port = std::to_string(base_send_port + thread_id);
    std::string unique_recv_port = std::to_string(base_recv_port + thread_id);
  
   this->num_work_threads = get_num_client_threads(config);
   // Create network
   std::string multicast_ip = "";

    net = std::make_shared<Network>(
        unique_send_port, 
        unique_recv_port,
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
        
    spdlog::info("Corfu Client: net init");
    
    // Start receiving thread 
    for (size_t i = 0; i < num_pkt_types; i++) {
        this->pkt_q.insert(std::pair<PacketType, std::queue<std::unique_ptr<char[]>>>(PacketType(i), std::queue<std::unique_ptr<char[]>>()));
    }

    this->num_m_per_extent = get_num_m_per_extent(config);
    this->num_m_per_rep_set = get_num_m_per_rep_set(config);
    this->extent_size = get_extent_size(config);
    this->seq_ips = get_seq_ips(config);
    this->storage_ips = get_storage_ips(config);

    setup_auxiliary();

    // FOR DEBUGGING AUXILIARY FORMAT
    uint64_t epoch = 0;
    const std::map<uint64_t, std::map<std::pair<uint64_t, uint64_t>, std::vector<std::vector<uint64_t>>>>& aux = auxiliary;
    auto epoch_it = aux.find(epoch);
    if (epoch_it == aux.end()) {
        spdlog::debug("[]");
        return;
    }

    const auto& epoch_ranges = epoch_it->second;
    std::ostringstream oss;
    bool first_extent = true;

    for (const auto& [range, replica_sets] : epoch_ranges) {
        if (!first_extent) {
            oss << ", ";
        }
        first_extent = false;

        // Construct the replica sets string for this extent
        oss << "[";
        for (size_t i = 0; i < replica_sets.size(); ++i) {
            oss << "[";
            for (size_t j = 0; j < replica_sets[i].size(); ++j) {
                oss << replica_sets[i][j];
                if (j + 1 < replica_sets[i].size()) {
                    oss << ", ";
                }
            }
            oss << "]";
            if (i + 1 < replica_sets.size()) {
                oss << ", ";
            }
        }
        oss << "]";
    }
    
    spdlog::debug("{}", oss.str());
    // FOR DEBUGGING AUXILIARY ^


    // Protocol types
    // this->seq = SequencerType(get_sequencer_type(config));

    // Updating the log 
    this->started_append = false; 

    // this->payload_size = get_payload_size(config);
    // this->batch_size = num_work_threads * payload_size;
    this->batch_size = 100;
    spdlog::debug("Batch size: {}", batch_size);
    // this->stat = std::make_unique<Stats>(get_batch_size(config), get_batch_on(config), get_json_name(config), thread_id);
    // this->max_duration = get_experiment_duration(config);
    // this->warm_up = get_warm_up(config);
    // this->cool_down = get_cool_down(config);
    this->global_thread_id = thread_id;
   
    // this->switch_mac = get_switch_mac(config);
    // this->switch_ip = get_switch_ip(config); 

    // this->execution_thread = std::thread(&CorfuClient::execute, this, thread_id);
    // pthread_t native_handle = this->execution_thread.native_handle();

    // Create a CPU set and add the desired core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(thread_id, &cpuset); // Pin to core 'i'

    // Set thread affinity
    // int result = pthread_setaffinity_np(native_handle, sizeof(cpu_set_t), &cpuset);
    // if (result != 0) {
    //     std::cerr << "Error setting thread affinity for thread " << this->execution_thread.get_id() << ": " << result << std::endl;
    // }

    collect_stats = false;
}

CorfuClient::~CorfuClient() {
    //TODO subscribe_thread.join();
    /*for (uint64_t i = 0; i < num_work_threads; i++) {
        recv_threads[i].join();
    }*/
    //duration_thread.join(); 
    // execution_thread.join();
    spdlog::debug("Joined the client threads!");
    net->done();
}

void CorfuClient::execute(uint64_t /*thread_id*/) {
    // spdlog::debug("At the beginning of execution here!");	
    // spdlog::critical("Execute thread starting with TID = {}", gettid());
    // std::string payload(payload_size, 'X');
    // uint64_t cnt = 0;
    // while (experiment_status()) {	    
 	// uint32_t idx = append(std::make_unique<std::string>(payload)); // dummy(payload); //append(payload);
    // spdlog::debug("The entry was given index: {}", idx);
	// cnt += 1;
    // }
    // spdlog::critical("Total number of sent appends (NOT necessarily successful): {} from thread {}", cnt, thread_id);
}

void CorfuClient::setup_auxiliary() {
    // NOTE: this seems like a maybe costly way to do this there must be a better way to organize things, maybe
    //       instead we can change the config file to be nested so you directly go for the ip of each replica?
    uint64_t total_machines_per_extent = num_m_per_extent * num_m_per_rep_set;
    
    if (total_machines_per_extent == 0) {
        spdlog::critical("Setup Auxiliary: num_m_per_extent or num_m_per_rep_set can't be 0!");
        return;
    }

    // NOTE: currently this only supports 1 machines/extent size and 1 machines/replica set size
    //       can add future support if we want either of these sizes to vary between replica sets/extents
    if (storage_ips.size() % total_machines_per_extent != 0) {
        spdlog::critical("Setup Auxiliary: Total configured storage machines ({}) is not a multiple of machines required per extent ({})!", this->storage_ips.size(), total_machines_per_extent);
        return;
    }

    uint64_t num_extents = storage_ips.size() / total_machines_per_extent;
    spdlog::debug("num_storages = {}", storage_ips.size());
    uint64_t current_ssid = 0;

    std::map<std::pair<uint64_t, uint64_t>, std::vector<std::vector<uint64_t>>> epoch_ranges;

    for (uint64_t i = 0; i < num_extents; i++) {
        uint64_t range_start = i * extent_size;
        uint64_t range_end = (i + 1) * extent_size;

        std::vector<std::vector<uint64_t>> extent_replica_sets;

        for (uint64_t m = 0; m < num_m_per_extent; m++) {
            std::vector<uint64_t> replica_set;
            for (uint64_t r = 0; r < num_m_per_rep_set; r++) {
                replica_set.push_back(current_ssid++);
            }
            extent_replica_sets.push_back(replica_set);
        }

        epoch_ranges[{range_start, range_end}] = extent_replica_sets;
    }

    this->auxiliary[this->curr_epoch] = epoch_ranges;
    spdlog::info("Setup Auxiliary: Created layout for epoch {} containing {} extent(s) from range {} to {}", this->curr_epoch, num_extents, 0, num_extents * extent_size);
}   

void CorfuClient::reconfigure(uint64_t /*log_idx*/, CorfuStorage& /*failing_unit*/) {
    return;
}

// given a log index, figure out which machine set to contact
std::pair<uint64_t, std::vector<std::vector<uint64_t>>> CorfuClient::map(uint64_t log_idx) {
    std::map<std::pair<uint64_t, uint64_t>, std::vector<std::vector<uint64_t>>> ranges = auxiliary[curr_epoch];

    auto it = ranges.upper_bound({log_idx, std::numeric_limits<uint64_t>::max()});

    if (it != ranges.begin()) {
        --it;

        uint64_t range_start = it->first.first;
        uint64_t range_end = it->first.second;

        if (log_idx >= range_start && log_idx < range_end) {
            uint64_t relative_log_pos = log_idx - range_start;
            return std::pair<uint64_t, std::vector<std::vector<uint64_t>>>(relative_log_pos, it->second);
        }
    }
    spdlog::critical("Map: no send machines were found for the specified log_index, should probably reconfigure");
    return std::pair<uint64_t, std::vector<std::vector<uint64_t>>>(0, std::vector<std::vector<uint64_t>>{});
}

// error codes are lowk useless btw since this func returns the log_idx and the log_idx can be 1
uint32_t CorfuClient::append(std::string entry) {
    // SEQUENCING PART
    std::unique_ptr<std::string> sequencing_packet = corfu_client_serialize_str_entry("", CORFU_GETTOKEN_PROTO_TYPE, cid, 0, 0);
    uint64_t allocated_packet_size = sequencing_packet->length() + 1;
    spdlog::debug("Append: gettoken packet is of size: {}", allocated_packet_size);
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    memcpy(packet.get(), sequencing_packet->c_str(), allocated_packet_size);
    net->send_client_udp_packet(
        std::move(packet), 
        allocated_packet_size,
        seq_ips[0],
        seq_port // THIS IS HARDCODED, FIND A BETTER FIX TODO
    );
    char* msg = net->recv_packet();

    // start timer
    auto start_time = std::chrono::high_resolution_clock::now();
    auto read_time = std::chrono::high_resolution_clock::duration::zero();
    while (read_time < TIMEOUT && !msg) {
        msg = net->recv_packet();
        read_time = std::chrono::high_resolution_clock::now() - start_time;
    }

    if (!msg && read_time >= TIMEOUT) {
        // TODO i think you need to reconfigure here
        spdlog::critical("Append: We did not receive anything from sequencer before timeout");
        return 1; // failure
    }

    // recv packet from the sequencer
    corfusequencer::Payload packet_contents = corfu_sequencer_deserialize_str_entry(std::make_unique<std::string>(msg));

    uint64_t log_idx = packet_contents.send_token().token();

    spdlog::debug("Append: cid {} got index value {} from sequencer", cid, log_idx);

    // APPEND PART

    // MAPPING FUNCTION PART
    // find machines to send to
    std::pair<uint64_t, std::vector<std::vector<uint64_t>>> map_result = map(log_idx);
    uint64_t relative_log_pos = map_result.first;
    std::vector<std::vector<uint64_t>> send_machines = map_result.second;

    if (send_machines.empty()) {
        spdlog::critical("Append: Unable to find send machines");
        return 1; 
    }

    // mod relative log pos, even = first machine, odd = second machine
    uint64_t machine = relative_log_pos % num_m_per_extent;

    // for every replica in the set
    for (uint64_t sm : send_machines[machine]) {
        std::unique_ptr<std::string> append_packet = corfu_client_serialize_str_entry(entry, CORFU_APPEND_PROTO_TYPE, cid, relative_log_pos, curr_epoch);
        allocated_packet_size = append_packet->length() + 1;
        spdlog::debug("append packet is of size: {}", allocated_packet_size);
        packet = std::make_unique<char[]>(allocated_packet_size);
        memcpy(packet.get(), append_packet->c_str(), allocated_packet_size);

        spdlog::info("client {} is appending '{} to machine {}'", cid, entry, sm);

        net->send_client_udp_packet(
            std::move(packet), 
            allocated_packet_size, 
            storage_ips[sm],
            std::to_string(base_storage_port + sm) // THIS IS HARDCODED, FIND A BETTER FIX TODO
        );
        msg = net->recv_packet();

        // start timer
        start_time = std::chrono::high_resolution_clock::now();
        read_time = std::chrono::high_resolution_clock::duration::zero();
        while (read_time < TIMEOUT && !msg) {
            msg = net->recv_packet();
            read_time = std::chrono::high_resolution_clock::now() - start_time;
        }

        // must reconfigure if there's no response
        if (!msg && read_time >= TIMEOUT) {
            // TODO: THIS IS A BLACK BOX
            spdlog::info("Append: Must reconfigure because there was no response");
            // std::shared_ptr<CorfuStorage> failing_unit = send_machines[0];
            // reconfigure(log_idx, *failing_unit);
            // spdlog::info("Append: Just reconfigured, you should attempt to append again");
            return 1; // return error
        }

        // CHECK FOR ACK OR ERROR

        corfustorage::Payload packet_contents = corfu_storage_deserialize_str_entry(std::make_unique<std::string>(msg));

        if (packet_contents.packet_type() == CORFU_SEALED_PROTO_TYPE) {
            spdlog::info("Append: Must reconfigure because the current epoch was sealed");
            // std::shared_ptr<CorfuStorage> failing_unit = send_machines[0];
            // reconfigure(log_idx, *failing_unit);
            // spdlog::info("Append: Just reconfigured, you should attempt to append again");
            return 1;
        } else if (packet_contents.packet_type() == CORFU_DELETED_PROTO_TYPE) {
            spdlog::critical("Append: We got an err_deleted and now we're returning the error code");
            return 1;
        } else if (packet_contents.packet_type() == CORFU_WRITTEN_PROTO_TYPE) {
            spdlog::critical("Append: We got an err_written and now we're returning the error code");
            return 1;
        }

        // check to make sure that we've received an ack
        if (packet_contents.ack().ack_code()) {
            spdlog::info("Append: received an ack, continuing to write to next server if there are replicas");
            continue;
        } else {
            spdlog::critical("Append: We did not receive an ack :(");
            return 1;
        }
    }
    spdlog::info("Append: end of append");
    return (uint32_t) log_idx;
}

std::string CorfuClient::read(uint64_t log_idx) {
    // MAPPING FUNCTION PART
    // find machines in extent
    std::pair<uint64_t, std::vector<std::vector<uint64_t>>> map_result = map(log_idx);
    uint64_t relative_log_pos = map_result.first;
    std::vector<std::vector<uint64_t>> send_machines = map_result.second;

    if (send_machines.empty()) {
        spdlog::critical("Read: Unable to find send machines");
        return "should reconfigure";
    }

    // mod relative log pos, even = first machine, odd = second machine
    uint64_t machine = relative_log_pos % num_m_per_extent;

    spdlog::debug("reading from machine {}", machine);

    std::string content;
    // for every replica in the set
    // SEND READ REQ TO SERVER
    // client must go to the last replica in the set because they're not sure if the full set was written to or not
    // POSSIBLE OPTIMIZATION: can go to any replica in the set if client KNOWS this value has been written already (ex. via notification from writing client)
    std::unique_ptr<std::string> read_packet = corfu_client_serialize_str_entry("", CORFU_READ_PROTO_TYPE, cid, relative_log_pos, curr_epoch);
    uint64_t allocated_packet_size = read_packet->length() + 1;
    spdlog::debug("Read: read packet is of size: {}", allocated_packet_size);
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    memcpy(packet.get(), read_packet->c_str(), allocated_packet_size);

    net->send_client_udp_packet(
        std::move(packet), 
        allocated_packet_size,
        storage_ips[send_machines[machine].back()],
        std::to_string(base_storage_port + machine) // THIS IS HARDCODED, FIND A BETTER FIX
    );
    char* msg = net->recv_packet();

    // start timer
    auto start_time = std::chrono::high_resolution_clock::now();
    auto read_time = std::chrono::high_resolution_clock::duration::zero();
    while (read_time < TIMEOUT && !msg) {
        msg = net->recv_packet();
        read_time = std::chrono::high_resolution_clock::now() - start_time;
    }

    // must reconfigure if there's no response
    if (!msg && read_time >= TIMEOUT) {
        // TODO: THIS IS A BLACK BOX
        spdlog::info("Read: Must reconfigure because there was no response from storage server");
        // std::shared_ptr<CorfuStorage> failing_unit = send_machines[0];
        // reconfigure(log_idx, *failing_unit);
        // spdlog::info("Append: Just reconfigured, you should attempt to append again");
        return "reconfigured"; // return error
    }

    corfustorage::Payload packet_contents = corfu_storage_deserialize_str_entry(std::make_unique<std::string>(msg));

    if (packet_contents.packet_type() == CORFU_SEALED_PROTO_TYPE) {
        spdlog::info("Read: Must reconfigure because the current epoch was sealed");
        // std::shared_ptr<CorfuStorage> failing_unit = send_machines[0];
        // reconfigure(log_idx, *failing_unit);
        // spdlog::info("Read: Just reconfigured, you should attempt to read again");
        return "err_sealed";
    } else if (packet_contents.packet_type() == CORFU_DELETED_PROTO_TYPE) {
        spdlog::critical("Read: We got an err_deleted and now we're returning the error code");
        return "err_deleted";
    } else if (packet_contents.packet_type() == CORFU_UNWRITTEN_PROTO_TYPE) {
        spdlog::critical("Read: We got an err_unwritten and now we're returning the error code");
        // TODO: this needs to actually fill the holes in the replica set instead bc this means a client prolly failed when writing to the set
        return "err_unwritten";
    }

    // check to make sure that we've received an ack
    if (packet_contents.packet_type() == CORFU_STORE_READ_PROTO_TYPE) {
        spdlog::info("Read: got page contents");
        content = packet_contents.read().content();
    } else {
        spdlog::critical("Read: idk what we just received but it's not an error nor page contents :(");
    }
    spdlog::info("Read: end of read");
    return content;
}

uint64_t CorfuClient::fill(uint64_t idx) {
    std::string junk = "err_junk";

    // MAPPING FUNCTION PART
    // find machines to send to
    std::pair<uint64_t, std::vector<std::vector<uint64_t>>> map_result = map(idx);
    uint64_t relative_log_pos = map_result.first;
    std::vector<std::vector<uint64_t>> send_machines = map_result.second;

    if (send_machines.empty()) {
        spdlog::critical("Fill: Unable to find send machines");
        return 1; 
    }

    // mod relative log pos, even = first machine, odd = second machine
    uint64_t machine = relative_log_pos % num_m_per_extent;

    // for every replica in the set
    for (uint64_t sm : send_machines[machine]) {
        std::unique_ptr<std::string> fill_packet = corfu_client_serialize_str_entry(junk, CORFU_APPEND_PROTO_TYPE, cid, relative_log_pos, curr_epoch);
        uint64_t allocated_packet_size = fill_packet->length() + 1;
        spdlog::debug("fill packet is of size: {}", allocated_packet_size);
        std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
        memcpy(packet.get(), fill_packet->c_str(), allocated_packet_size);

        spdlog::info("client {} is filling idx {} on machine {}", cid, idx, sm);

        net->send_client_udp_packet(
            std::move(packet), 
            allocated_packet_size, 
            storage_ips[sm],
            std::to_string(base_storage_port + sm) // THIS IS HARDCODED, FIND A BETTER FIX TODO
        );
        char* msg = net->recv_packet();

        // start timer
        auto start_time = std::chrono::high_resolution_clock::now();
        auto read_time = std::chrono::high_resolution_clock::duration::zero();
        while (read_time < TIMEOUT && !msg) {
            msg = net->recv_packet();
            read_time = std::chrono::high_resolution_clock::now() - start_time;
        }

        // must reconfigure if there's no response
        if (!msg && read_time >= TIMEOUT) {
            // TODO: THIS IS A BLACK BOX
            spdlog::info("Fill: Must reconfigure because there was no response");
            // std::shared_ptr<CorfuStorage> failing_unit = send_machines[0];
            // reconfigure(log_idx, *failing_unit);
            // spdlog::info("Fill: Just reconfigured, you should attempt to append again");
            return 1; // return error
        }

        // CHECK FOR ACK OR ERROR

        corfustorage::Payload packet_contents = corfu_storage_deserialize_str_entry(std::make_unique<std::string>(msg));

        if (packet_contents.packet_type() == CORFU_SEALED_PROTO_TYPE) {
            spdlog::info("Fill: Must reconfigure because the current epoch was sealed");
            // std::shared_ptr<CorfuStorage> failing_unit = send_machines[0];
            // reconfigure(log_idx, *failing_unit);
            // spdlog::info("Fill: Just reconfigured, you should attempt to append again");
            // needs to redo Fill
            return 1;
        } else if (packet_contents.packet_type() == CORFU_WRITTEN_PROTO_TYPE) {
            junk = packet_contents.err_written().content();
            spdlog::info("Fill: received err_written with content {}", junk);
        }

        // check to make sure that we've received an ack
        if (packet_contents.ack().ack_code()) {
            spdlog::info("Fill: received an ack, continuing to write to next server if there are replicas");
            continue;
        } else {
            spdlog::critical("Fill: We did not receive an ack :(");
            return 1;
        }
    }
    spdlog::info("Fill: end of fill");
    return 0; // success
}

bool CorfuClient::trim(uint64_t log_idx) {
    std::pair<uint64_t, std::vector<std::vector<uint64_t>>> map_result = map(log_idx);
    uint64_t relative_log_pos = map_result.first;
    std::vector<std::vector<uint64_t>> send_machines = map_result.second;

    if (send_machines.empty()) {
        spdlog::critical("Trim: Unable to find send machines");
        return false; 
    }

    // mod relative log pos, even = first machine, odd = second machine
    uint64_t machine = relative_log_pos % num_m_per_extent;

    // for every replica in the set
    for (uint64_t sm : send_machines[machine]) {
        std::unique_ptr<std::string> trim_packet = corfu_client_serialize_str_entry("", CORFU_TRIM_PROTO_TYPE, cid, relative_log_pos, 0);
        uint64_t allocated_packet_size = trim_packet->length() + 1;
        spdlog::debug("Trim packet is of size: {}", allocated_packet_size);
        std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
        memcpy(packet.get(), trim_packet->c_str(), allocated_packet_size);

        spdlog::info("client {} is trimming idx {} on {}", cid, log_idx, sm);

        net->send_client_udp_packet(
            std::move(packet), 
            allocated_packet_size, 
            storage_ips[sm],
            std::to_string(base_storage_port + sm) // THIS IS HARDCODED, FIND A BETTER FIX TODO
        );
        char* msg = net->recv_packet();

        // start timer
        auto start_time = std::chrono::high_resolution_clock::now();
        auto read_time = std::chrono::high_resolution_clock::duration::zero();
        while (read_time < TIMEOUT && !msg) {
            msg = net->recv_packet();
            read_time = std::chrono::high_resolution_clock::now() - start_time;
        }

        // must reconfigure if there's no response
        if (!msg && read_time >= TIMEOUT) {
            // TODO: THIS IS A BLACK BOX
            spdlog::info("Trim: Must reconfigure because there was no response");
            // std::shared_ptr<CorfuStorage> failing_unit = send_machines[0];
            // reconfigure(log_idx, *failing_unit);
            // spdlog::info("Append: Just reconfigured, you should attempt to append again");
            return false; // return error
        }

        // CHECK FOR ACK OR ERROR

        corfustorage::Payload packet_contents = corfu_storage_deserialize_str_entry(std::make_unique<std::string>(msg));

        // check to make sure that we've received an ack
        if (packet_contents.ack().ack_code()) {
            spdlog::info("Trim: received an ack, continuing to write to next server if there are replicas");
            continue;
        } else {
            spdlog::critical("Trim: We did not receive an ack, should probably reconfigure");
            return false;
        }
    }
    return true;
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

uint64_t CorfuClient::getTail() {
    return 0;
}
void CorfuClient::subscribe(uint64_t idx) {
    idx++;
}
