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

#define ERROR 0

CorfuClient::CorfuClient(std::string input_file, uint64_t thread_id) {
   // Get packet types for sending/receiving
   YAML::Node config = YAML::LoadFile(input_file);
  
   this->num_work_threads = get_num_client_threads(config);
   this->cid = get_cli_id(config);
   this->thread_id = thread_id;
   // Create network
   std::string multicast_ip = "";

   this->send_port = std::to_string(get_send_port(config));
   this->recv_port = std::to_string(get_recv_port(config) + cid * num_work_threads + thread_id);

   std::string self_ip = get_self_ip(config);

    this->net = std::make_shared<Network>(
        send_port,
        recv_port,
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

    this->seq_recv_port = get_seq_recv_port(config);
    this->stor_recv_port = get_stor_recv_port(config);

    this->num_m_per_extent = get_num_m_per_extent(config);
    this->num_m_per_rep_set = get_num_m_per_rep_set(config);
    this->extent_size = get_extent_size(config);
    this->seq_ip = get_seq_ip(config);
    this->storage_ips = get_stor_ips(config);

    setup_auxiliary();

    // FOR DEBUGGING AUXILIARY FORMAT
    // uint64_t epoch = 0;
    // const std::map<uint64_t, std::map<std::pair<uint64_t, uint64_t>, std::vector<std::vector<uint64_t>>>>& aux = auxiliary;
    // auto epoch_it = aux.find(epoch);
    // if (epoch_it == aux.end()) {
    //     spdlog::debug("[]");
    //     return;
    // }

    // const auto& epoch_ranges = epoch_it->second;
    // std::ostringstream oss;
    // bool first_extent = true;

    // for (const auto& [range, replica_sets] : epoch_ranges) {
    //     if (!first_extent) {
    //         oss << ", ";
    //     }
    //     first_extent = false;

    //     // Construct the replica sets string for this extent
    //     oss << "[";
    //     for (size_t i = 0; i < replica_sets.size(); ++i) {
    //         oss << "[";
    //         for (size_t j = 0; j < replica_sets[i].size(); ++j) {
    //             oss << replica_sets[i][j];
    //             if (j + 1 < replica_sets[i].size()) {
    //                 oss << ", ";
    //             }
    //         }
    //         oss << "]";
    //         if (i + 1 < replica_sets.size()) {
    //             oss << ", ";
    //         }
    //     }
    //     oss << "]";
    // }
    
    // spdlog::debug("{}", oss.str());
    // FOR DEBUGGING AUXILIARY ^


    // Protocol types
    // this->seq = SequencerType(get_sequencer_type(config));

    // Updating the log 
    this->started_append = false;

    this->payload_size = get_payload_size(config);
    this->batch_size = num_work_threads * payload_size;
    this->stat = std::make_unique<Stats>(get_batch_size(config), get_batch_on(config), get_json_name(config), thread_id, self_ip);

    this->warm_up = get_warm_up(config);
    this->cool_down = get_cool_down(config);
    this->max_duration = get_experiment_duration(config) - warm_up - cool_down; // Duration of the actual experiment
    this->global_thread_id = thread_id;

    this->cnt = 0;
    this->collect_stats = false;
    this->testing_append = false;
    this->end_thread = false;

    this->collect_stats = false;
}

CorfuClient::~CorfuClient() {
    net->done();
    if (testing_append) {
        execution_thread.join();
    }
    spdlog::debug("Joined the client threads!");
}

void CorfuClient::launch_append_execute() {
    this->execution_thread = std::thread(&CorfuClient::execute, this, global_thread_id);
    this->testing_append = true;
}

void CorfuClient::execute(uint64_t thread_id) {
    spdlog::debug("At the beginning of execution here!");
    spdlog::critical("Execute thread starting with TID = {}", gettid());
    
    // Generate the dummy payload based on payload_size config
    std::string payload(payload_size, 'X');
    uint64_t total_count = 0;
    int i = 0;
    while (experiment_status()) {
    // while (i < 3) {
        uint32_t idx = append(payload);
        total_count += 1;
        spdlog::debug("The entry was given index: {}", idx);
        i++;
    }
    spdlog::critical("Total number of sent appends (NOT necessarily successful): {} from thread {}", total_count, thread_id);
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


// update so boolean in toml determines if just sequencing or both sequencing & writing
uint32_t CorfuClient::append(const std::string& /*entry*/) {
    double start_time = collect_stats ? stat->getStartLat() : 0;
    
    // SEQUENCING PART
    std::unique_ptr<std::string> sequencing_packet = corfu_client_serialize_str_entry("", CORFU_GETTOKEN_PROTO_TYPE, cid, thread_id, 0, 0);
    uint64_t allocated_packet_size = sequencing_packet->length() + 1;
    spdlog::debug("Append: gettoken packet is of size: {}", allocated_packet_size);
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    memcpy(packet.get(), sequencing_packet->c_str(), allocated_packet_size);
    spdlog::debug("sending to {}:{}", seq_ip, seq_recv_port);
    
    net->send_client_udp_packet(
        std::move(packet), 
        allocated_packet_size,
        seq_ip,
        seq_recv_port
    );
    
    char* msg = net->recv_packet();

    if (end_thread) {
        return 0;
    }
    
    if (!msg) {
        spdlog::debug("problem here");
        return ERROR;
    }

    // start timer
    // auto msg_time = std::chrono::high_resolution_clock::now();
    // auto read_time = std::chrono::high_resolution_clock::duration::zero();
    // while (read_time < TIMEOUT && !msg) {
    //     msg = net->recv_packet();
    //     read_time = std::chrono::high_resolution_clock::now() - msg_time;
    // }

    // if (!msg && read_time >= TIMEOUT) {
    //     // TODO i think you need to reconfigure here
    //     spdlog::critical("Append: We did not receive anything from sequencer before timeout");
    //     return ERROR; // failure
    // }

    // recv packet from the sequencer
    corfusequencer::Payload packet_contents = corfu_sequencer_deserialize_str_entry(std::make_unique<std::string>(msg));

    if (packet_contents.packet_type() != CORFU_GETTOKEN_REPLY_PROTO_TYPE) {
        return ERROR;
    }

    uint64_t log_idx = packet_contents.token();

    spdlog::debug("Append: cid {} got index value {} from sequencer", cid, log_idx);

    // // APPEND PART

    // // MAPPING FUNCTION PART
    // // find machines to send to
    // std::pair<uint64_t, std::vector<std::vector<uint64_t>>> map_result = map(log_idx);
    // uint64_t relative_log_pos = map_result.first;
    // std::vector<std::vector<uint64_t>> send_machines = map_result.second;

    // if (send_machines.empty()) {
    //     spdlog::critical("Append: Unable to find send machines");
    //     return ERROR; 
    // }

    // // mod relative log pos, even = first machine, odd = second machine
    // uint64_t machine = relative_log_pos % num_m_per_extent;

    // // for every replica in the set
    // for (uint64_t sm : send_machines[machine]) {
    //     std::unique_ptr<std::string> append_packet = corfu_client_serialize_str_entry(entry, CORFU_APPEND_PROTO_TYPE, cid, thread_id, relative_log_pos, curr_epoch);
    //     allocated_packet_size = append_packet->length() + 1;
    //     spdlog::debug("append packet is of size: {}", allocated_packet_size);
    //     packet = std::make_unique<char[]>(allocated_packet_size);
    //     memcpy(packet.get(), append_packet->c_str(), allocated_packet_size);

    //     // spdlog::info("client {} is appending to machine {}", cid, sm);

    //     net->send_client_udp_packet(
    //         std::move(packet), 
    //         allocated_packet_size, 
    //         storage_ips[sm],
    //         stor_recv_port
    //     );
    //     msg = net->recv_packet();

    //     if (end_thread) {
    //         return 0;
    //     }

    //     if (!msg) {
    //         spdlog::debug("send machine didn't send back a packet");
    //         return ERROR;
    //     }

    //     // start timer
    //     // msg_time = std::chrono::high_resolution_clock::now();
    //     // read_time = std::chrono::high_resolution_clock::duration::zero();
    //     // while (read_time < TIMEOUT && !msg) {
    //     //     msg = net->recv_packet();
    //     //     read_time = std::chrono::high_resolution_clock::now() - msg_time;
    //     // }

    //     // // must reconfigure if there's no response
    //     // if (!msg && read_time >= TIMEOUT) {
    //     //     // TODO: THIS IS A BLACK BOX
    //     //     spdlog::info("Append: Must reconfigure because there was no response");
    //     //     // std::shared_ptr<CorfuStorage> failing_unit = send_machines[0];
    //     //     // reconfigure(log_idx, *failing_unit);
    //     //     // spdlog::info("Append: Just reconfigured, you should attempt to append again");
    //     //     return ERROR; // return error
    //     // }

    //     // CHECK FOR ACK OR ERROR

    //     corfustorage::Payload packet_contents = corfu_storage_deserialize_str_entry(std::make_unique<std::string>(msg));

    //     if (packet_contents.packet_type() == CORFU_SEALED_PROTO_TYPE) {
    //         spdlog::info("Append: Must reconfigure because the current epoch was sealed");
    //         // std::shared_ptr<CorfuStorage> failing_unit = send_machines[0];
    //         // reconfigure(log_idx, *failing_unit);
    //         // spdlog::info("Append: Just reconfigured, you should attempt to append again");
    //         return ERROR;
    //     } else if (packet_contents.packet_type() == CORFU_DELETED_PROTO_TYPE) {
    //         spdlog::critical("Append: We got an err_deleted and now we're returning the error code");
    //         return ERROR;
    //     } else if (packet_contents.packet_type() == CORFU_WRITTEN_PROTO_TYPE) {
    //         spdlog::critical("Append: We got an err_written and now we're returning the error code");
    //         return ERROR;
    //     }

    //     // check to make sure that we've received an ack
    //     if (packet_contents.packet_type() == CORFU_ACK_PROTO_TYPE) {
    //         spdlog::info("Append: received an ack, continuing to write to next server if there are replicas");
    //         continue;
    //     } else {
    //         spdlog::critical("Append: We did not receive an ack :(");
    //         return ERROR;
    //     }
    // }

    if (collect_stats && start_time > 0) {
        stat->getDuration(start_time);
        stat->addOp();
    }
    cnt += 1;

    // spdlog::info("Append: end of append");
    return (uint32_t) log_idx;
}

std::string CorfuClient::read(uint64_t log_idx) {
    // std::chrono::high_resolution_clock::time_point start_time = 0;
    // if (collect_stats) {
    //     start_time = stat->getStartLat();
    // }

    // MAPPING FUNCTION PART
    // find machines in extent
    std::pair<uint64_t, std::vector<std::vector<uint64_t>>> map_result = map(log_idx);
    uint64_t relative_log_pos = map_result.first;
    std::vector<std::vector<uint64_t>> send_machines = map_result.second;

    if (send_machines.empty()) {
        spdlog::critical("Read: Unable to find send machines");
        return "should reconfigure";
    }

    // mod relative log pos, even = first machine, odd = second machine if num m per extent = 2
    uint64_t machine = relative_log_pos % num_m_per_extent;

    spdlog::debug("reading from machine {}", machine);

    std::string content;
    // for every replica in the set
    // SEND READ REQ TO SERVER
    // client must go to the last replica in the set because they're not sure if the full set was written to or not
    // POSSIBLE OPTIMIZATION: can go to any replica in the set if client KNOWS this value has been written already (ex. via notification from writing client)
    std::unique_ptr<std::string> read_packet = corfu_client_serialize_str_entry("", CORFU_READ_PROTO_TYPE, cid, thread_id, relative_log_pos, curr_epoch);
    uint64_t allocated_packet_size = read_packet->length() + 1;
    spdlog::debug("Read: read packet is of size: {}", allocated_packet_size);
    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    memcpy(packet.get(), read_packet->c_str(), allocated_packet_size);

    net->send_client_udp_packet(
        std::move(packet), 
        allocated_packet_size,
        storage_ips[send_machines[machine].back()],
        recv_port
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

    // if (collect_stats) {
    //     stat->addResult(start_time, contents.size()); 
    // }
    
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
        return ERROR; 
    }

    // mod relative log pos, even = first machine, odd = second machine
    uint64_t machine = relative_log_pos % num_m_per_extent;

    // for every replica in the set
    for (uint64_t sm : send_machines[machine]) {
        std::unique_ptr<std::string> fill_packet = corfu_client_serialize_str_entry(junk, CORFU_APPEND_PROTO_TYPE, cid, thread_id, relative_log_pos, curr_epoch);
        uint64_t allocated_packet_size = fill_packet->length() + 1;
        spdlog::debug("fill packet is of size: {}", allocated_packet_size);
        std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
        memcpy(packet.get(), fill_packet->c_str(), allocated_packet_size);

        spdlog::info("client {} is filling idx {} on machine {}", cid, idx, sm);

        net->send_client_udp_packet(
            std::move(packet), 
            allocated_packet_size, 
            storage_ips[sm],
            recv_port
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
            return ERROR; // return error
        }

        // CHECK FOR ACK OR ERROR

        corfustorage::Payload packet_contents = corfu_storage_deserialize_str_entry(std::make_unique<std::string>(msg));

        if (packet_contents.packet_type() == CORFU_SEALED_PROTO_TYPE) {
            spdlog::info("Fill: Must reconfigure because the current epoch was sealed");
            // std::shared_ptr<CorfuStorage> failing_unit = send_machines[0];
            // reconfigure(log_idx, *failing_unit);
            // spdlog::info("Fill: Just reconfigured, you should attempt to append again");
            // needs to redo Fill
            return ERROR;
        } else if (packet_contents.packet_type() == CORFU_WRITTEN_PROTO_TYPE) {
            junk = packet_contents.err_written().content();
            spdlog::info("Fill: received err_written with content {}", junk);
        }

        // check to make sure that we've received an ack
        if (packet_contents.packet_type() == CORFU_ACK_PROTO_TYPE) {
            spdlog::info("Fill: received an ack, continuing to write to next server if there are replicas");
            continue;
        } else {
            spdlog::critical("Fill: We did not receive an ack :(");
            return ERROR;
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
        std::unique_ptr<std::string> trim_packet = corfu_client_serialize_str_entry("", CORFU_TRIM_PROTO_TYPE, cid, thread_id, relative_log_pos, 0);
        uint64_t allocated_packet_size = trim_packet->length() + 1;
        spdlog::debug("Trim packet is of size: {}", allocated_packet_size);
        std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
        memcpy(packet.get(), trim_packet->c_str(), allocated_packet_size);

        spdlog::info("client {} is trimming idx {} on {}", cid, log_idx, sm);

        net->send_client_udp_packet(
            std::move(packet), 
            allocated_packet_size, 
            storage_ips[sm],
            recv_port
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
        if (packet_contents.packet_type() == CORFU_ACK_PROTO_TYPE) {
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
void CorfuClient::wait_to_finish(bool is_append) {
    collect_stats = true;
    spdlog::debug("Collecting statistics for {}!", max_duration);
    std::chrono::seconds sleep_duration(max_duration);
    std::this_thread::sleep_for(sleep_duration);

    if (is_append) {
	    spdlog::critical("========================= CLIENT STATISTICS ================================");
    	// spdlog::critical("APPEND Highest index seen is: {}", highest_idx_seen);
    	spdlog::critical("APPEND sent {} appends in {} seconds.", cnt, max_duration);
    	spdlog::critical("APPEND STATISTICS: lat is: {}, tput: {}, total ops: {}", stat->getAvgLatency(), stat->getThroughput(max_duration), stat->getTotalOps());
    	stat->getAvgLatency();
    	stat->getThroughput(max_duration);
    	stat->getTotalOps();
        stat->dumpAllLatencies();
    	stat->exportResultsToJson();
    } else {
	    // spdlog::critical("========================= CLIENT STATISTICS ================================");
	    // spdlog::critical("READ sent {} appends in {} seconds.", read_cntr, max_duration);
    	// spdlog::critical("READ STATISTICS: lat is: {}, tput: {}, total ops: {}", read_stat->getAvgLatency(), read_stat->getThroughput(max_duration), read_stat->getTotalOps());
	    // read_stat->getAvgLatency();
    	// read_stat->getThroughput(max_duration);
    	// read_stat->getTotalOps();
    	// read_stat->exportResultsToJson();
    }
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
