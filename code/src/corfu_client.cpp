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
   this->recv_port = get_recv_port(config) + cid * num_work_threads + thread_id;

   std::string self_ip = get_self_ip(config);

    this->net = std::make_shared<Network>(
        send_port,
        std::to_string(recv_port),
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

    this->corfu_hdr = std::make_unique<struct corfu_cli_header>();
    corfu_hdr.get()->proto_type = htons(0);
    corfu_hdr.get()->thread_id = htonl(thread_id);
    corfu_hdr.get()->recv_port = htons(recv_port);
    this->corfu_append_hdr = std::make_unique<struct corfu_append>();
    corfu_append_hdr.get()->log_idx = htonl(0);
    corfu_append_hdr.get()->curr_epoch = htonl(0);
    corfu_append_hdr.get()->entry_size = htonl(0);
    struct in_addr addr;
    if (inet_pton(AF_INET, self_ip.c_str(), &addr) != 1) {
        throw std::runtime_error("Invalid IP address format");
    }
    uint32_t self_client_ip = addr.s_addr;
    corfu_hdr.get()->client_ip = self_client_ip;

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
    this->full_append = get_full_append(config);
    this->just_storage = get_just_storage(config);
}

CorfuClient::~CorfuClient() {
    net->done();
    if (testing_append) {
        execution_thread.join();
    }
    // spdlog::debug("Joined the client threads!");
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
    while (experiment_status()) {
    // while (total_count < 3) {
        uint32_t idx = append(payload);
        total_count += 1;
        spdlog::debug("The entry was given index: {}", idx);
    }
    spdlog::critical("Total number of sent appends (NOT necessarily successful): {} from thread {}", total_count, thread_id);
}

void CorfuClient::setup_auxiliary() {
    // NOTE: this seems like a maybe costly way to do this there must be a better way to organize things, maybe
    //       instead we can change the config file to be nested so you directly go for the ip of each replica?
    uint64_t total_machines_per_extent = num_m_per_extent * num_m_per_rep_set;
    
    if (total_machines_per_extent == 0) {
        // spdlog::critical("Setup Auxiliary: num_m_per_extent or num_m_per_rep_set can't be 0!");
        return;
    }

    // NOTE: currently this only supports 1 machines/extent size and 1 machines/replica set size
    //       can add future support if we want either of these sizes to vary between replica sets/extents
    if (storage_ips.size() % total_machines_per_extent != 0) {
        // spdlog::critical("Setup Auxiliary: Total configured storage machines ({}) is not a multiple of machines required per extent ({})!", this->storage_ips.size(), total_machines_per_extent);
        return;
    }

    uint64_t num_extents = storage_ips.size() / total_machines_per_extent;
    // spdlog::debug("num_storages = {}", storage_ips.size());
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
    // spdlog::info("Setup Auxiliary: Created layout for epoch {} containing {} extent(s) from range {} to {}", this->curr_epoch, num_extents, 0, num_extents * extent_size);
}   

bool CorfuClient::update_projection() {
    // epoch = log idx for layout
    uint32_t target_epoch = curr_epoch + 1;
    
    std::string layout_data = layout_client->read(target_epoch);
    
    if (layout_data.empty() || layout_data == "err_unwritten") {
        return false; // no new configuration exists yet
    }

    struct corfu_projection_layout* layout = (struct corfu_projection_layout*)layout_data.data();
    
    this->curr_epoch = ntohl(layout->epoch);
    this->num_m_per_extent = ntohl(layout->num_machines_per_extent);
    uint32_t num_extents = ntohl(layout->num_extents);
    
    // extract the new IP addresses dynamically appended after the struct
    uint32_t* ips = (uint32_t*)(layout_data.data() + get_corfu_layout_header_size());
    this->storage_ips.clear();
    for(size_t i = 0; i < (num_extents * num_m_per_extent); i++) {
        uint32_t ip = ntohl(ips[i]);
        
        // Convert the int back to an ipv4 string format
        std::string ip_str = std::to_string((ip >> 24) & 0xFF) + "." +
                             std::to_string((ip >> 16) & 0xFF) + "." +
                             std::to_string((ip >> 8) & 0xFF) + "." +
                             std::to_string(ip & 0xFF);
                             
        this->storage_ips.push_back(ip_str);
    }
    
    return true;
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
    // spdlog::critical("Map: no send machines were found for the specified log_index, should probably reconfigure");
    return std::pair<uint64_t, std::vector<std::vector<uint64_t>>>(0, std::vector<std::vector<uint64_t>>{});
}


// update so boolean in toml determines if just sequencing or both sequencing & writing
uint32_t CorfuClient::append(const std::string& entry) {
    uint32_t log_idx = 0;
    double start_time = collect_stats ? stat->getStartLat() : 0;
    
    // SEQUENCING PART
    if (!just_storage) {
        size_t size_of_type_hdr = get_corfu_cli_header_size();
        corfu_hdr.get()->proto_type = htons(CORFU_GETTOKEN_PROTO_TYPE);
        // spdlog::debug("Corfu header: {}, size of: {}", ntohs(corfu_hdr.get()->proto_type), size_of_type_hdr);
        uint64_t allocated_packet_size = size_of_type_hdr + 1;

        // Only need to read from the map once to get the queue
        // Create packet buffer which will be sent  
        std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);

        memcpy(packet.get(), reinterpret_cast<const char*>(corfu_hdr.get()), size_of_type_hdr + 1);

        // spdlog::debug("sending to {}:{}", seq_ip, seq_recv_port);
        
        // TODO: CHECK IF THIS SHOULD BE SENT WITH SENDTO SYSCALL bc sending to socket lmao
        net->send_client_udp_packet(
            std::move(packet), 
            allocated_packet_size,
            seq_ip,
            seq_recv_port
        );

        while (!end_thread) {
        
            char* msg = net->recv_packet();

            if (!msg) continue;

            // start timer
            // auto msg_time = std::chrono::high_resolution_clock::now();
            // auto read_time = std::chrono::high_resolution_clock::duration::zero();
            // while (read_time < TIMEOUT && !msg) {
            //     msg = net->recv_packet();
            //     read_time = std::chrono::high_resolution_clock::now() - msg_time;
            // }

            // if (!msg && read_time >= TIMEOUT) {
            //     // TODO i think you need to reconfigure here
            //     // spdlog::critical("Append: We did not receive anything from sequencer before timeout");
            //     return ERROR; // failure
            // }

            // recv packet from the sequencer
            // corfusequencer::Payload packet_contents = corfu_sequencer_deserialize_str_entry(std::make_unique<std::string>(msg));

            // if (packet_contents.packet_type() != CORFU_GETTOKEN_REPLY_PROTO_TYPE) {
            //     return ERROR;
            // }

            struct corfu_seq_header* recv_hdr = (struct corfu_seq_header*)msg;
            if (ntohs(recv_hdr->proto_type) != CORFU_GETTOKEN_REPLY_PROTO_TYPE) {
                continue;
            }

            struct corfu_gettoken_reply* reply = (struct corfu_gettoken_reply*)(msg + sizeof(struct corfu_seq_header));
            log_idx = ntohl(reply->log_idx);
            break;
        }
        spdlog::debug("Append: cid {} got index value {} from sequencer", cid, log_idx);
    }

    if (full_append) {
        // APPEND PART

        // MAPPING FUNCTION PART
        // find machines to send to
        std::pair<uint64_t, std::vector<std::vector<uint64_t>>> map_result = map(log_idx);
        uint64_t relative_log_pos = map_result.first;
        std::vector<std::vector<uint64_t>> send_machines = map_result.second;

        if (send_machines.empty()) {
            // spdlog::critical("Append: Unable to find send machines");
            return ERROR;
        }

        // mod relative log pos, even = first machine, odd = second machine
        uint64_t machine = relative_log_pos % num_m_per_extent;

        // for every replica in the set
        for (uint64_t sm : send_machines[machine]) {
            // NEED TO VERIFY CID, TID, IDX returned in response otherwise we could be reading an older packet not meant for us
            // std::unique_ptr<std::string> append_packet = corfu_client_serialize_str_entry(entry, CORFU_APPEND_PROTO_TYPE, cid, thread_id, relative_log_pos, curr_epoch);
            // uint64_t allocated_packet_size = append_packet->length() + 1;
            // // spdlog::debug("append packet is of size: {}", allocated_packet_size);
            // std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
            // memcpy(packet.get(), append_packet->c_str(), allocated_packet_size);


            size_t size_of_type_hdr = get_corfu_append_header_size();
            size_t size_of_hdr = get_corfu_cli_header_size();
            corfu_hdr.get()->proto_type = htons(CORFU_APPEND_PROTO_TYPE);
            spdlog::debug("Type header: {}, size of corfu hdr: {} and corfu append hdr: {}", corfu_hdr.get()->proto_type, size_of_hdr, size_of_type_hdr);
            uint64_t allocated_packet_size = size_of_type_hdr + size_of_hdr + payload_size + 1;

            // Only need to read from the map once to get the queue
            // Create packet buffer which will be sent
            std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
            corfu_append_hdr.get()->log_idx = htonl(log_idx);
            corfu_append_hdr.get()->curr_epoch = htonl(curr_epoch);
            corfu_append_hdr.get()->entry_size = htonl(payload_size);
            memcpy(packet.get(), reinterpret_cast<const char*>(corfu_hdr.get()), size_of_hdr);
            memcpy(packet.get() + size_of_hdr, reinterpret_cast<const char*>(corfu_append_hdr.get()), size_of_type_hdr);
            memcpy(packet.get() + size_of_hdr + size_of_type_hdr, entry.c_str(), payload_size + 1);

            spdlog::debug("Size of packet: {} and size of payload: {} and size of hdr: {}", allocated_packet_size, payload_size, size_of_hdr);
            spdlog::debug("client {} is appending to machine {}", cid, sm);

            net->send_client_udp_packet(
                std::move(packet), 
                allocated_packet_size, 
                storage_ips[sm],
                stor_recv_port
            );
            char* msg = net->recv_packet();

            if (end_thread) {
                return ERROR;
            }

            if (!msg) {
                // spdlog::debug("send machine didn't send back a packet");
                return ERROR;
            }

            // start timer
            // msg_time = std::chrono::high_resolution_clock::now();
            // read_time = std::chrono::high_resolution_clock::duration::zero();
            // while (read_time < TIMEOUT && !msg) {
            //     msg = net->recv_packet();
            //     read_time = std::chrono::high_resolution_clock::now() - msg_time;
            // }

            // // must reconfigure if there's no response
            // if (!msg && read_time >= TIMEOUT) {
            //     // TODO: THIS IS A BLACK BOX
            //     // spdlog::info("Append: Must reconfigure because there was no response");
            //     // std::shared_ptr<CorfuStorage> failing_unit = send_machines[0];
            //     // reconfigure(log_idx, *failing_unit);
            //     // // spdlog::info("Append: Just reconfigured, you should attempt to append again");
            //     return ERROR; // return error
            // }

            // CHECK FOR ACK OR ERROR

            struct corfu_storage_header* recv_hdr = (struct corfu_storage_header*)msg;
            uint16_t proto = ntohs(recv_hdr->proto_type);

            if (proto == CORFU_ACK_PROTO_TYPE) {
                spdlog::debug("Append: received an ack, continuing to write to next server if there are replicas");
                continue;
            }

            if (proto == CORFU_SEALED_PROTO_TYPE) {
                if (update_projection()) { // check if the projection was updated & copy it locally and try the read again
                    return append(entry);
                }
                return ERROR;
            } else if (proto == CORFU_DELETED_PROTO_TYPE) {
                spdlog::debug("Append: We got an err_deleted and now we're returning the error code");
                return ERROR;
            } else if (proto == CORFU_WRITTEN_PROTO_TYPE) {
                spdlog::debug("Append: We got an err_written and now we're returning the error code");
                return ERROR;
            } else {
                spdlog::debug("Append: We did not receive an ack or valid error :(");
                return ERROR;
            }
        }
    }
    
    if (collect_stats && start_time > 0) {
        stat->getDuration(start_time);
        stat->addOp();
    }
    cnt += 1;

    return (uint32_t) log_idx;
}

std::string CorfuClient::read(uint64_t log_idx) {
    std::pair<uint64_t, std::vector<std::vector<uint64_t>>> map_result = map(log_idx);
    uint64_t relative_log_pos = map_result.first;
    std::vector<std::vector<uint64_t>> send_machines = map_result.second;

    if (send_machines.empty()) {
        return "should reconfigure";
    }

    uint64_t machine = relative_log_pos % num_m_per_extent;

    size_t size_of_hdr = get_corfu_cli_header_size();
    size_t size_of_type_hdr = get_corfu_read_header_size();
    uint64_t allocated_packet_size = size_of_hdr + size_of_type_hdr + 1;

    std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
    
    corfu_hdr.get()->proto_type = htons(CORFU_READ_PROTO_TYPE);
    corfu_append_hdr.get()->log_idx = htonl(log_idx);
    corfu_append_hdr.get()->curr_epoch = htonl(curr_epoch);

    memcpy(packet.get(), reinterpret_cast<const char*>(corfu_hdr.get()), size_of_hdr);
    memcpy(packet.get() + size_of_hdr, reinterpret_cast<const char*>(corfu_append_hdr.get()), size_of_type_hdr);

    net->send_client_udp_packet(
        std::move(packet), 
        allocated_packet_size,
        storage_ips[send_machines[machine].back()],
        std::to_string(recv_port)
    );
    
    char* msg = net->recv_packet();
    auto start_time = std::chrono::high_resolution_clock::now();
    auto read_time = std::chrono::high_resolution_clock::duration::zero();
    
    while (read_time < TIMEOUT && !msg) {
        msg = net->recv_packet();
        read_time = std::chrono::high_resolution_clock::now() - start_time;
    }

    if (!msg && read_time >= TIMEOUT) {
        return "reconfigured";
    }

    struct corfu_storage_header* recv_hdr = (struct corfu_storage_header*)msg;
    uint16_t proto = ntohs(recv_hdr->proto_type);

    if (proto == CORFU_SEALED_PROTO_TYPE) {
        if (update_projection()) { // check if the projection was updated & copy it locally and try the read again
            return read(log_idx);
        }
        return "err_sealed"; // otherwise return err_sealed to mark we need to reconfigure
    }
    if (proto == CORFU_DELETED_PROTO_TYPE) return "err_deleted";
    if (proto == CORFU_UNWRITTEN_PROTO_TYPE) return "err_unwritten";

    if (proto == CORFU_STORE_READ_PROTO_TYPE) {
        char* content = msg + sizeof(struct corfu_storage_header);
        return std::string(content);
    }
    
    return "";
}

uint64_t CorfuClient::fill(uint64_t idx) {
std::string junk = "err_junk";
    std::pair<uint64_t, std::vector<std::vector<uint64_t>>> map_result = map(idx);
    uint64_t relative_log_pos = map_result.first;
    std::vector<std::vector<uint64_t>> send_machines = map_result.second;

    if (send_machines.empty()) return ERROR; 

    uint64_t machine = relative_log_pos % num_m_per_extent;
    size_t size_of_hdr = get_corfu_cli_header_size();
    size_t size_of_type_hdr = get_corfu_append_header_size();
    uint64_t allocated_packet_size = size_of_hdr + size_of_type_hdr + junk.size() + 1;

    for (uint64_t sm : send_machines[machine]) {
        std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
        
        corfu_hdr.get()->proto_type = htons(CORFU_APPEND_PROTO_TYPE);
        corfu_append_hdr.get()->log_idx = htonl(relative_log_pos);
        corfu_append_hdr.get()->curr_epoch = htonl(curr_epoch);
        corfu_append_hdr.get()->entry_size = htonl(junk.size());

        memcpy(packet.get(), reinterpret_cast<const char*>(corfu_hdr.get()), size_of_hdr);
        memcpy(packet.get() + size_of_hdr, reinterpret_cast<const char*>(corfu_append_hdr.get()), size_of_type_hdr);
        memcpy(packet.get() + size_of_hdr + size_of_type_hdr, junk.c_str(), junk.size() + 1);

        net->send_client_udp_packet(
            std::move(packet), 
            allocated_packet_size, 
            storage_ips[sm],
            std::to_string(recv_port)
        );
        
        char* msg = net->recv_packet();
        auto start_time = std::chrono::high_resolution_clock::now();
        auto read_time = std::chrono::high_resolution_clock::duration::zero();
        
        while (read_time < TIMEOUT && !msg) {
            msg = net->recv_packet();
            read_time = std::chrono::high_resolution_clock::now() - start_time;
        }

        if (!msg && read_time >= TIMEOUT) return ERROR;

        struct corfu_storage_header* recv_hdr = (struct corfu_storage_header*)msg;
        uint16_t proto = ntohs(recv_hdr->proto_type);

        if (proto == CORFU_SEALED_PROTO_TYPE) {
            if (update_projection()) { // check if the projection was updated & copy it locally and try the read again
                return fill(idx);
            }
            return ERROR; // otherwise return err_sealed to mark we need to reconfigure
        }
        if (proto == CORFU_WRITTEN_PROTO_TYPE) {
            // already written --> skip
        } else if (proto == CORFU_ACK_PROTO_TYPE) {
            continue;
        } else {
            return ERROR;
        }
    }
    return 0;
}

bool CorfuClient::trim(uint64_t log_idx) {
    std::pair<uint64_t, std::vector<std::vector<uint64_t>>> map_result = map(log_idx);
    uint64_t relative_log_pos = map_result.first;
    std::vector<std::vector<uint64_t>> send_machines = map_result.second;

    if (send_machines.empty()) return false; 

    uint64_t machine = relative_log_pos % num_m_per_extent;
    size_t size_of_hdr = get_corfu_cli_header_size();
    size_t size_of_type_hdr = get_corfu_trim_header_size();
    uint64_t allocated_packet_size = size_of_hdr + size_of_type_hdr + 1;

    for (uint64_t sm : send_machines[machine]) {
        std::unique_ptr<char[]> packet = std::make_unique<char[]>(allocated_packet_size);
        
        corfu_hdr.get()->proto_type = htons(CORFU_TRIM_PROTO_TYPE);
        corfu_append_hdr.get()->log_idx = htonl(relative_log_pos);
        corfu_append_hdr.get()->curr_epoch = htonl(0);

        memcpy(packet.get(), reinterpret_cast<const char*>(corfu_hdr.get()), size_of_hdr);
        memcpy(packet.get() + size_of_hdr, reinterpret_cast<const char*>(corfu_append_hdr.get()), size_of_type_hdr);

        net->send_client_udp_packet(
            std::move(packet), 
            allocated_packet_size, 
            storage_ips[sm],
            std::to_string(recv_port)
        );
        
        char* msg = net->recv_packet();
        auto start_time = std::chrono::high_resolution_clock::now();
        auto read_time = std::chrono::high_resolution_clock::duration::zero();
        
        while (read_time < TIMEOUT && !msg) {
            msg = net->recv_packet();
            read_time = std::chrono::high_resolution_clock::now() - start_time;
        }

        if (!msg && read_time >= TIMEOUT) return false;

        struct corfu_storage_header* recv_hdr = (struct corfu_storage_header*)msg;
        if (ntohs(recv_hdr->proto_type) == CORFU_ACK_PROTO_TYPE) {
            continue;
        } else {
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
	    // spdlog::critical("========================= CLIENT STATISTICS ================================");
    	// // // spdlog::critical("APPEND Highest index seen is: {}", highest_idx_seen);
    	// spdlog::critical("APPEND sent {} appends in {} seconds.", cnt, max_duration);
    	// spdlog::critical("APPEND STATISTICS: lat is: {}, tput: {}, total ops: {}", stat->getAvgLatency(), stat->getThroughput(max_duration), stat->getTotalOps());
    	stat->getAvgLatency();
    	stat->getThroughput(max_duration);
    	stat->getTotalOps();
        stat->dumpAllLatencies();
    	stat->exportResultsToJson();
    } else {
	    // // spdlog::critical("========================= CLIENT STATISTICS ================================");
	    // // spdlog::critical("READ sent {} appends in {} seconds.", read_cntr, max_duration);
    	// // spdlog::critical("READ STATISTICS: lat is: {}, tput: {}, total ops: {}", read_stat->getAvgLatency(), read_stat->getThroughput(max_duration), read_stat->getTotalOps());
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

uint64_t CorfuClient::getTail() {
    return 0;
}
void CorfuClient::subscribe(uint64_t idx) {
    idx++;
}
