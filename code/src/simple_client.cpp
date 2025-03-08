#include "simple_client.h"
#include "utils.h"
#include "spdlog/spdlog.h"

SimpleClient::SimpleClient(std::string input_file) {
    YAML::Node config = YAML::LoadFile(input_file);

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
    
    // Set SimpleClient log level
    set_spdlog_level(get_log_level(config));
    // Set number of storage servers
    num_storage_servers = get_num_ss(config);
    // Create trace
    trace = std::make_shared<Trace<std::string>>(get_trace_file(config));
    // Get Client type
    cli_type = fromStringToClientType(get_type(config));
    pending_appends_updated = false;

    // Timeouts
    wait_for_read_acks = get_read_timeout(config);
    wait_for_write_acks = get_write_timeout(config);

    // Set junk TODO

    // Create recv thread
    recv = std::thread(&SimpleClient::recv_pkt, this);

    // TODO: set cid?
}

SimpleClient::~SimpleClient() {
    terminate = true;
    net->done();
    recv.join();
    sub.join();
}

/***  Virtual function implementations ***/
uint64_t SimpleClient::append(std::string entry) {
    uint64_t idx = new_seq_no();
    std::unique_ptr<std::string> payload = trace->serialize_str_entry(entry, cli_type, "append", idx);
    net->add_to_send_queue(payload, "");
    uint64_t num_acks = num_storage_servers;
    while(num_acks < (Math.floor(num_storage_servers/2) + 1)) {
        std::lock_guard pal(pending_append_lock);
        palCV.wait(pal, []{return pending_appends_updated;});
        num_acks = pending_appends[idx];
    }
    return num_acks;
}

std::unique_ptr<std::string> SimpleClient::read(uint64_t idx) {
    std::unique_ptr<std::string> payload = trace->serialize_str_entry("", cli_type, "read", idx);
    {
        std::lock_guard payload_lk(pending_read_lock);
        pending_reads.emplace(idx, 0);
    }
    net->add_to_send_queue(payload, "");
    // TODO add read specific logic here
    return wait_for_read();
}

uint64_t SimpleClient::getTail() {
    return get_seq_no()+1;
}

void SimpleClient::subscribe(uint64_t idx) {
    sub_start_idx = idx;
    sub = std::thread(&SimpleClient::get_next_entries, this);
}

bool SimpleClient::trim(uint64_t idx) {
    for (uint64_t i = 1; i <= idx; i++) {
        // send a message to 
    }
    return true;
}

/*** External trace functions ***/
void SimpleClient::get_next_entries() {
    while(!terminate) {
        sleep(sub_timeout);
        uint64_t tail = getTail();
        {
            for(uint64_t i = curr_sub_idx+1; i < tail; i++) {
                std::unique_ptr<std::string> protoEntry = read(i);
                std::string entry = trace->deserialize_str_entry(protoEntry, cli_type)
                local_log.emplace(i, entry);
            }
        }
        {
            std::lock_guard csil(curr_sub_idx_lk);
            curr_sub_idx = tail > curr_sub_idx ? tail : curr_sub_idx;
        }
    }
}

void SimpleClient::append_trace() {
    // Iterate through the whole trace and call 
    // the append operation on each
}

void read_trace() {
    // Now read every entry and check that they
    // are in the original trace. Maybe even separately
    // keep track of a "local" KV store to make sure they 
    // are stored at the correct idx?
}

/*** Helper function implementations ***/
uint64_t new_seq_no() {
    std::lock_guard seq_guard(seq_no_lock);
    seq_no += 1;
    return seq_no;
}

uint64_t get_seq_no() {
    std::lock_guard get_guard(seq_no_lock);
    return seq_no;
}

void SimpleClient::recv_packets() { // TODO don't forget the nonce!!
    while (true) {
        // 0. Reset condition variables
        // 1. Constantly pull from the recv queue
        // 2. Determine what kind of packet SimpleClient received
        // 3. Determine the correct processing thread to send info to
        // 4. Send info
        std::unique_ptr<std::string> recv_pkt = net->read_from_recv_queue();
        Payload p = trace->deserialize_str_entry(recv_pkt, cli_type);
        std::string pkt_type = p.get_packet_type();
        if (pkt_type == "append") {
            std::string entry = p.get_append().get_entry();
            std::lock_guard pal(pending_appends_locks);
            if ((pending_appends.find(std::hash<std::string>{}(entry))) != pending_appends.end()) {
                pending_appends[std::hash<std::string>{}(entry)] += 1;
            } else {
                pending_appends.emplace(std::hash<std::string>{}(entry), 1);
            }
            pending_appends_updated = true; // TODO correct cv?
        } else (pkt_type == "read") {
            std::string entry = p.get_read().get_entry();
            if (entry == "") {
                fill(entry, p.get_read().get_idx());
            } else {
                // do stuff
            }
        }
    }
}

uint64_t fill(std::string entry. uint64_t idx) { // TODO
    if (entry == "") {
        entry = junk;
    }
    std::unique_ptr<std::string> payload = trace->serialize_str_entry(entry, cli_type, "append", idx);
    net->add_to_send_queue(payload, "");
}
