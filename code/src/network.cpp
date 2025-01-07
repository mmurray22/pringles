#include "network.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <netdb.h>
#include <unistd.h>
#include <iostream>
#include <string.h>
#include "yaml-cpp/yaml.h"

/*
 * Assumption: Assumes the ip_file is a YAML file that has an entry called
 * "ip_addrs" that indexes to a list of IP address strings
 */
Network::Network(uint64_t maxThreads, std::string ip_file) {
    total_num_threads = maxThreads == 0 ? std::thread::hardware_concurrency() : maxThreads;   
    // Create sending threadpool
    for (uint64_t i = 0; i < total_num_threads - 1; i++) {
        send_threads.emplace_back(std::thread(&Network::run_send, this)); 
    }
    // Create receiving threadpool (only 1 thread for now since it's Network I/O bound)
    for (uint64_t i = 0; i < 1; i++) {
        recv_threads.emplace_back(std::thread(&Network::run_send, this)); 
    }


    // Initialize initial IP list
    YAML::Node config = YAML::LoadFile(ip_file);
    for (std::size_t i=0; i< config["ip_addrs"].size(); i++) {
        std::string ip_addr = config["ip_addrs"][i].as<std::string>();
        all_ip_addrs.emplace_back(ip_addr); 
    }
    if (all_ip_addrs.empty()) {
        std::cout << "NO IP ADDRESSES SUBMITTED, ABORTING" << std::endl;
        throw; // TODO Do I need a exception code? 
    }
    chosen_ip_addr = all_ip_addrs[0];
}

Network::~Network() {
    stop_threads();
}

// Threadpool send thread function
void Network::run_send() {
    std::string curr_ip = chosen_ip_addr;
    int s_fd = setup_socket(curr_ip, false);
    if (s_fd < 0) {
        std::cout << "Socket creation unsuccessful. Aborting" << std::endl;
        return;
    }
    // Send packets as they are queued
    while (!terminate) {
        // If IP has changed, create new datagram socket
        if (curr_ip != chosen_ip_addr) {
            destroy_socket(s_fd);
            s_fd = setup_socket(curr_ip, false);
            if (s_fd < 0) {
                std::cout << "Socket creation unsuccessful. Aborting" << std::endl;
                return;
            }
        }
        char* send_packet = NULL;
        {
            std::unique_lock<std::mutex> lock(send_queue_mutex);
            mutex_condition.wait(lock, [this] {
                return !send_pkt.empty() || terminate;        
            });
            if (terminate) {
                return;
            }
            send_packet = send_pkt.front();
            send_pkt.pop();
        }
        if (!send_packet) { // In the case that send_packet is still NULL
            continue;
        }
        ssize_t num_bytes = send(s_fd, send_packet, *send_packet.length(), 0);
        if (num_bytes != *send_packet.length()) {
             if (num_bytes == -1) {
                 std::cout << "Error " << errno << "occurred: " << strerror(errno) << std::endl;
             }
        }
    }
}

void Network::run_recv() {
    std::string curr_ip = chosen_ip_addr;
    int s_fd = setup_socket(curr_ip, true);
    if (s_fd < 0) {
        std::cout << "Socket creation unsuccessful. Aborting" << std::endl;
        return;
    }
    accept(s_fd, NULL, NULL);
    epoll_create(s_fd);
    // Send packets as they are queued
    while (!terminate) {
        // If IP has changed, create new datagram socket
        if (curr_ip != chosen_ip_addr) {
            destroy_socket(s_fd);
            s_fd = setup_socket(curr_ip, true);
            if (s_fd < 0) {
                std::cout << "Socket creation unsuccessful. Aborting" << std::endl;
                return;
            }
            accept(s_fd, NULL, NULL);
            epoll_create(s_fd);
        }
        // Poll the socket to see if it has received a packet (UPDATE)
        epoll_wait(s_fd, NULL, 1, MAX_POLL_TIME); // maxevents??
        char buf[10000]; // Best way to size this??
        recv(s_fd, buf, sizeof(buf), 0);
        {
            std::unique_lock<std::mutex> lock(rcv_queue_mutex);
            rcv_pkt.emplace(&buf);
        }
    }
}

void Network::add_to_send_queue(char* buf) {
    std::unique_lock<std::mutex> lock(send_queue_mutex);
    if (terminate) {
        std::cout << "No more packets accepted!" << std::endl;
        return; // false;
    }
    send_pkt.emplace(buf); // Do I need to register output?
    mutex_condition.notify_one();
    return;
    //return (buf != NULL);
}

char* Network::read_from_recv_queue() {
    std::unique_lock<std::mutex> lock(rcv_queue_mutex);
    if (rcv_pkt.empty()) {
        return NULL;
    }
    char* receive_pkt = rcv_pkt.front();
    rcv_pkt.pop();
    return receive_pkt;
}

std::string Network::update_ip_addrs() {
    std::unique_lock<std::mutex> lock(ip_addrs_idx_mutex);
    curr_ip_addrs_idx = (curr_ip_addrs_idx + 1) % all_ip_addrs.size();
    chosen_ip_addr = all_ip_addrs[curr_ip_addrs_idx];
    return chosen_ip_addr;
}

bool Network::pkts_in_queue() {
    bool busy = true;;
    {
        std::unique_lock<std::mutex> lock(send_queue_mutex);
        busy = !send_pkt.empty();
    }
    return busy;
}

/*Sets up a datagram UDP socket for chosen_ip_addr*/  
int Network::setup_socket(std::string curr_ip, bool recv_socket) {
    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM; //Datagram socket
    int64_t status = getaddrinfo(curr_ip.c_str(), NULL, &hints, &res);
    if (status != 0) {
        std::cout << "Error " << status << "occurred: " << gai_strerror(status) << std::endl;
        return -1;
    }
    int s_fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);    
    if (s_fd == -1) {
        std::cout << "Error " << errno << "occurred: " << strerror(errno) << std::endl;
        return -1;
    }
    if (recv_socket){
        int ret = bind(s_fd, res->ai_addr, res->ai_addrlen);
        if (ret == -1) { // TODO abstract error handling into function
            std::cout << "Error " << errno << "occurred: " << strerror(errno) << std::endl;
            return -1;
        }
        listen(s_fd, BACKLOG);
        return s_fd;
    } else {
        int ret = connect(s_fd, res->ai_addr, res->ai_addrlen);
        if (ret == -1) {
            std::cout << "Error " << errno << "occurred: " << strerror(errno) << std::endl;
            return -1;
        }
    }
    return s_fd;
}

void Network::destroy_socket(int s_fd) {
    close(s_fd);
}

void Network::stop_threads() {
    {
        std::unique_lock<std::mutex> lock(lock_terminate);
        terminate = true;
    }
    while (pkts_in_queue()) {
        sleep(10); // probably better way to do this
    }
    mutex_condition.notify_all();
    for (uint64_t i = 0; i < send_threads.size(); i++) {
        send_threads[i].join();
    }
    send_threads.clear();
    for (uint64_t i = 0; i < recv_threads.size(); i++) {
        recv_threads[i].join();
    }
    recv_threads.clear();
}

