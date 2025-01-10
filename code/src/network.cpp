#include "network.h"
#include <netinet/in.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <utility>
#include <fcntl.h>
#include "spdlog/spdlog.h"
#include "yaml-cpp/yaml.h"


/*
 * Assumption: Assumes the ip_file is a YAML file that has an entry called
 * "ip_addrs" that indexes to a list of IP address strings
 */
Network::Network(uint64_t maxThreads, std::string input_yaml, std::string send_port) {
    SEND_PORT = send_port;
    // Initialize initial IP list TODO reading from YAML not working
    YAML::Node config = YAML::LoadFile(input_yaml);
    uint64_t log_level = config["log_level"].as<uint64_t>(); // TODO make log level determiation utility function
    if (log_level == 1) { // Prints all log levels except trace
        spdlog::set_level(spdlog::level::debug);
        spdlog::debug("Log level set to: Debug");
    } else if (log_level == 2) { // Prints error and critical
        spdlog::set_level(spdlog::level::err);
        spdlog::error("Log level set to: Error");
    } else if (log_level == 3) { // Prints warn, error, and critical
        spdlog::set_level(spdlog::level::warn);
        spdlog::warn("Log level set to: Warn");
    } else if (log_level == 4) { // Prints all but debug and trace
        spdlog::set_level(spdlog::level::info);
        spdlog::debug("Log level set to: Info");
    } else if (log_level == 5) { // Prints all log levels
        spdlog::set_level(spdlog::level::trace);
        spdlog::debug("Log level set to: Trace");
    } else { // Prints only critical
        spdlog::set_level(spdlog::level::critical);
        spdlog::critical("Log level set to: Critical");
    }
    for (std::size_t i=0; i< config["ip_addrs"].size(); i++) {
        std::string ip_addr = config["ip_addrs"][i].as<std::string>();
        all_ip_addrs.emplace_back(ip_addr); 
    }
    all_ip_addrs.push_back("127.0.0.1"); // TODO TODO
    if (all_ip_addrs.empty()) {
        spdlog::critical("NO IP ADDRESSES SUBMITTED, ABORTING");
        throw; // TODO Do I need a exception code? 
    }
    chosen_ip_addr = "127.0.0.1";//all_ip_addrs[0];
    spdlog::info("Chosen IP Addr constructor: {} from file {}", chosen_ip_addr, input_yaml); 
    
    total_num_threads = maxThreads == 0 ? std::thread::hardware_concurrency()-1 : maxThreads;   
    // Create sending threadpool
    for (uint64_t i = 0; i < total_num_threads; i++) {
        send_threads.emplace_back(std::thread(&Network::run_send, this)); 
    }
    // Create receiving threadpool (only 1 thread for now since it's Network I/O bound)
    for (uint64_t i = 0; i < 1; i++) {
        recv_threads.emplace_back(std::thread(&Network::run_recv, this)); 
    }
}
    
Network::~Network() {
    stop_threads();
}

// Threadpool send thread function
// Send packets as they are queued
void Network::run_send() {
    std::string curr_ip = "";
    int s_fd = -1;
    std::unique_ptr<struct addrinfo> it = std::make_unique<struct addrinfo>();
    while (!terminate) {
        // If IP has changed, create new datagram socket
        if (curr_ip != chosen_ip_addr) {
            curr_ip = chosen_ip_addr;
            // TODO: reset sockaddr?
            spdlog::debug("Chosen IP Addr : {}, Vector size: {}", chosen_ip_addr, std::to_string(all_ip_addrs.size()));
            if (s_fd > -1) {
                destroy_socket(s_fd);
            }
            s_fd = setup_talker_socket(curr_ip, false, it);
            if (s_fd < 0) {
                spdlog::critical("SENDER Socket creation unsuccessful. Aborting");
                return;
            }
        }
        std::unique_ptr<std::string> send_packet = NULL; //std::make_unique<std::string>(NULL);
        {
            std::unique_lock<std::mutex> lock(send_queue_mutex);
            mutex_condition.wait(lock, [this] {
                return !send_pkt.empty() || terminate;        
            });
            if (terminate) {
                return;
            }
            send_packet = std::move(send_pkt.front());
            send_pkt.pop();
        }
        if (!send_packet) { // In the case that send_packet is still NULL
            continue;
        }
        ssize_t num_bytes = sendto(s_fd, (*send_packet.get()).c_str(), (*send_packet.get()).length(), 0, it->ai_addr, it->ai_addrlen);
        if (num_bytes < 0 || ((uint64_t)num_bytes != (*send_packet.get()).length())) {
            spdlog::warn("Error {} occurred: {}", std::to_string(errno), strerror(errno));
            continue;
        }
        spdlog::info("Successfully sent {} bytes to the receiver.", std::to_string(num_bytes));
    }
}

// Queue packets as they are received
void Network::run_recv() {
    spdlog::info("RUNNING RECV THREAD");
    std::string curr_ip = "";
    int s_fd, efd = -1;
    int numbytes;
    while (!terminate) {
        struct sockaddr_storage src_addr;
        socklen_t addr_len = sizeof src_addr;
        // If IP has changed, create new datagram socket
        if (curr_ip != chosen_ip_addr) {
            curr_ip = chosen_ip_addr;
            if (s_fd > -1) {
                destroy_socket(s_fd);
            }
            s_fd = setup_listener_socket(curr_ip, true);
            if (s_fd < 0) {
                spdlog::critical("RECEIVER Socket creation unsuccessful. Aborting");
                return;
            }
            efd = epoll_create(s_fd);
            if (efd < 0) {
                spdlog::critical("Epoll creation unsuccessful. Aborting");
                return;
            }
        }
        // Poll the socket to see if it has received a packet (UPDATE to do async?? prob don't want thread just spinning)
        struct epoll_event ev;
        ev.data.fd = s_fd;
        ev.events = 0;
        epoll_ctl(efd, EPOLL_CTL_ADD, s_fd, &ev);
        epoll_wait(efd, &ev, 1, MAX_POLL_TIME); // maxevents??
        std::unique_ptr<char[]> buf = std::make_unique<char[]>(BUF_SIZE);  
        if ((numbytes = recvfrom(s_fd, buf.get(), BUF_SIZE-1, 0, (struct sockaddr *)&src_addr, &addr_len)) == -1) {
            spdlog::critical("Received error number of bytes");
            return;
        }
        buf[numbytes] = '\0';
        //std::cout << "Receiver received the message with num bytes: " << numbytes << std::endl;
        {
            std::unique_lock<std::mutex> lock(rcv_queue_mutex);
            std::string str(buf.get());
            std::unique_ptr<std::string> str_ptr = std::make_unique<std::string>(str);
            rcv_pkt.emplace(std::move(str_ptr));
        }
    }
}

// TODO: Pass around the unique pointers!! need to pass in pointer because
// this is the compiled pointer to protobuf string
void Network::add_to_send_queue(std::unique_ptr<std::string> buf) {
    std::unique_lock<std::mutex> lock(send_queue_mutex);
    if (terminate) {
        spdlog::info("No more packets accepted!");
        return;
    }
    send_pkt.emplace(std::move(buf)); // Do I need to register output?
    mutex_condition.notify_one();
    return;
    //return (buf != NULL);
}

std::unique_ptr<std::string> Network::read_from_recv_queue() {
    std::unique_lock<std::mutex> lock(rcv_queue_mutex);
    if (rcv_pkt.empty()) {
        return NULL;
    }
    std::unique_ptr<std::string> receive_pkt = std::move(rcv_pkt.front());
    rcv_pkt.pop();
    return receive_pkt;
}

// To only be called by the sender
void Network::done() {
    std::unique_lock<std::mutex> lock(send_queue_mutex);
    terminate = true;
    // TODO send termiating message to receiver
}

std::string Network::update_ip_addrs() {
    std::unique_lock<std::mutex> lock(ip_addrs_idx_mutex);
    curr_ip_addrs_idx = (curr_ip_addrs_idx + 1) % all_ip_addrs.size();
    chosen_ip_addr = all_ip_addrs[curr_ip_addrs_idx];
    return chosen_ip_addr;
}

bool Network::pkts_in_queue() {
    bool busy = true;
    {
        std::unique_lock<std::mutex> lock(send_queue_mutex);
        busy = !send_pkt.empty();
    }
    return busy;
}

/*Sets up a datagram UDP socket for chosen_ip_addr*/  
int Network::setup_listener_socket(std::string curr_ip, bool recv_socket) {
    struct addrinfo hints, *servinfo, *temp;
    int s_fd;
    int yes = 1;
    //struct sockaddr_storage their_addr;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM; //Datagram socket
    hints.ai_flags = AI_PASSIVE;
    int64_t status = getaddrinfo(curr_ip.c_str(), SEND_PORT.c_str(), &hints, &servinfo);
    if (status != 0) {
        spdlog::critical("Cannot get getaddrinfo for IP {}, Error {} occurred: {}", curr_ip.c_str(), status, gai_strerror(status));
        return -1;
    }
    for (temp = servinfo; temp != NULL; temp = temp->ai_next) {
        if ((s_fd = socket(temp->ai_family, temp->ai_socktype, temp->ai_protocol)) == -1) {
            spdlog::critical("Cannot get socket fd, Error {} occurred: {}", std::to_string(errno), strerror(errno));
            continue;
        }
        if (recv_socket && setsockopt(s_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &yes, sizeof(int)) == -1) {
            spdlog::critical("Cannot get socket fd, Error {} occurred: {}", std::to_string(errno), strerror(errno));
            freeaddrinfo(servinfo);
            return -1;
        }
        if (recv_socket && bind(s_fd, temp->ai_addr, temp->ai_addrlen) == -1) { // TODO abstract error handling into function
            close(s_fd);
            spdlog::critical("Cannot bind socket fd, Error {} occurred: {}", std::to_string(errno), strerror(errno));
            continue;
        }
        break;
    }
    if (temp == NULL) {
        spdlog::critical("Socket failed to bind!");
        return -1;
    }
    freeaddrinfo(servinfo);
    int flags = fcntl(s_fd, F_GETFL, 0);
    if (flags == -1) return false;
    flags = flags | O_NONBLOCK;
    if (fcntl(s_fd, F_SETFL, flags) != 0) 
        spdlog::critical("UNABLE TO SET FCNTL FLAGS");
    return s_fd;
}

/*Sets up a datagram UDP socket for chosen_ip_addr*/  
int Network::setup_talker_socket(std::string curr_ip, bool recv_socket, std::unique_ptr<struct addrinfo>& it) {
    struct addrinfo hints, *servinfo, *temp;
    int s_fd;
    if (recv_socket) {
        return -1;
    }
    //struct sockaddr_storage their_addr;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM; //Datagram socket
    int64_t status = getaddrinfo(curr_ip.c_str(), SEND_PORT.c_str(), &hints, &servinfo);
    if (status != 0) {
        std::cout << "Cannot get getaddrinfo for IP " << curr_ip.c_str() << ", Error " << status << " occurred: " << gai_strerror(status) << std::endl;
        return -1;
    }
    for (temp = servinfo; temp != NULL; temp = temp->ai_next) {
        if ((s_fd = socket(temp->ai_family, temp->ai_socktype, temp->ai_protocol)) == -1) {
            std::cout << "Cannot get socket fd, Error " << errno << " occurred: " << strerror(errno) << std::endl;
            continue;
        }
        memcpy(it.get(), temp, sizeof (struct addrinfo));
        break;
    }
    if (temp == NULL) {
        std::cout << "Socket failed to bind!" << std::endl;
        return -1;
    }
    //std::cout << "IT info: Addr " << it.get()->ai_addr << " Len: " << it.get()->ai_addrlen << std::endl;
    freeaddrinfo(servinfo);
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

