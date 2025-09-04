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

#include "network.h"
#include "yaml-cpp/yaml.h"
#include "utils.h"
#include "spdlog/spdlog.h"

Network::Network(uint64_t maxThreads, 
                 std::string seq_ip,
                 std::string storage_multicast_addr,
                 //std::vector<std::string> storage_ips, 
                 std::string send_port, 
                 std::string recv_port,
                 std::string socket_type,
                 uint64_t log_level,
                 uint64_t batch_size,
                 std::string send_interface,
                 std::string src_ip,
                 std::vector<std::string> pkt_types) {
    
    if (geteuid() != 0) { // Check if we are running as root
        throw std::runtime_error("Not running as root!");
    }
    if(!check_socket_type(socket_type)) { 
        throw std::runtime_error("Invalid socket type!");
    }
    
    this->socket_type = socket_type;
    this->send_interface = send_interface;
    this->batch_size = batch_size;
    this->src_ip = src_ip;
    total_num_threads = maxThreads;

    SEND_PORT = send_port;
    RECV_PORT = recv_port;
    this->storage_multicast_addr = storage_multicast_addr;

    this->seq_ip = seq_ip;
    seq_socket = -1;
    seq_recv_socket = -1;
    seq_it = NULL;

    set_spdlog_level(log_level);

    /* Initialize sockets */
    if (storage_multicast_addr != "") {
        /* Create storage sockets */ 
        std::shared_ptr<struct addrinfo> storage_it = std::make_shared<struct addrinfo>();
        storage_socket = socket_type == "UDP" ? setup_talker_socket(storage_multicast_addr, storage_it) : setup_raw_talker_socket();
        if (storage_socket < 0) {
            spdlog::critical("SENDER Storage Socket creation for IP {} unsuccessful. Aborting", this->storage_multicast_addr);
            throw std::runtime_error("Can't create sending socket");
        } 
        storage_recv_socket = setup_listener_socket(storage_multicast_addr);
        if (storage_recv_socket < 0) {
            spdlog::critical("RECEIVER Storage Socket creation for IP {} unsuccessful. Aborting", this->storage_multicast_addr);
            throw std::runtime_error("Can't create receiving socket");
        }
    }
    
    // If this protocol requires a seq_ip
    if (seq_ip != "") {
        seq_it = std::make_shared<struct addrinfo>();
        seq_socket = socket_type == "UDP" ? setup_talker_socket(seq_ip, seq_it) : setup_raw_talker_socket();
        if (seq_socket < 0) {
            spdlog::critical("SENDER Sequence Socket creation for IP {} unsuccessful. Aborting", seq_ip);
            throw std::runtime_error("Can't create sending socket");
        }
        seq_recv_socket = setup_listener_socket(seq_ip);
        if (seq_recv_socket < 0) {
            spdlog::critical("RECEIVER Sequence Socket creation for IP {} unsuccessful. Aborting", seq_ip);
            throw std::runtime_error("Can't create receiving socket");
        }
    }

    /*Initialize queues*/
    for (std::string pkt_type : pkt_types) {
        send_pkt_qs.insert(std::pair<std::string, std::queue<std::unique_ptr<std::string>>>(pkt_type, std::queue<std::unique_ptr<std::string>>()));
    }

    /*Initialize threads*/
    //total_num_threads = maxThreads == 0 ? std::thread::hardware_concurrency()-1 : maxThreads;   
    // Create sending threadpools - 1 thread per packet type
    for (uint64_t i = 0; i < pkt_types.size(); i++) {
        send_threads.emplace_back(std::thread(&Network::run_send, this, pkt_types[i])); 
    }

    // Create receiving threadpool (only 1 thread for now since it's Network I/O bound)
    recv_threads.emplace_back(std::thread(&Network::run_recv, this, seq_recv_socket));
    if (seq_recv_socket > 0)
        recv_threads.emplace_back(std::thread(&Network::run_recv, this, seq_recv_socket)); // TODO why 2x?

}
    
Network::~Network() {
    stop_threads();
    destroy_socket(storage_socket);
    destroy_socket(seq_socket);
}

unsigned short Network::checksum(unsigned short *buf, int nwords) {
    unsigned long sum;
    for(sum=0; nwords>0; nwords--) {
        sum += *buf++;
    }
    sum = (sum >> 16) + (sum &0xffff);
    sum += (sum >> 16);
    return (unsigned short)(~sum);
}

// Threadpool send thread function
// Send packets as they are queued
void Network::run_send(std::string pkt_type) {
    std::unique_ptr<char> send_packet = std::make_unique<char>(batch_size + batch_size*sizeof(size_t)); 
    size_t offset = 0;
    uint64_t batch_bytes = 0;
    while (!terminate) {
        {
            std::unique_ptr<std::string> send_pkt = nullptr;
            std::unique_lock<std::mutex> lock(send_pkt_qs_mutex); // TODO: PER QUEUE LOCK
            mutex_condition.wait(lock, [&, this] {
                return !send_pkt_qs[pkt_type].empty() || terminate;        
            });
            if (terminate) {
                return;
            }
            send_pkt = std::move(send_pkt_qs[pkt_type].front()); // TODO: drops packets??
            if ((*send_pkt.get()).length() <= (batch_size - batch_bytes)) {
                memcpy(send_packet.get() + offset, std::to_string((*send_pkt.get()).length()).c_str(), sizeof(size_t));
                offset += sizeof(size_t);
                memcpy(send_packet.get() + offset, (*send_pkt.get()).c_str(), (*send_pkt.get()).length());
                batch_bytes += (*send_pkt.get()).length();
                offset += (*send_pkt.get()).length();
                send_pkt_qs[pkt_type].pop();
                lock.release();
                continue;
            }
        }
        //std::unique_ptr<char> send_packet = std::make_unique<char>(send_packet);
        //
        
        // If the packet type is IP addresses AND socket_type UDP
        // There is no support for custom headers + IP addresses
        if (validate_ip_address(pkt_type) && socket_type == "UDP") {
            std::shared_ptr<struct addrinfo> it = std::make_shared<struct addrinfo>();
            int s_fd = setup_talker_socket(pkt_type, it);
            if (s_fd < 0) {
                spdlog::critical("SENDER Socket creation for IP {} unsuccessful. Aborting", pkt_type);
                throw std::runtime_error("Can't create sending socket");
            } 
            ssize_t num_bytes = sendto(s_fd, send_packet.get(), batch_bytes, 0, it->ai_addr, it->ai_addrlen);
            if (num_bytes < 0 || ((uint64_t)num_bytes != batch_bytes)) {
                spdlog::warn("Error {} occurred: {}", std::to_string(errno), strerror(errno));
                memset(send_packet.get(), 0, batch_size);
                batch_bytes = 0;
                offset = 0;
                continue;
            }
            spdlog::info("Successfully sent {} bytes to the receiver.", std::to_string(num_bytes));
            memset(send_packet.get(), 0, batch_size);
            batch_bytes = 0;
            offset = 0;
            continue;
        }

        // If the packet type is a descriptive string to indicate header type
        int s_fd = get_socket(pkt_type, protocol_type);
        if (s_fd < 0) {
            spdlog::critical("No socket found, dropping buffers");
            continue;
        }
        

        if (socket_type == "UDP") { // If we are running UDP
            std::shared_ptr<struct addrinfo> it = get_it(s_fd);
            ssize_t num_bytes = sendto(s_fd, send_packet.get(), batch_bytes, 0, it->ai_addr, it->ai_addrlen);
            if (num_bytes < 0 || ((uint64_t)num_bytes != batch_bytes)) {
                spdlog::warn("Error {} occurred: {}", std::to_string(errno), strerror(errno));
                memset(send_packet.get(), 0, batch_size);
                batch_bytes = 0;
                offset = 0;
                continue;
            }
            spdlog::info("Successfully sent {} bytes to the receiver.", std::to_string(num_bytes));
            memset(send_packet.get(), 0, batch_size);
            batch_bytes = 0;
            offset = 0;
            continue;
        }

        /* Running a raw socket based protocol */
        size_t size_of_hdr = get_size_of_hdr(pkt_type, protocol_type);
        std::string ip_addr = get_ip(s_fd);
        if (size_of_hdr == 0) {
            spdlog::warn("Packet type is invalid for protocol id! No packets sent.");
            continue;
        }

        
        size_t packet_size = sizeof(struct ethhdr) + sizeof(struct iphdr) + size_of_hdr + batch_bytes;
        spdlog::debug("Eth hdr: {}, IP hdr: {}, Size hdr: {}, Send packet: {}", sizeof(struct ethhdr), sizeof(struct iphdr), size_of_hdr, batch_bytes);
        std::unique_ptr<char[]> packet = std::make_unique<char[]>(packet_size);
       
        /*Create ethernet header - dest addr will currently indicate multicast TODO unicast*/
        std::unique_ptr<struct ethhdr> eth = create_eth_hdr(s_fd, pkt_type);
        memcpy(packet.get(), eth.get(), sizeof(struct ethhdr));
        
        /*Create IP header*/
        std::unique_ptr<struct iphdr> ip = create_ip_hdr(ip_addr, size_of_hdr, (unsigned short *)packet.get()); 
        memcpy(packet.get() + sizeof(struct ethhdr), ip.get(), sizeof(struct iphdr));

        /*Protocol specific code starts*/
        if (protocol_type == CORFU) { // Corfu TODO CID NEEDED HOW TO PASS THAT IN??
            if (pkt_type == "get_seq_no") {
                std::unique_ptr<struct get_sequence_number> hdr = create_get_sequence_num(0);
                memcpy(packet.get() + sizeof(struct ethhdr) + sizeof(struct iphdr), hdr.get(), size_of_hdr);
            }
        } else if (protocol_type == RING) { // Ringlog
            if (pkt_type == "append_req") {
                std::unique_ptr<struct ring_append_entry> hdr = create_ring_append_entry(*(generate_nonce().get()), 0);
                memcpy(packet.get() + sizeof(struct iphdr), hdr.get(), size_of_hdr);
            } else if (pkt_type == "append_resp") {
                std::unique_ptr<struct ring_append_success> hdr = create_ring_append_reply(*(generate_nonce().get()), 0);
                memcpy(packet.get() + sizeof(struct ethhdr) + sizeof(struct iphdr), hdr.get(), size_of_hdr);
            }
        }
        spdlog::debug("Size of custom header is: {} and total packet size is {}", size_of_hdr, packet_size);
        /*Protocol specific code ends*/

        memcpy(packet.get() + sizeof(struct ethhdr) + sizeof(struct iphdr) + size_of_hdr, (send_packet.get()), batch_bytes);
        ssize_t num_bytes = 0;
        
        /*Create sockaddr_ll struct*/
        struct sockaddr_ll sin; // TODO: This is for packets where I'm not maually putting the header on them I think, I need to use sockaddr_ll
        /* Index of the network device */
        sin.sll_ifindex = if_nametoindex((const char*)send_interface.c_str());//ifr.get()->ifr_ifindex;
        /* Address length*/
        sin.sll_halen = ETH_ALEN;
        for (int i = 0; i < 6; i++) { // 48 bit mac address - local broadcast
            sin.sll_addr[i] = eth.get()->h_dest[i];
        }

        if ((num_bytes = sendto(s_fd, packet.get(), packet_size, 0, (struct sockaddr*)(&sin), sizeof(sin))) < 0 || 
        //if ((num_bytes = send(s_fd, packet.get(), packet_size, 0)) < 0 ||
                ((uint64_t)num_bytes != packet_size)) {
            spdlog::warn("Error {} occurred: {}", std::to_string(errno), strerror(errno));
            spdlog::debug("Num bytes sent: {} vs. expected: {}", num_bytes, packet_size);
            continue;
        }
        spdlog::debug("Successfully sent {} bytes to the receiver", std::to_string(num_bytes));
        memset(send_packet.get(), 0, batch_size);
        batch_bytes = 0;
        offset = 0;
    }
}

int Network::get_socket(std::string pkt_type, ClientType protocol_type) {
    if (protocol_type == CORFU) {
        if (pkt_type == "seq_req") {
            return seq_socket;
        }
        return storage_socket;
    } else if (protocol_type == RING) {
        return storage_socket;
    }
    return -1; // No other protocols supported
}

std::string Network::get_ip(int s_fd) {
    if (s_fd == seq_socket) {
        return seq_ip;
    }
    return storage_multicast_addr;
}

std::shared_ptr<struct addrinfo> Network::get_it(int s_fd) {
    if (s_fd == seq_socket) {
        return seq_it;
    }
    return storage_it;
}

std::unique_ptr<struct ethhdr> Network::create_eth_hdr(int s_fd, std::string pkt_type) {
    int eth_type = get_eth_type(pkt_type, protocol_type);
    if (eth_type < 0) {
        spdlog::warn("Unable to ethernet type for this packet type! No packets sent.");
        return NULL;
    } 
    std::unique_ptr<struct ethhdr> eth = std::make_unique<struct ethhdr>();
    for (int i = 0; i < 6; i++) { // 48 bit mac address - local broadcast
        eth.get()->h_dest[i] = 0xff;
    }
        
    /*Get src address*/
    std::unique_ptr<struct ifreq> ifr = std::make_unique<struct ifreq>();
    memset(ifr.get(), 0, sizeof(struct ifreq));
        
    /*Get interface*/
    /*struct ifaddrs *ifap, *temp;
    int64_t status = getifaddrs(&ifap);
    if (status != 0) {
        spdlog::debug("Cannot get interface info, Error {} occurred: {}", curr_ip.c_str(), status, gai_strerror(status));
        return -1;
    }
    char* interface;
    for (temp = ifap; temp != NULL; temp = temp->ifa_next) {
        if (ifap->addr == NULL) {
            continue;
        }
        interface = temp->name;
        break;
    }
    if (interface == NULL) {
        spdlog::debug("No valid interfaces found");
        return -1;
    }*/
    snprintf (ifr.get()->ifr_name, sizeof (ifr.get()->ifr_name), "%s", (const char*)send_interface.c_str());
    if (ioctl(s_fd, SIOCGIFHWADDR, ifr.get()) < 0) {
        spdlog::critical("Unable to get our MAC address! Errno {} with error {}", std::to_string(errno), strerror(errno));
        return NULL;
    }
    memcpy(eth.get()->h_source, ifr.get()->ifr_hwaddr.sa_data, 6 * sizeof (uint8_t));
    /*Set ethernet type*/
    eth.get()->h_proto = eth_type; // Tells receiver how to parse packet
    return eth;
}

std::unique_ptr<struct iphdr> Network::create_ip_hdr(std::string dst_ip, size_t size_of_hdr, unsigned short* pkt) {
    /*Create IP header*/
    std::unique_ptr<struct iphdr> ip = std::make_unique<struct iphdr>();
    ip.get()->ihl      = 5; //version length
    ip.get()->version  = 4; // version; should we allow for ipv6?
    ip.get()->tos      = 0; // type of service - set to normal, could change in future
    ip.get()->tot_len  = sizeof(struct iphdr) + size_of_hdr; // total length of packet header
    ip.get()->id       = htons(54321); // default ID number for ip packet
    ip.get()->ttl      = 64; // default hops; circle back in case of change
    ip.get()->protocol = IPPROTO_RAW; // Raw IP

    ip.get()->saddr = inet_addr(src_ip.c_str()); // source address
    ip.get()->daddr = inet_addr(dst_ip.c_str()); // destination address
    ip.get()->check = checksum(pkt, sizeof(struct iphdr)); // checksum ONLY for the IPv4 header^
    return ip;
}


// Queue packets as they are received
void Network::run_recv(int s_fd) {
    spdlog::info("RUNNING RECV THREAD");
    std::string curr_ip = "";
    int efd = epoll_create(s_fd);
    if (efd < 0) {
        spdlog::critical("Epoll creation unsuccessful. Aborting");
        return;
    }
    int numbytes;
    while (!terminate) {
        struct sockaddr_storage src_addr;
        socklen_t addr_len = sizeof src_addr;
        // If IP has changed, create new datagram socket
        // Poll the socket to see if it has received a packet (UPDATE to do async?? prob don't want thread just spinning)
        struct epoll_event ev;
        ev.data.fd = s_fd;
        ev.events = 0;
        epoll_ctl(efd, EPOLL_CTL_ADD, s_fd, &ev);
        epoll_wait(efd, &ev, 1, MAX_POLL_TIME); // maxevents??
        
        std::unique_ptr<char[]> buf = std::make_unique<char[]>(batch_size + batch_size*sizeof(int));  
        if ((numbytes = recvfrom(s_fd, buf.get(), batch_size + batch_size*sizeof(int), 0, (struct sockaddr *)&src_addr, &addr_len)) == -1) {
            spdlog::warn("Error {} occurred: {}", std::to_string(errno), strerror(errno));
            continue;
        }
        size_t offset = 0;
        for (uint64_t i = 0; i < batch_size; i++) {
            offset += sizeof(size_t);
            size_t size_of_pkt = *((size_t*)(buf.get() + offset));
            offset += size_of_pkt;
            std::unique_ptr<char[]> sample_pkt = std::make_unique<char[]>(size_of_pkt);
            struct ethhdr* eth = (struct ethhdr*)sample_pkt.get();
            size_t custom_hdr_size = get_size_of_hdr_int(eth->h_proto, protocol_type);
            spdlog::debug("Ethernet protocol with size {}", custom_hdr_size);
            if (custom_hdr_size == 0) {
                continue;
            }
            // TODO TODO ADD THE PROCESSING OF THE ETHERNET HEADER TO READ THE TYPE AND THE DYAMICALLY DETERMINE THE HEADER
            char* rcv_str = (char*)(sample_pkt.get() + sizeof(struct ethhdr) + sizeof(struct iphdr) + custom_hdr_size);

            spdlog::debug("Receiver received the message with num bytes: {}, eth hdr: {}, ip hdr: {}, append hdr: {}", std::to_string(numbytes), std::to_string(sizeof(struct ethhdr)), std::to_string(sizeof(struct iphdr)), std::to_string(custom_hdr_size));
            {
                std::unique_lock<std::mutex> lock(rcv_queue_mutex);
                std::string str(rcv_str);
                spdlog::debug("The string is: {}", str);
                std::unique_ptr<std::string> str_ptr = std::make_unique<std::string>(str);
                rcv_pkt.emplace(std::move(str_ptr));
            }
        }
    }
}

// this is the compiled pointer to protobuf string
void Network::add_to_send_queue(std::unique_ptr<std::string> buf, 
                                std::string packet_type) {
    std::unique_lock<std::mutex> lock(send_pkt_qs_mutex);
    if (terminate) {
        spdlog::info("No more packets accepted!");
        return;
    }
    spdlog::debug("Going to send packet type: {}, with content: {}.", packet_type, *(buf.get()));
    send_pkt_qs[packet_type].push(std::move(buf));
    mutex_condition.notify_one();
    return;
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
    std::unique_lock<std::mutex> lock(send_pkt_qs_mutex);
    terminate = true;
    // TODO send termiating message to receiver
}

/*std::string Network::update_ip_addrs() {
    std::unique_lock<std::mutex> lock(ip_addrs_idx_mutex);
    curr_ip_addrs_idx = (curr_ip_addrs_idx + 1) % all_ip_addrs.size();
    chosen_ip_addr = all_ip_addrs[curr_ip_addrs_idx];
    return chosen_ip_addr;
}*/

bool Network::pkts_in_queue() {
    {
        std::unique_lock<std::mutex> lock(send_pkt_qs_mutex);
        for (const auto& [pkt_type, q] : send_pkt_qs) {
            if (!q.empty()) {
                return true;
            }
        }
    }
    return false;
}

/*Sets up a datagram receiver socket for chosen_ip_addr*/  
int Network::setup_listener_socket(std::string curr_ip) {
    struct addrinfo hints, *servinfo, *temp;
    int s_fd;
    int yes = 1;
    if (socket_type != "UDP") {
        if ((s_fd = socket(AF_PACKET, SOCK_RAW, htons(ETH_P_ALL))) == -1) { // TODO TODO: INCORRECT CHANGE TO AF_PACKET AS PER MAN 7 packet
            spdlog::critical("Cannot get getaddrinfo for IP {}, Error {} occurred: {}", curr_ip.c_str(), std::to_string(errno), strerror(errno));
            return -1;
        }
        spdlog::debug("Created raw receive socket of fd {}", s_fd);
        /*if (setsockopt(s_fd, IPPROTO_IP, SO_REUSEADDR | IP_HDRINCL, &yes, sizeof(int)) == -1) {
            spdlog::critical("Cannot set socket options, Error {} occurred: {}", std::to_string(errno), strerror(errno));
            return -1;
        }*/
        /*if (bind(s_fd, curr_ip.c_str(), ) == -1) {
        }*/
        int flags = fcntl(s_fd, F_GETFL, 0);
        if (flags == -1) return false;
        flags = flags | O_NONBLOCK;
        if (fcntl(s_fd, F_SETFL, flags) != 0) {
            spdlog::critical("UNABLE TO SET FCNTL FLAGS");
        }
        return s_fd;
    }
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_flags = AI_PASSIVE;
    int64_t status = getaddrinfo(curr_ip.c_str(), RECV_PORT.c_str(), &hints, &servinfo);
    if (status != 0) {
        spdlog::critical("Cannot get getaddrinfo for IP {}, Error {} occurred: {}", curr_ip.c_str(), std::to_string(status), gai_strerror(status));
        return -1;
    }
    for (temp = servinfo; temp != NULL; temp = temp->ai_next) {
        if ((s_fd = socket(temp->ai_family, temp->ai_socktype, temp->ai_protocol)) == -1) {
            spdlog::critical("Cannot get socket fd, Error {} occurred: {}", std::to_string(errno), strerror(errno));
            continue;
        }
        if (setsockopt(s_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &yes, sizeof(int)) == -1) {
            spdlog::critical("Cannot set socket options, Error {} occurred: {}", std::to_string(errno), strerror(errno));
            freeaddrinfo(servinfo);
            return -1;
        }
        if (bind(s_fd, temp->ai_addr, temp->ai_addrlen) == -1) { // TODO abstract error handling into function
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
    int flags = fcntl(s_fd, F_GETFL, 0); // TODO abstract into a helper function
    if (flags == -1) return false;
    flags = flags | O_NONBLOCK;
    if (fcntl(s_fd, F_SETFL, flags) != 0) {
        spdlog::critical("UNABLE TO SET FCNTL FLAGS");
    }
    return s_fd;
}

/*Sets up a datagram UDP socket for chosen_ip_addr*/  
int Network::setup_talker_socket(std::string curr_ip, std::shared_ptr<struct addrinfo>& it) {
    struct addrinfo hints, *servinfo, *temp;
    int s_fd;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM; // Normal UDP Datagram Socket
    int64_t status = getaddrinfo(curr_ip.c_str(), SEND_PORT.c_str(), &hints, &servinfo);
    if (status != 0) {
        spdlog::debug("Cannot get getaddrinfo for IP {}, Error {} occurred: {}", curr_ip.c_str(), status, gai_strerror(status));
        return -1;
    }
    for (temp = servinfo; temp != NULL; temp = temp->ai_next) {
        if ((s_fd = socket(temp->ai_family, temp->ai_socktype, temp->ai_protocol)) == -1) {
            spdlog::debug("Can't get socket! Error {} occurred: {}", std::to_string(errno), strerror(errno));
            continue;
        }
        memcpy(it.get(), temp, sizeof (struct addrinfo));
        break;
    }
    if (temp == NULL) {
        spdlog::critical("Failed to create socket!");
        return -1;
    }
    freeaddrinfo(servinfo);
    return s_fd;
}

int Network::setup_raw_talker_socket() {
    int s_fd = -1;
    if ((s_fd = socket(AF_PACKET, SOCK_RAW, htons(ETH_P_ALL))) == -1) {
        spdlog::critical("Unable to create raw socket! Error {} occurred: {}", std::to_string(errno), strerror(errno));
        return -1;
    }
    spdlog::debug("Created raw receive socket of fd {}", std::to_string(s_fd));
    return s_fd;
}

void Network::destroy_socket(int s_fd) {
    if (s_fd > -1) {
        close(s_fd);
    }
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

bool Network::check_socket_type(std::string socket_type) {
    return (socket_type == "UDP" || socket_type == "RAW");
}

bool Network::validate_ip_address(const std::string &ip_addr) {
    struct sockaddr_in sa;
    int result = inet_pton(AF_INET, ip_addr.c_str(), &(sa.sin_addr));
    return result != 0;
}

void Network::add_pkt_type(std::string pkt_type) {
    send_pkt_qs.insert(std::pair<std::string, std::queue<std::unique_ptr<std::string>>>(pkt_type, std::queue<std::unique_ptr<std::string>>()));
}

bool Network::remove_pkt_type(std::string pkt_type) {
    for (auto it = send_pkt_qs.begin(); it != send_pkt_qs.end(); it++) {
        if (it->first == pkt_type) {
            send_pkt_qs.erase(it);
            return true;
        }
    }
    return false;
}
