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
#include <unordered_map>

#include "ring_headers.h"
#include "network.h"
#include "yaml-cpp/yaml.h"
#include "utils.h"
#include "spdlog/spdlog.h"

Network::Network(std::string send_port, 
                 std::string recv_port,
                 std::string socket_type,
                 uint64_t log_level,
                 uint64_t batch_size,
                 bool batch_on,
		 uint64_t batch_timeout,
                 std::string send_interface,
                 std::string self_ip,
		 std::string multicast_ip,
		 bool is_in_shard,
		 bool run_threads) :  rcv_pkt(1000000)
{ 
    /*if (geteuid() != 0) { // Check if we are running as root
        throw std::runtime_error("Not running as root!");
    }*/
    if(!check_socket_type(socket_type)) { 
        throw std::runtime_error("Invalid socket type!");
    }
    set_spdlog_level(log_level);
    this->run_threads = run_threads;
    this->socket_type = socket_type;
    this->send_interface = send_interface;
    this->batch_size = batch_size;
    this->self_ip = self_ip;
    this->multicast_ip = multicast_ip;
    this->batch_on = batch_on;
    SEND_PORT = send_port;
    RECV_PORT = recv_port;
    spdlog::critical("Send port: {}, Receive port: {}", SEND_PORT, RECV_PORT);
    this->pkt_type_to_fd = {};
    this->running_pkt_size = 0;
    this->num_pkts = 0;
    this->batch_timeout = static_cast<double>(batch_timeout) / 1000000; //.000460
    auto start_time = (std::chrono::steady_clock::now()).time_since_epoch();
    this->batch_timer = std::chrono::duration_cast<std::chrono::duration<double>>(start_time).count();
    /*send_socket = setup_raw_talker_socket();
    if (send_socket < 0) {
        spdlog::critical("SENDER Socket creation unsuccessful. Aborting");
        throw std::runtime_error("Can't create sending socket");
    }
    spdlog::debug("The socket fd is {}", send_socket);*/
    if (is_in_shard) {
	recv_socket = setup_multicast_receiver(self_ip);
        if (recv_socket < 0) {
            spdlog::critical("MULTICAST RECEIVER Socket creation unsuccessful. Aborting");
            throw std::runtime_error("Can't create multicast receiving socket");
        }
    } else {
       recv_socket = setup_listener_socket(self_ip);
       if (recv_socket < 0) {
           spdlog::critical("RECEIVER Socket creation unsuccessful. Aborting");
           throw std::runtime_error("Can't create receiving socket");
       }

    }
    spdlog::debug("The socket fd is {}", recv_socket);

    this->send_ip_hdr = create_ip_hdr(); //ip_addr, pkt_len, (unsigned short *)packet.get()); 
    this->norm_buf = (char*)std::malloc(MAX_PACKET_SIZE);
    this->final_send_packet = (char*)std::malloc(MAX_PACKET_SIZE);
    //this->send_eth_hdr = create_eth_hdr(send_socket);

    this->sin.sll_ifindex = if_nametoindex((const char*)send_interface.c_str());//ifr.get()->ifr_ifindex;
    this->sin.sll_halen = ETH_ALEN;
    if (run_threads) {
   	 //this->send_thread = std::thread(&Network::run_send, this);	
   	 this->recv_thread = std::thread(&Network::run_recv, this, recv_socket);
    }
    /* End of new initializing socket */
}
    
Network::~Network() {
    // TODO cleanup???
}

std::string Network::get_recv_port() {
    return RECV_PORT;
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

std::string Network::get_ip(uint64_t pkt_type, int idx) {
    return pkt_type_to_ip[pkt_type][idx];
}

std::shared_ptr<struct addrinfo> Network::get_it(int s_fd) {
    return fd_to_it[s_fd]; 
}

/*Create ethernet header*/
std::unique_ptr<struct ethhdr> Network::create_eth_hdr(int s_fd) {
    std::array<uint8_t, 6> default_mac = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
    int eth_type_def = 0x800;
    if (eth_type_def < 0) {
        spdlog::warn("Unable to ethernet type for this packet type! No packets sent.");
        return NULL;
    } 
    std::unique_ptr<struct ethhdr> eth = std::make_unique<struct ethhdr>();
    for (int i = 0; i < 6; i++) { // 48 bit mac address - local broadcast
        eth.get()->h_dest[i] = default_mac[i]; // TODO
    }
        
    /*Get src address*/
    std::unique_ptr<struct ifreq> ifr = std::make_unique<struct ifreq>();
    memset(ifr.get(), 0, sizeof(struct ifreq));
        
    /*Get interface*/
    snprintf (ifr.get()->ifr_name, sizeof (ifr.get()->ifr_name), "%s", (const char*)send_interface.c_str());
    if (ioctl(s_fd, SIOCGIFHWADDR, ifr.get()) < 0) {
        spdlog::critical("Unable to get our MAC address! Errno {} with error {}", std::to_string(errno), strerror(errno));
        return NULL;
    }
    memcpy(eth.get()->h_source, ifr.get()->ifr_hwaddr.sa_data, 6 * sizeof (uint8_t));

    /*Set ethernet type*/
    eth.get()->h_proto = htons(eth_type_def); // Tells receiver how to parse packet
    return eth;
}


/*Create IP header*/
//std::unique_ptr<struct iphdr> Network::create_ip_hdr(std::string dst_ip, size_t size_of_pkt, unsigned short* pkt) {
std::unique_ptr<struct iphdr> Network::create_ip_hdr() {
    std::unique_ptr<struct iphdr> ip = std::make_unique<struct iphdr>();
    ip.get()->ihl      = 5; //version length
    ip.get()->version  = 4; // version; should we allow for ipv6?
    ip.get()->tos      = 0; // type of service - set to normal, could change in future
    ip.get()->tot_len  = htons(sizeof(struct iphdr)); // TODO check total length of packet header
    ip.get()->id       = htons(54321); // default ID number for ip packet
    ip.get()->ttl      = 64; // default hops; circle back in case of change
    ip.get()->protocol = IPPROTO_RAW; // Raw IP

    ip.get()->saddr = inet_addr(self_ip.c_str()); // source address
    ip.get()->daddr = inet_addr(self_ip.c_str()); // destination address
    //ip.get()->check = checksum(pkt, sizeof(struct iphdr)); // checksum ONLY for the IPv4 header^
    return ip;
}

// Queue packets as they are received
void Network::run_recv(int s_fd) {
    spdlog::info("RUNNING RECV THREAD");
    spdlog::critical("Network Recv Thread starting with TID = {}", gettid());
    std::string curr_ip = "";
    uint64_t cnt = 0;
    int numbytes;

    while (!terminate) {
        struct sockaddr_storage src_addr;
        socklen_t addr_len = sizeof src_addr;
    	char* norm_buf = (char*)std::malloc(MAX_PACKET_SIZE);
        if ((numbytes = recvfrom(s_fd, norm_buf, MAX_PACKET_SIZE, 0, (struct sockaddr *)&src_addr, &addr_len)) < 0) {
            //spdlog::critical("Receive Error {} occurred: {}", std::to_string(errno), strerror(errno));
	    rcv_cond.notify_all();
            continue;
        }

	bool succeeded =  rcv_pkt.try_enqueue(norm_buf);
	while(!succeeded & !terminate) {
	    succeeded = rcv_pkt.try_enqueue(norm_buf);
	}
	cnt += 1;
    }
    spdlog::debug("Done with the recv thread!");
    spdlog::critical("Network received this many GENERIC packets: {} for thread {}", cnt, gettid());
}

// To only be called by the sender
void Network::done() {
    stop_threads();
}

/*Sets up a datagram receiver socket for chosen_ip_addr*/  
int Network::setup_listener_socket(std::string curr_ip) {
    struct addrinfo hints, *servinfo; //, *temp;
    int s_fd;
    //int yes = 1;
    
    spdlog::critical("USING UDP with RECV PORT: {} with curr ip: {}!", RECV_PORT, curr_ip);
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_flags = AI_PASSIVE;
    int64_t status = getaddrinfo(NULL /*curr_ip.c_str()*/, RECV_PORT.c_str(), &hints, &servinfo);
    if (status != 0) {
        spdlog::critical("Cannot get getaddrinfo for IP {}, Error {} occurred: {}", curr_ip.c_str(), std::to_string(status), gai_strerror(status));
        return -1;
    }
    
    if ((s_fd = socket(AF_INET, SOCK_DGRAM, 0)) == -1) {
        spdlog::critical("Cannot get socket fd, Error {} occurred: {}", std::to_string(errno), strerror(errno));
        return -1;
    }
    
    /*if (setsockopt(s_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
        spdlog::critical("Cannot set SO_REUSEADDR socket options, Error {} occurred: {}", std::to_string(errno), strerror(errno));
        freeaddrinfo(servinfo);
        return -1;
    }
    if (setsockopt(s_fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(int)) == -1) {
        spdlog::critical("Cannot set SO_REUSEPORT socket options, Error {} occurred: {}", std::to_string(errno), strerror(errno));
        freeaddrinfo(servinfo);
        return -1;
    }*/

    struct timeval timeout;
    timeout.tv_sec = 0;  // 5 seconds timeout
    timeout.tv_usec = batch_timeout * 1000000;	
    if (setsockopt(s_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
        spdlog::critical("Cannot set socket options, Error {} occurred: {}", std::to_string(errno), strerror(errno));
        return -1;
    }

    struct sockaddr_in local_addr;
    local_addr.sin_family = AF_INET;
    spdlog::critical("RECEIVER PORT: {}", std::stoi(RECV_PORT));
    local_addr.sin_port = htons(std::stoi(RECV_PORT));
    local_addr.sin_addr.s_addr = inet_addr(curr_ip.c_str()); 
    memset(local_addr.sin_zero, '\0', sizeof(local_addr.sin_zero));

    if (bind(s_fd, (struct sockaddr*)&local_addr, sizeof(local_addr)) == -1) { // TODO abstract error handling into function
        close(s_fd);
        spdlog::critical("Cannot bind socket fd for port {}, Error {} occurred: {}", RECV_PORT, std::to_string(errno), strerror(errno));
        return -1;
    }
    
    //}
    /*if (temp == NULL) {
        spdlog::critical("Socket failed to bind!");
        return -1;
    }*/
    freeaddrinfo(servinfo);
    return s_fd;
}

/*Sets up a datagram UDP socket for destination IP */  
int Network::setup_talker_socket(std::string dst_ip, std::string dst_port, bool use_multicast) {
    int s_fd;
    if (use_multicast) { 
        s_fd = socket(AF_INET, SOCK_DGRAM, 0);
        if (s_fd < 0) {
            perror("Socket creation failed");
            return -1;
        }       

        struct ip_mreqn mreqn;
        memset(&mreqn, 0, sizeof(mreqn));
        
        // Get the index of the interface you WANT to send from (e.g., "enp1s0d1")
        mreqn.imr_ifindex = if_nametoindex(send_interface.c_str());
        if (mreqn.imr_ifindex == 0) {
            spdlog::error("Interface {} not found", send_interface);
            close(s_fd);
            return -1;
        }

        // Tell the kernel: "Send all multicast traffic from this socket via this interface"
        if (setsockopt(s_fd, IPPROTO_IP, IP_MULTICAST_IF, (char *)&mreqn, sizeof(mreqn)) < 0) {
            spdlog::critical("Setting outgoing multicast interface error: {}", strerror(errno));
            close(s_fd);
            return -1;
        }

        // Optional: Set TTL if you need to cross routers (default is usually 1)
        int ttl = 64;
        setsockopt(s_fd, IPPROTO_IP, IP_MULTICAST_TTL, (char *)&ttl, sizeof(ttl));

        // 1. Define the Multicast Destination
        struct sockaddr_in dest_addr;
        memset(&dest_addr, 0, sizeof(dest_addr));
        dest_addr.sin_family = AF_INET;
        dest_addr.sin_addr.s_addr = inet_addr(dst_ip.c_str());
        dest_addr.sin_port = htons(std::stoi(dst_port));
        
        // 2. "Connect" the UDP socket to the multicast group
        if (connect(s_fd, (struct sockaddr*)&dest_addr, sizeof(dest_addr)) < 0) {
            spdlog::critical("Connect to multicast group failed: {}", strerror(errno));
            close(s_fd);
            return -1;
        }
    } else {
        struct addrinfo hints, *servinfo, *temp;
        memset(&hints, 0, sizeof hints);
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_DGRAM; // Normal UDP Datagram Socket
        int64_t status = getaddrinfo(dst_ip.c_str(), dst_port.c_str(), &hints, &servinfo);
        if (status != 0) {
            spdlog::debug("Cannot get getaddrinfo for IP {}, Error {} occurred: {}", dst_ip.c_str(), status, gai_strerror(status));
            return -1;
        }
        for (temp = servinfo; temp != NULL; temp = temp->ai_next) {
            if ((s_fd = socket(temp->ai_family, temp->ai_socktype, temp->ai_protocol)) == -1) {
                spdlog::debug("No socket created! Error {} occurred: {}. Still searching...", std::to_string(errno), strerror(errno));
                continue;
            }
            if (connect(s_fd, temp->ai_addr, temp->ai_addrlen) == -1) { // TODO abstract error handling into function
                close(s_fd);
                spdlog::critical("Cannot bind socket fd, Error {} occurred: {}", std::to_string(errno), strerror(errno));
                continue;
            }
            //memcpy(it.get(), temp, sizeof (struct addrinfo));
            break;
        }
        if (temp == NULL) {
            spdlog::critical("Failed to create socket!");
            return -1;
        }
        freeaddrinfo(servinfo);
    }
    return s_fd;
}

int Network::setup_multicast_receiver(std::string self_ip) {
    (void) self_ip;
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    // 1. Bind the socket to the PORT
    struct sockaddr_in localAddr;
    memset(&localAddr, 0, sizeof(localAddr));
    localAddr.sin_family = AF_INET;
    localAddr.sin_port = htons(std::stoi(RECV_PORT));
    // Still bind to INADDR_ANY to accept the packets at the OS level
    localAddr.sin_addr.s_addr = htonl(INADDR_ANY); 

    bind(sockfd, (struct sockaddr*)&localAddr, sizeof(localAddr)); 

    struct ip_mreqn mreq;
    mreq.imr_multiaddr.s_addr = inet_addr(multicast_ip.c_str());
    mreq.imr_address.s_addr = htonl(INADDR_ANY); //self_ip.c_str()); // Use default interface // TODO TODO
    mreq.imr_ifindex = if_nametoindex(send_interface.c_str()); //"enp1s0d1");

    if (setsockopt(sockfd, IPPROTO_IP, IP_ADD_MEMBERSHIP, (char *)&mreq, sizeof(mreq)) < 0) {
	spdlog::critical("Adding multicast group error");
        close(sockfd);
        return -1;
    } 
    int reuse = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (char *)&reuse, sizeof(reuse)) < 0) {
        spdlog::error("SO_REUSEADDR failed");
    }
    spdlog::debug("Multicast receiver created! Multicast IP is {}", multicast_ip);
    return sockfd;
}

bool Network::send_packet(std::unique_ptr<char[]> send_packet, 
		          uint64_t pkt_len, 
			  uint64_t pkt_type, 
			  int eth_type,
			  std::array<uint8_t,6> dst_mac,
			  in_addr_t dst_ip) {
    (void) pkt_type;
    bool sent_all = true;

    // If socket_type UDP
    if (socket_type == "UDP") {
        return false;
    }

    // If the packet type is a descriptive string to indicate header type
    for (int j = 0; j < 6; j++) { // 48 bit mac address - local broadcast
        send_eth_hdr.get()->h_dest[j] = dst_mac[j];
        sin.sll_addr[j] = dst_mac[j];
    }
    send_eth_hdr.get()->h_proto = htons(eth_type);
    /* Running a raw socket based protocol */
    size_t packet_size = sizeof(struct ethhdr) + sizeof(struct iphdr) + pkt_len;

    std::unique_ptr<char[]> packet = std::make_unique<char[]>(packet_size); // TODO phase this out eventually
    
    /*Create ethernet header - dest addr will currently indicate multicast TODO unicast*/
    memcpy(packet.get(), send_eth_hdr.get(), sizeof(struct ethhdr));
    
    /*Create IP header*/
    send_ip_hdr.get()->tot_len  = htons(sizeof(struct iphdr) + pkt_len);
    send_ip_hdr.get()->daddr = dst_ip; // destination address
    send_ip_hdr.get()->check = checksum((unsigned short *)packet.get(), sizeof(struct iphdr)); // checksum ONLY for the IPv4 header
    memcpy(packet.get() + sizeof(struct ethhdr), send_ip_hdr.get(), sizeof(struct iphdr));
    memcpy(packet.get() + sizeof(struct ethhdr) + sizeof(struct iphdr), reinterpret_cast<const char*>(send_packet.get()), pkt_len);

    ssize_t num_bytes = 0;
    if ((num_bytes = sendto(send_socket, packet.get(), packet_size, 0, (struct sockaddr*)(&sin), sizeof(sin))) < 0 || 
            ((uint64_t)num_bytes != packet_size)) {
        spdlog::warn("Error {} occurred: {}", std::to_string(errno), strerror(errno));
        spdlog::debug("Num bytes sent: {} vs. expected: {}", num_bytes, packet_size);
        sent_all = false;
    }
    return sent_all;
}

bool Network::send_client_udp_packet(std::unique_ptr<char[]> send_packet,  // TODO get rid of this
		          uint64_t pkt_len, 
			  std::string dst_ip,
			  std::string dst_port) {
    bool sent_all = false;
    
    // If socket_type is not UDP
    if (socket_type != "UDP") {
        return false;
    }
    
    spdlog::debug("Sending a UDP packet!");
    int s_fd;
    spdlog::debug("SINGLE CLIENT Dst ip: {} with Dst Port: {}", dst_ip, dst_port);
    std::string combined_addr = dst_ip + ":" + dst_port;
    if (port_to_fd.count(combined_addr) > 0) {
        s_fd = port_to_fd[combined_addr];
	if (s_fd < 0) {
            spdlog::critical("SENDER Socket creation for IP {} unsuccessful. Aborting", dst_ip);
            throw std::runtime_error("Can't create sending socket");
	}
    } else {
        s_fd = setup_talker_socket(dst_ip, dst_port, false);
        if (s_fd < 0) {
            spdlog::critical("SENDER Socket creation for IP {} unsuccessful. Aborting", dst_ip);
            throw std::runtime_error("Can't create sending socket");
        } 
        port_to_fd.insert({combined_addr, s_fd});
    }
    while (!terminate && !sent_all) {
        ssize_t num_bytes = send(s_fd, send_packet.get(), pkt_len, 0);
        if (num_bytes < 0 || ((uint64_t)num_bytes != pkt_len)) {
            //spdlog::warn("Send Error {} occurred: {}", std::to_string(errno), strerror(errno));
        } else {
             spdlog::info("Successfully sent {} bytes to the receiver!", std::to_string(num_bytes));
	     sent_all = true;
        }
    }
    return sent_all;
}


bool Network::send_udp_packet(std::unique_ptr<char[]> send_packet, 
		          uint64_t pkt_len, 
			  std::string dst_ip,
			  std::string dst_port,
			  bool use_multicast) {
    bool sent_all = false;
    // If socket_type is not UDP
    if (socket_type != "UDP") {
        return false;
    }
    
    int s_fd;
    std::string combined_addr = dst_ip + ":" + dst_port;
    if (port_to_fd.count(combined_addr) > 0) {
        s_fd = port_to_fd[combined_addr];
	if (s_fd < 0) {
            spdlog::critical("SENDER Socket from the port_to_fd chat for IP {} unsuccessful. Aborting", dst_ip);
            throw std::runtime_error("Can't get sending socket");
	}
    } else {
        s_fd = setup_talker_socket(dst_ip, dst_port, use_multicast);
        if (s_fd < 0) {
            spdlog::critical("SENDER Socket creation for IP {} unsuccessful. Aborting", dst_ip);
            throw std::runtime_error("Can't create sending socket");
        } 
        port_to_fd.insert({combined_addr, s_fd});
    }

    if (batch_on && pkt_len > 0) {
        memcpy(final_send_packet + running_pkt_size, send_packet.get(), pkt_len);
        running_pkt_size += pkt_len; 
	num_pkts += 1;
    } else if (!batch_on && pkt_len > 0) {
        memcpy(final_send_packet, send_packet.get(), pkt_len);
	running_pkt_size = pkt_len;
	num_pkts = 1;
    }

    auto curr_time = (std::chrono::steady_clock::now()).time_since_epoch();
    double curr_time_s = std::chrono::duration_cast<std::chrono::duration<double>>(curr_time).count();
    double dur = curr_time_s - batch_timer;
    if (batch_on && num_pkts < batch_size && (running_pkt_size + pkt_len) < MAX_PACKET_SIZE && dur < batch_timeout) {
        // Copy new packet into the batch and update the running packet size for the append request batch
        spdlog::info("Waiting to fill the batch!");
	return false;
    }
    if (running_pkt_size == 0 || num_pkts == 0) {
        spdlog::info("No packets or the packet size is zero");
        return false;
    }
  
    ((struct ring_type*)(final_send_packet))->num_entries = htons(num_pkts); 
    char* actual_test_send = (char*)std::malloc(running_pkt_size);    
    memcpy(actual_test_send, final_send_packet, running_pkt_size);
    while (!terminate && !sent_all) {
        ssize_t num_bytes = send(s_fd, actual_test_send, running_pkt_size, 0);
        if (num_bytes < 0 || ((uint64_t)num_bytes != running_pkt_size)) {
            spdlog::warn("Send Error {} occurred: {}", std::to_string(errno), strerror(errno));
        } else {
            spdlog::info("Successfully sent {} bytes to the receiver!", std::to_string(num_bytes));
	    sent_all = true;
        }
    }
    free(actual_test_send);
    running_pkt_size = 0;
    num_pkts = 0;
    auto new_time = (std::chrono::steady_clock::now()).time_since_epoch();
    batch_timer = std::chrono::duration_cast<std::chrono::duration<double>>(new_time).count();
    return sent_all;
}

char* Network::recv_packet() { // do you need to memset? TODO
    int numbytes = 0;
    struct sockaddr_storage src_addr;
    socklen_t addr_len = sizeof src_addr;

    //TODO epoll    
    if ((numbytes = recvfrom(recv_socket, norm_buf, MAX_PACKET_SIZE, 0, (struct sockaddr *)&src_addr, &addr_len)) < 0) {
        //spdlog::warn("Receiver Error {} occurred: {}", std::to_string(errno), strerror(errno));
        return NULL;
    }
    //spdlog::debug("Returning unrelated buffer!");
    return norm_buf;
}

int Network::get_recv_socket() {
    return recv_socket;
}

char* Network::recv_packet(int udp_recv_socket) { // do you need to memset? TODO
    (void) udp_recv_socket;
    int numbytes = 0;
    struct sockaddr_storage src_addr;
    socklen_t addr_len = sizeof src_addr;

    while ((numbytes = recvfrom(recv_socket, norm_buf, MAX_PACKET_SIZE, 0, (struct sockaddr *)&src_addr, &addr_len)) < 0) {
        return NULL;
    }
    return norm_buf;
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
    spdlog::debug("Starting to clean up threads!");
    {
        std::unique_lock<std::mutex> lock(lock_terminate);
        terminate = true;
    }
    
    spdlog::debug("The threads are being cleaned up!");
    mutex_condition.notify_all();
    for (auto it = port_to_fd.begin(); it != port_to_fd.end(); ++it) {
        close(it->second);
    }
    close(recv_socket);

    for (uint64_t i = 0; i < send_threads.size(); i++) {
        send_threads[i].join();
    }
    send_threads.clear();
    for (uint64_t i = 0; i < recv_threads.size(); i++) {
        recv_threads[i].join();
    }
    recv_threads.clear();
    free(norm_buf);
    free(final_send_packet);
}

bool Network::check_socket_type(std::string socket_type) {
    return (socket_type == "UDP" || socket_type == "RAW");
}

bool Network::validate_ip_address(const std::string &ip_addr) {
    struct sockaddr_in sa;
    int result = inet_pton(AF_INET, ip_addr.c_str(), &(sa.sin_addr));
    return result != 0;
}

uint16_t Network::string_to_u16(const std::string& str) {
    unsigned long value = std::stoul(str);
    
    if (value > 65535) {
        throw std::out_of_range("Value too large for uint16_t");
    }
    
    return static_cast<uint16_t>(value);
}
