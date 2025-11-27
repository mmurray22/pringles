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
                 std::string send_interface,
                 std::string self_ip,
		 uint64_t num_pkt_type,
		 bool run_threads) :  rcv_pkt(1000000)
{ 
    if (geteuid() != 0) { // Check if we are running as root
        throw std::runtime_error("Not running as root!");
    }
    if(!check_socket_type(socket_type)) { 
        throw std::runtime_error("Invalid socket type!");
    }
    set_spdlog_level(log_level);
    this->run_threads = run_threads;
    this->socket_type = socket_type;
    this->send_interface = send_interface;
    this->batch_size = batch_size;
    this->self_ip = self_ip;
    this->batch_on = batch_on;
    this->num_pkt_type = num_pkt_type;
    SEND_PORT = send_port;
    RECV_PORT = recv_port;
    this->pkt_type_to_fd = {};
    send_socket = setup_raw_talker_socket();
    if (send_socket < 0) {
        spdlog::critical("SENDER Socket creation unsuccessful. Aborting");
        throw std::runtime_error("Can't create sending socket");
    }
    spdlog::debug("The socket fd is {}", send_socket);
    recv_socket = setup_listener_socket(self_ip);
    if (recv_socket < 0) {
        spdlog::critical("SENDER Socket creation unsuccessful. Aborting");
        throw std::runtime_error("Can't create receiving socket");
    }
    spdlog::debug("The socket fd is {}", recv_socket);

    this->send_ip_hdr = create_ip_hdr(); //ip_addr, pkt_len, (unsigned short *)packet.get()); 
    this->norm_buf = (char*)std::malloc(MAX_PACKET_SIZE);
    this->send_eth_hdr = create_eth_hdr(send_socket);


    this->sin.sll_ifindex = if_nametoindex((const char*)send_interface.c_str());//ifr.get()->ifr_ifindex;
    this->sin.sll_halen = ETH_ALEN;
    if (run_threads) {
   	 this->send_thread = std::thread(&Network::run_send, this);	
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

// Threadpool send thread function
// Send packets as they are queued
void Network::run_send() {
    /*spdlog::critical("Network Send Thread starting with TID = {}", gettid());
    uint64_t cnt = 0;
    
    std::unique_ptr<struct iphdr> ip = create_ip_hdr(); //ip_addr, pkt_len, (unsigned short *)packet.get()); 

    char* send_packet = (char*)std::malloc(MAX_PACKET_SIZE);
    while (!terminate) {

        uint64_t pkt_len = 0;
        {
            std::unique_lock<std::mutex> lock(send_pkt_qs_mutex); // TODO: PER QUEUE LOCK
            mutex_condition.wait(lock, [&, this] {
                return !send_pkt_qs[pkt_type].empty() || terminate;        
            });

	    if (terminate && send_pkt_qs[pkt_type].empty()) {
		    break;
	    }
            
	    pkt_len = send_pkt_qs[pkt_type].front().first;
	    memcpy(send_packet, send_pkt_qs[pkt_type].front().second.get(), pkt_len);//std::move(send_pkt_qs[pkt_type].front().second);
	    if (send_packet == NULL) { // TODO should this ever be NUL?
		spdlog::debug("The received packet is NULL?");
	    	return;
	    }
	    //spdlog::debug("The number of queued packets is: {}", send_pkt_qs[pkt_type].size());
	    //spdlog::debug("Packet has been found! {}", send_packet.get());

	    //spdlog::debug("Debug: {}", pkt_len);
            send_pkt_qs[pkt_type].pop();
        }
        //spdlog::debug("Past preprocessing! The packet value is still: {}", send_packet.get()); 
*/

	// If socket_type UDP
        /*if (socket_type == "UDP") {
	    spdlog::debug("Sending a UDP packet!");
	    for (size_t i = 0; i < pkt_type_to_ip[pkt_type].size(); i++) {
	    	//std::shared_ptr<struct addrinfo> it = std::make_shared<struct addrinfo>();
            	int s_fd = pkt_type_to_fd[pkt_type][i]; //setup_talker_socket(pkt_type_to_ip[pkt_type][0], it); // TODO: Send to all entries!
            	if (s_fd < 0) {
                    spdlog::critical("SENDER Socket creation for IP {} unsuccessful. Aborting", pkt_type);
                    throw std::runtime_error("Can't create sending socket");
                } 
            	ssize_t num_bytes = sendto(s_fd, send_packet, pkt_len, 0, fd_to_it[s_fd]->ai_addr, fd_to_it[s_fd]->ai_addrlen);
            	if (num_bytes < 0 || ((uint64_t)num_bytes != pkt_len)) {
                    spdlog::warn("Send Error {} occurred: {}", std::to_string(errno), strerror(errno));
		    {
            		std::unique_lock<std::mutex> lock(send_pkt_qs_mutex); // TODO: PER QUEUE LOCK         
	    		send_pkt_qs[pkt_type].pop();
	    	    }
                    continue;
            	}
            	spdlog::info("Successfully sent {} bytes to the receiver with packet value {}", std::to_string(num_bytes), send_packet);
	    }
            continue;
        }

        // If the packet type is a descriptive string to indicate header type
	std::vector<int> send_fds = pkt_type_to_fd[pkt_type];
	for (size_t i = 0; i < send_fds.size(); i++) {
	    int s_fd = send_fds[i];
            if (s_fd < 0) {
                spdlog::critical("No socket found, dropping buffers");
                continue;
            }
            
            std::string ip_addr = pkt_type_to_ip[pkt_type][i];
            //spdlog::critical("IP Address: {}", ip_addr);
            size_t packet_size = sizeof(struct ethhdr) + sizeof(struct iphdr) + pkt_len;
            spdlog::debug("Eth hdr: {}, IP hdr: {}, Size hdr + payload: {}", sizeof(struct ethhdr), sizeof(struct iphdr), pkt_len);

            std::unique_ptr<char[]> packet = std::make_unique<char[]>(packet_size); // TODO phase this out eventually
       
            memcpy(packet.get(), eth_hdr_vecs[i].get(), sizeof(struct ethhdr));
            
	    ip.get()->tot_len  = htons(sizeof(struct iphdr) + pkt_len);
            ip.get()->daddr = inet_addr(ip_addr.c_str()); // destination address
            ip.get()->check = checksum((unsigned short *)packet.get(), sizeof(struct iphdr)); // checksum ONLY for the IPv4 header^

            memcpy(packet.get() + sizeof(struct ethhdr), ip.get(), sizeof(struct iphdr));

            //spdlog::debug("Total packet size is {}", packet_size);

            memcpy(packet.get() + sizeof(struct ethhdr) + sizeof(struct iphdr), reinterpret_cast<const char*>(send_packet), pkt_len);
            ssize_t num_bytes = 0;
   	    for (int j = 0; j < 6; j++) { // 48 bit mac address - local broadcast
       		sin.sll_addr[j] = eth_hdr_vecs[i].get()->h_dest[j];
   	    }
            
            if ((num_bytes = sendto(s_fd, packet.get(), packet_size, 0, (struct sockaddr*)(&sin), sizeof(sin))) < 0 || 
                    ((uint64_t)num_bytes != packet_size)) {
                spdlog::warn("Error {} occurred: {}", std::to_string(errno), strerror(errno));
                spdlog::debug("Num bytes sent: {} vs. expected: {}", num_bytes, packet_size);
                continue;
            }
            spdlog::debug("Successfully sent {} bytes to the receiver", num_bytes);
	    cnt += 1;

	}
	memset(send_packet, 0, MAX_PACKET_SIZE);*/
    //}
    //free(send_packet);
    //spdlog::debug("Done with the send thread focused on {}", pkt_type);
    //spdlog::critical("Network send this many packets: {} for thread {}", cnt, gettid());
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

// this is the compiled pointer to protobuf string
void Network::add_to_send_queue(std::unique_ptr<char[]> buf, uint64_t packet_type, uint64_t packet_size) {
    std::unique_lock<std::mutex> lock(send_pkt_qs_mutex);
    if (terminate) {
        spdlog::info("No more packets accepted!");
        return;
    }
    spdlog::debug("Going to send packet type: {}, with content: {} of size {}.", packet_type, buf.get(), packet_size);
    send_pkt_qs[packet_type].push(std::pair<uint64_t, std::unique_ptr<char[]>>(packet_size, std::move(buf)));
    mutex_condition.notify_one();
    return;
}

char* Network::read_from_recv_queue() {
    /*if (rcv_pkt.empty()) {
	return NULL;
    }
    std::unique_lock<std::mutex> lock(rcv_queue_mutex);
    //spdlog::debug("Number of packets received: {}", rcv_pkt.size());
    char* receive_pkt = rcv_pkt.front();
    rcv_pkt.pop();
    return receive_pkt;*/
    char* receive_pkt = NULL;
    bool succeeded = rcv_pkt.try_dequeue(receive_pkt);
    if (!succeeded) {
        return NULL;
    }
    return receive_pkt;
}

// To only be called by the sender
void Network::done() {
    stop_threads();
}

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
        if ((s_fd = socket(AF_PACKET, SOCK_RAW, htons(ETH_P_ALL))) == -1) {
            spdlog::critical("Cannot get getaddrinfo for IP {}, Error {} occurred: {}", curr_ip.c_str(), std::to_string(errno), strerror(errno)); // TODO
            return -1;
        }
        spdlog::debug("Created raw receive socket of fd {}", s_fd);
	struct timeval timeout;
	timeout.tv_sec = 1;  // 5 seconds timeout
	timeout.tv_usec = 0;
	
	if (setsockopt(s_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
	    spdlog::critical("Cannot set socket options, Error {} occurred: {}", std::to_string(errno), strerror(errno));
            return -1;
	}
	if (setsockopt(s_fd, SOL_SOCKET, SO_BINDTODEVICE, send_interface.c_str(), strlen(send_interface.c_str())) < 0) {
	    perror("Error binding socket to device. Interface name wrong or permissions failed.");
	    close(s_fd);
	    return -1;
	}
	int ignore_outgoing = 1;
	if (setsockopt(s_fd, SOL_PACKET, PACKET_IGNORE_OUTGOING, &ignore_outgoing, sizeof(ignore_outgoing)) < 0) {
	    perror("Error binding socket to device. Interface name wrong or permissions failed.");
	    close(s_fd);
	    return -1;
	}
        return s_fd;
    }
    //spdlog::debug("USING UDP with RECV PORT: {} with curr ip: {}!", RECV_PORT, curr_ip);
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_flags = AI_PASSIVE;
    int64_t status = getaddrinfo(NULL /*curr_ip.c_str()*/, RECV_PORT.c_str(), &hints, &servinfo);
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
       
       	struct timeval timeout;
        timeout.tv_sec = 0;  // 5 seconds timeout
        timeout.tv_usec = 10;	
	if (setsockopt(s_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
	    spdlog::critical("Cannot set socket options, Error {} occurred: {}", std::to_string(errno), strerror(errno));
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
    /*flags = flags | O_NONBLOCK;
    if (fcntl(s_fd, F_SETFL, flags) != 0) {
        spdlog::critical("UNABLE TO SET FCNTL FLAGS");
    }*/
    return s_fd;
}

/*Sets up a datagram UDP socket for destination IP */  
int Network::setup_talker_socket(std::string dst_ip, std::string dst_port) {
    struct addrinfo hints, *servinfo, *temp;
    int s_fd;
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
    return s_fd;
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

bool Network::send_udp_packet(std::unique_ptr<char[]> send_packet, 
		          uint64_t pkt_len, 
			  uint64_t pkt_type, 
			  int eth_type,
			  std::string dst_ip,
			  std::string dst_port) {
    bool sent_all = true;
    (void) pkt_type;
    (void) eth_type;
    
    // If socket_type is not UDP
    if (socket_type != "UDP") {
        return false;
    }
    
    //spdlog::debug("Sending a UDP packet!");

    int s_fd;
    spdlog::debug("Dst ip: {} with Dst Port: {}", dst_ip, dst_port);
    std::string combined_addr = dst_ip + ":" + dst_port;
    if (port_to_fd.count(combined_addr) > 0) {
        s_fd = port_to_fd[combined_addr];
	if (s_fd < 0) {
            spdlog::critical("SENDER Socket creation for IP {} unsuccessful. Aborting", dst_ip);
            throw std::runtime_error("Can't create sending socket");
	}
    } else {
        s_fd = setup_talker_socket(dst_ip, dst_port);
        if (s_fd < 0) {
            spdlog::critical("SENDER Socket creation for IP {} unsuccessful. Aborting", dst_ip);
            throw std::runtime_error("Can't create sending socket");
        } 
        port_to_fd.insert({combined_addr, s_fd});
    }
    ssize_t num_bytes = send(s_fd, send_packet.get(), pkt_len, 0);
    if (num_bytes < 0 || ((uint64_t)num_bytes != pkt_len)) {
        spdlog::warn("Send Error {} occurred: {}", std::to_string(errno), strerror(errno));
        sent_all = false;
    } else {
         //spdlog::info("Successfully sent {} bytes to the receiver!", std::to_string(num_bytes));
    }
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
    int numbytes = 0;
    struct sockaddr_storage src_addr;
    socklen_t addr_len = sizeof src_addr;

    while ((numbytes = recvfrom(udp_recv_socket, norm_buf, MAX_PACKET_SIZE, 0, (struct sockaddr *)&src_addr, &addr_len)) < 0) {
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
    /*uint64_t total_time = 0;
    while (num_sends_done < num_pkts_type && num_recv_done < 1) {
	if (total_time >= MAX_CLEANUP_TIME) {
	   break;
	}
        //sleep(2); // probably better way to do this
	total_time += 2;
    }*/
    spdlog::debug("The threads are being cleaned up!");
    mutex_condition.notify_all();
    for (auto it = port_to_fd.begin(); it != port_to_fd.end(); ++it) {
        close(it->second);
    }
    for (uint64_t i = 0; i < send_threads.size(); i++) {
        send_threads[i].join();
    }
    send_threads.clear();
    for (uint64_t i = 0; i < recv_threads.size(); i++) {
        recv_threads[i].join();
    }
    recv_threads.clear();
    if (run_threads) {
        free(norm_buf);
    }
}

bool Network::check_socket_type(std::string socket_type) {
    return (socket_type == "UDP" || socket_type == "RAW");
}

bool Network::validate_ip_address(const std::string &ip_addr) {
    struct sockaddr_in sa;
    int result = inet_pton(AF_INET, ip_addr.c_str(), &(sa.sin_addr));
    return result != 0;
}

void Network::add_pkt_type(uint64_t pkt_type) {
    //send_pkt_qs.insert(std::pair<uint64_t, std::queue<std::unique_ptr<char[]>>>(pkt_type, std::queue<std::unique_ptr<char[]>>()));
    (void) pkt_type;
}

bool Network::remove_pkt_type(uint64_t pkt_type) {
    for (auto it = send_pkt_qs.begin(); it != send_pkt_qs.end(); it++) {
        if (it->first == pkt_type) {
            send_pkt_qs.erase(it);
            return true;
        }
    }
    return false;
}
