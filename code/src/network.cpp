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
                 std::string send_port, 
                 std::string recv_port,
                 std::string socket_type,
                 uint64_t log_level,
                 uint64_t batch_size,
                 bool batch_on,
                 std::string send_interface,
                 std::string self_ip,
		 std::map<uint64_t, std::vector<std::string>> pkt_type_to_ip,
		 std::vector<int> pkt_type_to_eth_type) {
    
    if (geteuid() != 0) { // Check if we are running as root
        throw std::runtime_error("Not running as root!");
    }
    if(!check_socket_type(socket_type)) { 
        throw std::runtime_error("Invalid socket type!");
    }
    
    this->socket_type = socket_type;
    this->send_interface = send_interface;
    this->batch_size = batch_size;
    this->self_ip = self_ip;
    this->batch_on = batch_on;
    total_num_threads = maxThreads;

    SEND_PORT = send_port;
    RECV_PORT = recv_port;

    set_spdlog_level(log_level);

    /* Initialize sockets */
    this->pkt_type_to_ip = pkt_type_to_ip;
    this->pkt_type_to_fd = {};
    for (auto it =  pkt_type_to_ip.begin(); it != pkt_type_to_ip.end(); it++) {
	if (it->second.size() < 1) {
		spdlog::debug("Packet type {} has NO IP addresses!", it->first);
		continue;
	}
	pkt_type_to_fd.insert({it->first, {}});
        for (uint64_t i = 0; i < it->second.size(); i++) {
		std::shared_ptr<struct addrinfo> socket_it = std::make_shared<struct addrinfo>();
        	int socket = socket_type == "UDP" ? setup_talker_socket(it->second[i], socket_it) : setup_raw_talker_socket();
        	if (socket < 0) {
            		spdlog::critical("SENDER Socket creation for IP {} unsuccessful. Aborting", it->second[i]);
            		throw std::runtime_error("Can't create sending socket");
		}
		pkt_type_to_fd[it->first].push_back(socket);
		spdlog::debug("The key is {} and the socket fd is {}", it->first, socket);
		fd_to_it.insert({socket, socket_it});
    	}
	num_pkt_type += 1;
    }
    spdlog::debug("The number ofd fds for packet type 0 is {}", this->pkt_type_to_fd[0].size());

    spdlog::debug("Receiver socket setup start!");	
    recv_socket = setup_listener_socket(self_ip); // TODO: Only need one receive socket?
    spdlog::debug("Receiver socket setup done!");	
    /* End of new initializing socket */


    /*Initialize queues and threads*/
    //total_num_threads = maxThreads == 0 ? std::thread::hardware_concurrency()-1 : maxThreads; TODO add multiple threads in later
    for (const auto& [key, value] : pkt_type_to_ip) {
        send_pkt_qs.insert(std::pair<uint64_t, std::queue<std::pair<uint64_t, std::unique_ptr<char[]>>>>(key, std::queue<std::pair<uint64_t, std::unique_ptr<char[]>>>()));
	if (socket_type == "UDP") {
             send_threads.emplace_back(std::thread(&Network::run_send, this, key, 0));	
	} else {
             send_threads.emplace_back(std::thread(&Network::run_send, this, key, pkt_type_to_eth_type[key]));	
	}
    }

    // Create receiving threadpool (only 1 thread for now since it's Network I/O bound)
    recv_threads.emplace_back(std::thread(&Network::run_recv, this, recv_socket)); // TODO: Fix this jesus christ
}
    
Network::~Network() {
	// TODO cleanup???
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
void Network::run_send(uint64_t pkt_type, int eth_type) {
    spdlog::debug("RUNNING SEND THREAD for packet type: {}", pkt_type);

    while (!terminate) {
        std::unique_ptr<char[]> send_packet = std::make_unique<char[]>(MAX_PACKET_SIZE); 
        uint64_t pkt_len = 0;
        {
            std::unique_lock<std::mutex> lock(send_pkt_qs_mutex); // TODO: PER QUEUE LOCK
            mutex_condition.wait(lock, [&, this] {
                return !send_pkt_qs[pkt_type].empty() || terminate;        
            });

	    if (terminate && send_pkt_qs[pkt_type].empty()) {
		    break;
	    }
            
	    send_packet = std::move(send_pkt_qs[pkt_type].front().second);
	    if (send_packet.get() == NULL) { // TODO should this ever be NUL?
		spdlog::debug("The received packet is NULL?");
	    	return;
	    }
	    spdlog::debug("The number of queued packets is: {}", send_pkt_qs[pkt_type].size());
	    spdlog::debug("Packet has been found! {}", send_packet.get());
	    pkt_len = send_pkt_qs[pkt_type].front().first;
	    spdlog::debug("Debug: {}", pkt_len);
            send_pkt_qs[pkt_type].pop();
        }
        spdlog::debug("Past preprocessing! The packet value is still: {}", send_packet.get()); 


	// If socket_type UDP
        if (socket_type == "UDP") {
	    spdlog::debug("Sending a UDP packet!");
	    for (size_t i = 0; i < pkt_type_to_ip[pkt_type].size(); i++) {
	    	//std::shared_ptr<struct addrinfo> it = std::make_shared<struct addrinfo>();
            	int s_fd = pkt_type_to_fd[pkt_type][i]; //setup_talker_socket(pkt_type_to_ip[pkt_type][0], it); // TODO: Send to all entries!
            	if (s_fd < 0) {
                    spdlog::critical("SENDER Socket creation for IP {} unsuccessful. Aborting", pkt_type);
                    throw std::runtime_error("Can't create sending socket");
                } 
            	ssize_t num_bytes = sendto(s_fd, send_packet.get(), pkt_len, 0, fd_to_it[s_fd]->ai_addr, fd_to_it[s_fd]->ai_addrlen);
            	if (num_bytes < 0 || ((uint64_t)num_bytes != pkt_len)) {
                    spdlog::warn("Send Error {} occurred: {}", std::to_string(errno), strerror(errno));
                    continue;
            	}
            	spdlog::info("Successfully sent {} bytes to the receiver with packet value {}", std::to_string(num_bytes), send_packet.get());
	    }
            continue;
        }

        // If the packet type is a descriptive string to indicate header type
	std::vector<int> send_fds = pkt_type_to_fd[pkt_type];
	spdlog::debug("!!!!!We found {} number of file descriptors for packet type {}", send_fds.size(), pkt_type);
	for (size_t i = 0; i < send_fds.size(); i++) {
	    int s_fd = send_fds[i];
            if (s_fd < 0) {
                spdlog::critical("No socket found, dropping buffers");
                continue;
            }
            
            /* Running a raw socket based protocol */
            std::string ip_addr = pkt_type_to_ip[pkt_type][i];
            size_t packet_size = sizeof(struct ethhdr) + sizeof(struct iphdr) + pkt_len;
            spdlog::debug("Eth hdr: {}, IP hdr: {}, Size hdr + payload: {}", sizeof(struct ethhdr), sizeof(struct iphdr), pkt_len);
            std::unique_ptr<char[]> packet = std::make_unique<char[]>(packet_size);
       
            /*Create ethernet header - dest addr will currently indicate multicast TODO unicast*/
            std::unique_ptr<struct ethhdr> eth = create_eth_hdr(s_fd, eth_type); // TODO?????
            memcpy(packet.get(), eth.get(), sizeof(struct ethhdr));
            
            /*Create IP header*/
            std::unique_ptr<struct iphdr> ip = create_ip_hdr(ip_addr, pkt_len, (unsigned short *)packet.get()); 
            memcpy(packet.get() + sizeof(struct ethhdr), ip.get(), sizeof(struct iphdr));

            /*Protocol specific code starts*/
            spdlog::debug("Total packet size is {}", packet_size);
            /*Protocol specific code ends*/

            memcpy(packet.get() + sizeof(struct ethhdr) + sizeof(struct iphdr), reinterpret_cast<const char*>(send_packet.get()), pkt_len);
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
                    ((uint64_t)num_bytes != packet_size)) {
                spdlog::warn("Error {} occurred: {}", std::to_string(errno), strerror(errno));
                spdlog::debug("Num bytes sent: {} vs. expected: {}", num_bytes, packet_size);
                continue;
            }
            spdlog::debug("Successfully sent {} bytes to the receiver", std::to_string(num_bytes));
	}
    }
    spdlog::debug("Done with the send thread focused on {}", pkt_type);
}

// TODO
std::string Network::get_ip(uint64_t pkt_type, int idx) {
    return pkt_type_to_ip[pkt_type][idx];
}

std::shared_ptr<struct addrinfo> Network::get_it(int s_fd) {
    return fd_to_it[s_fd]; 
}

/*Create ethernet header*/
std::unique_ptr<struct ethhdr> Network::create_eth_hdr(int s_fd, int eth_type) {
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


/*Create IP header*/
std::unique_ptr<struct iphdr> Network::create_ip_hdr(std::string dst_ip, size_t size_of_pkt, unsigned short* pkt) {
    std::unique_ptr<struct iphdr> ip = std::make_unique<struct iphdr>();
    ip.get()->ihl      = 5; //version length
    ip.get()->version  = 4; // version; should we allow for ipv6?
    ip.get()->tos      = 0; // type of service - set to normal, could change in future
    ip.get()->tot_len  = sizeof(struct iphdr) + size_of_pkt; // total length of packet header
    ip.get()->id       = htons(54321); // default ID number for ip packet
    ip.get()->ttl      = 64; // default hops; circle back in case of change
    ip.get()->protocol = IPPROTO_RAW; // Raw IP

    ip.get()->saddr = inet_addr(self_ip.c_str()); // source address
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
        int ret = epoll_wait(efd, &ev, 1, MAX_POLL_TIME); // maxevents??
	if (ret == -1) {
		spdlog::debug("epoll_wait failed!");
	}
        std::unique_ptr<char[]> buf = std::make_unique<char[]>(MAX_PACKET_SIZE);  
        if ((numbytes = recvfrom(s_fd, buf.get(), MAX_PACKET_SIZE, 0, (struct sockaddr *)&src_addr, &addr_len)) == -1) {
            //spdlog::warn("Receive Error {} occurred: {}", std::to_string(errno), strerror(errno));
            continue;
        }
	rcv_pkt.emplace(std::move(buf));
    }
    spdlog::debug("Done with the recv thread!");
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

std::unique_ptr<char[]> Network::read_from_recv_queue() {
    std::unique_lock<std::mutex> lock(rcv_queue_mutex);
    if (rcv_pkt.empty()) {
        return NULL;
    }
    spdlog::debug("Number of packets received: {}", rcv_pkt.size());
    std::unique_ptr<char[]> receive_pkt = std::move(rcv_pkt.front());
    rcv_pkt.pop();
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
        if ((s_fd = socket(AF_PACKET, SOCK_RAW, htons(ETH_P_ALL))) == -1) { // TODO TODO: INCORRECT CHANGE TO AF_PACKET AS PER MAN 7 packet
            spdlog::critical("Cannot get getaddrinfo for IP {}, Error {} occurred: {}", curr_ip.c_str(), std::to_string(errno), strerror(errno));
            return -1;
        }
        spdlog::debug("Created raw receive socket of fd {}", s_fd);
	struct ifreq ifr;
	memset(&ifr, 0, sizeof(ifr));
	strncpy(ifr.ifr_name, send_interface.c_str(), IFNAMSIZ - 1);
	if (setsockopt(s_fd, SOL_SOCKET, SO_BINDTODEVICE, (void *)&ifr, sizeof(ifr)) < 0) {
	    perror("Error binding socket to device. Interface name wrong or permissions failed.");
	    close(s_fd);
	    return -1;
	}
        /*if (setsockopt(s_fd, IPPROTO_IP, SO_REUSEADDR | IP_HDRINCL, &yes, sizeof(int)) == -1) {
            spdlog::critical("Cannot set socket options, Error {} occurred: {}", std::to_string(errno), strerror(errno));
            return -1;
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

/*Sets up a datagram UDP socket for curr_ip*/  
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
            spdlog::debug("No socket created! Error {} occurred: {}. Still searching...", std::to_string(errno), strerror(errno));
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
