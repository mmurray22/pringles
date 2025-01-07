//#include "utils.h"
#include "base_client.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <iostream>
#include "yaml-cpp/yaml.h"

// Different Clients
#include "simple_client.h"
#include "pringles_client.h"

/*** Main function ***/
BaseClient createClient(std::string config_file) {
    YAML::Node config = YAML::LoadFile(config_file);
    std::client_str = config["type"].as<std::string>();
    ClientType type = fromStringToClientType(client_str);

	switch (type) {
		case ClientType::RING:
			return LogClient(config);
		case ClientType::SCALOG:
			break;
		case ClientType::CORFU:
			break;
		default:
            return SimpleClient(config);
	};
}

uint64_t BaseClient::get_cid() {
    return cid;
}

/*** Helper function ***/
ClientType BaseClient::fromStringToClientType(std::string type) {
	ClientType cliType = ClientType::NONE;
	if (type == "dummy") {
		cliType = ClientType::DUMMY;
	} else if (type == "ring") {
		cliType = ClientType::RING;
	} else if (type == "scalog") {
		cliType = ClientType::SCALOG;
	} else if (type == "corfu") {
		cliType = ClientType::CORFU;
	}
	return cliType;
}

// Run in separate thread
// ip address passed in needs to be the external IP
void send_packets(std::string ip) {
    /*Go through the steps of setting up a datagram UDP socket*/
    struct addrinfo hint, *res;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM; //Datagram socket
    int64_t status = getaddrinfo(ip.c_str(), NULL, &hints, res);
    if (status != 0) {
        std::cout << "Error " << status << "occurred: " << gai_strerror(status) << std::endl;
        return;
    }
    int s_fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);    
    if (s_fd == -1) {
         std::cout << "Error " << errno << "occurred: " << strerror(errno) << std::endl;
        return;
    }
    int ret = connect(s_fd, res->ai_addr, res->ai_addrlen);
    if (ret == -1) {
        std::cout << "Error " << errno << "occurred: " << strerror(errno) << std::endl;
    }

    while (true) {
        if (send_pkt.size() == 0) {
            sleep(10);
            continue;
        }
        if (/*check if the ip has changed*/) {
        
        }
     
        std::string* buf = send_pkt.dequeue();
        ssize_t num_bytes = send(s_fd, buf, *buf.length(), 0);
        if (num_bytes != *buf.length()) {
             if (num_bytes == -1) {
                 std::cout << "Error " << errno << "occurred: " << strerror(errno) << std::endl;
             }
        }
    }
}

void receive_packets() {
    // listen for receiving info from the ring
    struct sockaddr_storage their_addr;
    socklen_t addr_size;
    struct addrinfo hints, *res;
    int sockfd, new_fd;

    // !! don't forget your error checking for these calls !!

    // first, load up address structs with getaddrinfo():

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;  // use IPv4 or IPv6, whichever
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_flags = AI_PASSIVE;     // fill in my IP for me

    getaddrinfo(NULL, RECEIVE_PORT, &hints, &res);

    // make a socket, bind it, and listen on it:
    sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    bind(sockfd, res->ai_addr, res->ai_addrlen);
    listen(sockfd, BACKLOG);
}
