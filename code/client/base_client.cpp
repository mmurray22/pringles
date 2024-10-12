//#include "utils.h"
#include "base_client.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <iostream>
#include "yaml-cpp/yaml.h"

/*** Main function ***/
void BaseClient::initClient(std::string config_file) {
	// Get config object
	bool success = readConfigFile(config_file);
	// Initialize client variables
	ip_addr = configObj.ip_addr;
	port = configObj.port;
	ClientType client_type = fromStringToClientType(configObj.client_type);
	isRegistered = false;
}

BaseClient createClient(ClientType type) {
	BaseClient client;
	switch (type) {
		case ClientType::RING:
			//client = new LogClient();
			break;
		case ClientType::DUMMY:
			//client = new SimpleClient(); //TODO: create SimpleClient
			break;
		case ClientType::SCALOG:
			break;
		case ClientType::CORFU:
			break;
		default:
			// code block
	};
	return client;
}

/*
 * Sets up sockets and underlying network infrastructure so the client
 * can send packets to remote machines
 */
void BaseClient::registerClient() {
	int new_socket = socket(AF_INET, SOCK_STREAM, 0);
	if (new_socket == -1) {
		std::cerr << "Error creating socket" << std::endl;
        	return;
    	}

	clientSocket = new_socket;
	isRegistered = true;
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

bool BaseClient::readConfigFile(std::string config_file) { // combine with main initialiazation function
	// Read in config file
	YAML::Node config = YAML::LoadFile(config_file);
	// Fill in config object
	configObj->ip_addr = config["ip_addr"].as<std::string>();
	configObj->port = config["port"].as<uint64_t>();
    std::client_str = config["type"].as<std::string>();
	configObj->client_type = fromStringToClientType(client_str);
	return configObj;
}
