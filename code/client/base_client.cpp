//#include "utils.h"
#include "base_client.h"
#include <iostream>

/*** Main function ***/
void BaseClient::initClient(std::string config_file) {
	// Get config object
	Config configObj = readConfigFile(config_file);

	// Initialize client variables
	ip_addr = configObj.ip_addr;
	port = configObj.port;
	client_type = fromStringToClientType(configObj.client_type);
	isRegistered = false;
}

BaseClient createClient(ClientType type, std::string config_file) {
	BaseClient client = NULL;
	switch (type) {
		case ClientType.RING:
			client = new LogClient(config_file);
			break;
		case ClientType.DUMMY:
			client = new SimpleClient(); //TODO: create SimpleClient
			break;
		case ClientType.SCALOG:
			break;
		case ClientType.CORFU:
			break;
		default:
			// code block
	}
}

/*
 * Sets up sockets and underlying network infrastructure so the client
 * can send packets to remote machines
 */
void registerClient() {
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
	ClientType type = ClientType.NONE;
	if (type == "dummy") {
		type = ClientType.DUMMY;
	} else if (type == "ring") {
		type = ClientType.RING;
	} else if (type == "scalog") {
		type = ClientType.SCALOG;
	} else if (type == "corfu") {
		type = ClientType.CORFU;
	}
	return type;
}

Config readConfigFile(std::string config_file) { // combine with main initialiazation function
						 // Make Config object a pointer
	Config configObj;
	// Read in config file
	// TODO: Get yaml-cpp
	YAML::Node config = YAML::LoadFile(config_file);
	// Fill in config object
	configObj.ip_addr = config["ip_addr"].as<std::string>();
	configObj.port = config["port"].as<uint64_t>();
	configObj.client_type = config["type"].as<std::string>();
	return configObj;
}
