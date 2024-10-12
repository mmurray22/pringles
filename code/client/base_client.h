#include <string>
#include <cstdint>

/*** Structs, enums, etc. ***/
struct NetworkConfig {
	ClientType client;
    std::string ip_addr;
	uint64_t port;
};

enum ClientType {
	DUMMY,
	RING,
	SCALOG,
	CORFU,
	NONE
};

class BaseClient {
	public:
			
		/*** Functions ***/

		// Register client with system services (e.g. network)
		void registerClient();
		// Initializes client variables
		void initClient(std::string config_file);
		
		/// Helper fxns
		uint64_t get_cid();

	protected:
		uint64_t cid;

	private:
		/*** Variables ***/
		NetworkConfig* netConfig;
		type;
		int clientSocket;
		bool isRegistered;

		/*** Functions ***/
		
		// Initializes client variables
		ClientType fromStringToClientType(std::string type);
		// Convert config file to NetworkConfig object
		bool readConfigFile(std::string config_file);
}

// Create client object
BaseClient createClient(ClientType type);

