#include <string>

using byte = unsigned char;

public class BaseClient {
	
	public:
		/*** Structs, enums, etc. ***/
		enum ClientType {
			DUMMY,
			RING,
			SCALOG,
			CORFU,
			NONE
		}
		struct Config {
			std::string ip_addr;
			uint64_t port;
			std::string client_type;
		}
		
		/*** Functions ***/

		// Create client object
		BaseClient createClient(ClientType type);
		// Register client with system services (e.g. network)
		void registerClient();
		// Initializes client variables
		void initClient();
		
		/// Helper fxns
		uint64_t get_cid();



	protected:
		uint64_t cid;

	private:
		/*** Variables ***/
		Config* configObj;
		ClientType type;
		std::string ip_addr;
		uint64_t port;
		int clientSocket;
		bool isRegistered;

		/*** Functions ***/
		
		// Initializes client variables
		ClientType fromStringToClientType(std::string type);
		// Convert config file to Config object
		Config readConfigFile(std::string config_file);
}
