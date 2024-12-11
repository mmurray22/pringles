#include <string>
#include <cstdint>
#include <memory>

/*** Structs, enums, etc. ***/
enum ClientType {
	SIMPLE,
	RING,
	SCALOG,
	CORFU,
	NONE
};

struct Config {
    ClientType cli_type;
    std::string ip_addr;
	uint64_t port;
    uint64_t cli_socket;
    bool local;
};


class BaseClient {
	public:
	    /*** Variables ***/
        std::unique_ptr<Config> configObj;

        /*** Virtual functions ***/
        // Append entries to the log
        virtual uint64_t append(std::unique_ptr<std::string> entry) = 0;
        // Read from idx in the log
        virtual std::unique_ptr<std::string> read(uint64_t idx) = 0;
        // Get latest committed entry
        virtual std::unique_ptr<std::string> get_tail() = 0;
        // Subscribe to get all log updates after supplied index
        virtual void subscribe(uint64_t idx) = 0;
        // Garbage collect all log entries up to some index
        virtual bool trim(uint64_t idx) = 0;

        /*** Functions with inherited implementations ***/
		// Convert config file to ConfigObject object
		bool readConfigFile(std::string config_file);
		/// Getter for client ID
		uint64_t get_cid();
        
        /*Networking information*/
        std::queue<char* buf> send_pkt;
        std::queue<char* buf> rcv_pkt;

	    // Functions
        void send_packet();
        uint64_t wait_for_append_idx(std::unique_ptr<std::string> entry);
	    void receive_packets();

	protected:
		uint64_t cid;

	private:
	
		/*** Functions ***/
		// Initializes convert client types
		ClientType fromStringToClientType(std::string type);
}

// Create client object
BaseClient createClient(std::string config_file);

