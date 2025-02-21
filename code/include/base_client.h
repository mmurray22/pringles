#include <string>
#include <cstdint>
#include <memory>
#include <queue>
#include "network.h"

/*** Structs, enums, etc. ***/
enum ClientType {
	SIMPLE,
	RING,
	CORFU,
	NONE
};

class BaseClient {
	public:
        /*** Virtual functions ***/

        /** Class Creation **/
        // BaseClient constructor
        virtual BaseClient(std::string type, uint64_t cid) = 0;
        // BaseClient destructor
        virtual ~BaseClient() = 0;
        
        /** API Functions **/
        // Append entries to the log
        virtual uint64_t append(std::unique_ptr<std::string> entry) = 0;
        // Read from idx in the log
        virtual std::unique_ptr<std::string> read(uint64_t idx) = 0;
        // Get latest committed entry
        virtual std::unique_ptr<std::string> getTail() = 0;
        // Subscribe to get all log updates after supplied index
        virtual void subscribe(uint64_t idx) = 0;
        // Garbage collect all log entries up to some index
        virtual bool trim(uint64_t idx) = 0;

	private:
	    /*** Variables ***/
        uint64_t cid;
        std::unique_ptr<Network> net;
        std::unique_ptr<Trace<T>> trace;
        ClientType cli_type;
        bool local;
		
        /*** Helper function: converts string type to client type ***/
        ClientType BaseClient::fromStringToClientType(std::string type) {
        	ClientType cliType = ClientType::NONE;
        	if (type == "simple") {
        		cliType = ClientType::SIMPLE;
        	} else if (type == "ring") {
        		cliType = ClientType::RING;
        	} else if (type == "corfu") {
        		cliType = ClientType::CORFU;
        	} else {
                cliType = ClientType::NONE;
            }
        	return cliType;
        }
}
