#include <string>
#include <cstdint>
#include <memory>
#include <queue>
#include "network.h"
//#include "trace.h"

class BaseClient {
	public:
        /*** Virtual functions ***/
	virtual ~BaseClient() {
	}
        
	/** API Functions **/
        // Append entries to the log
        virtual uint64_t append(std::string entry) = 0;
        // Read from idx in the log
        virtual std::string read(uint64_t idx) = 0;
        // Get latest committed entry
        virtual uint64_t getTail() = 0;
        // Subscribe to get all log updates after supplied index
        virtual void subscribe(uint64_t idx) = 0;
        // Garbage collect all log entries up to some index
        virtual bool trim(uint64_t idx) = 0;
};
