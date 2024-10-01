#include <thread>
#include "api.h"

LogClient::LogClient(std::string config_file) {
	// Start receive thread
}

template<class Entry>
uint64_t append(Entry log_entry) {
	// Create AppendEntry packet
}

template<class Entry>
Entry read(uint64_t idx) {
	// Create ReadEntry packet
}





