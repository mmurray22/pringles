#include <thread>
#include "api.h"

LogClient::LogClient() {
	// Start receive thread
	// Get config object
	// Initialize client variables
	isRegistered = false;
}

template<class Entry>
uint64_t append(Entry log_entry) {
	// Create AppendEntry packet
}

template<class Entry>
Entry read(uint64_t idx) {
	// Create ReadEntry packet
}





