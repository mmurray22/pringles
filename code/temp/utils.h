#include <mutex>
#include <string>

class NumGenerator {
	/*** Functions ***/
	
	// Instantiate class
	NumGenerator(uint64_t mbit);

	// Consistent Hashing
	std::string generateConsistentHashID();
	std::string getLargerConsistentHashIDs(std::string hash_one, std::string hash_two);

	// Client ID
	uint64_t generateClientID();

	// Nonce
	uint64_t generateNonce();

	/*** Variables ***/
	uint64_t m;
	uint64_t latest_client_id;
	std::mutex cid_lock;
}
