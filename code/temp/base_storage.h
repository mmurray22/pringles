#include <cstdint>
#include <mutex> 

class BaseStorage {
    public:
	    BaseStorage();
        create_server();

	protected:
		// Networking info
		std::string ip_addr;
		uint64_t port;
}
