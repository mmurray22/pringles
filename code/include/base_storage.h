#include <cstdint>
#include <mutex> 
#include <map>
#include <string>

class BaseStorage {
    public:
        /** Class Creation **/
        virtual ~BaseStorage() {};

        /** Storage Functions **/
        virtual bool store(uint64_t idx, std::string entry) = 0;
	virtual std::string get(uint64_t idx) = 0;
};
