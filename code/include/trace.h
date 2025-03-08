#include <string>
#include <map>
#include <memory>
#include "structs.h" 

template <typename T> class Trace {
    public:
        Trace(std::string trace_file);
        ~Trace();
        
        /*String specific serialization/deserialization*/
        std::unique_ptr<std::string> serialize_str_entry(std::string entry, ClientType cli_type, std::string api_call, uint64_t idx = 0);       
        
        auto deserialize_str_entry(std::unique_ptr<std::string> entry, ClientType cli_type);
        
        // Map of operation to payload value
        // Created at the start of the program and should not change during runtime
        std::map<std::string, T> trace_vals;
};

