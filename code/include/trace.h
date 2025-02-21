#include <string>
#include <map>
#include <memory>

template <typename T> class Trace {
    public:
        Trace(std::string trace_file);
        ~Trace();
        
        /*String specific serialization/deserialization*/
        std::unique_ptr<std::string> serialize_str_entry(std::string entry, uint64_t proto_type);       
        std::string deserialize_str_entry(std::unique_ptr<std::string> entry, uint64_t proto_type);      
        
        // Map of operation to payload value
        // Created at the start of the program and should not change during runtime
        std::map<std::string, T> trace_vals;
};

