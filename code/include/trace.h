#include <string>
#include <map>
#include <memory>

template <typename T> class Trace {
    public:
        Trace(std::string trace_file);
        ~Trace();
        
        /*String specific serialization/deserialization*/
        std::unique_ptr<std::string> serialize_str_entry(std::string entry, uint64_t proto_type);         
        std::unique_ptr<std::string> corfu_client_serialize_str_entry(std::string entry, uint64_t proto_type, uint64_t client_id, uint64_t log_idx, uint64_t curr_epoch);
        std::unique_ptr<std::string> corfu_storage_serialize_str_entry(std::string entry, uint64_t proto_type, uint64_t highest_addr);
        std::unique_ptr<std::string> corfu_sequencer_serialize_str_entry(uint64_t proto_type, uint64_t log_idx);

        std::string deserialize_str_entry(std::unique_ptr<std::string> entry, uint64_t proto_type);
        std::string corfu_client_deserialize_str_entry(std::unique_ptr<std::string> entry);
        std::string corfu_storage_deserialize_str_entry(std::unique_ptr<std::string> entry);
        std::string corfu_sequencer_deserialize_str_entry(std::unique_ptr<std::string> entry);

        // Map of operation to payload value
        // Created at the start of the program and should not change during runtime
        std::map<std::string, T> trace_vals;
};

