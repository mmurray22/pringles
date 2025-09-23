#include <fstream>
#include "trace.h"
#include "spdlog/spdlog.h"
#include "ringclient.pb.h"
#include "structs.h"
// #include "simple_client.h"

/*
 * Reads in a txt file trace of the format "operation: payload"
 */
template <typename T>
Trace<T>::Trace(std::string filename) {
    // read in text file line by line and parse
    std::ifstream txt(filename, std::ios::in);
    spdlog::debug("Filename: {}", filename); // TODO: ADD DEBUGGING
    std::string op;
    T val;
    while (txt >> op >> val) {
        spdlog::debug("Operation: {}, Val: {}", op, val);
        trace_vals.emplace(val, op);
    }
    spdlog::debug("Size of map: {}", trace_vals.size());
}

template <typename T>
Trace<T>::~Trace() {
}

template <typename T>
std::unique_ptr<std::string> Trace<T>::serialize_str_entry(std::string entry, std::string cli_type, std::string api_call, uint64_t idx) {
    std::unique_ptr<std::string> output = NULL;
    /*if (cli_type == ClientType::SIMPLE) {
        simpleclient::Payload p;
        if (api_call == "append") {
            p.set_packet_type(api_call);
            AppendEntry app;
            app.set_allocated_entry(entry);
            app.set_idx(idx);
            p.set_allocated_append(app);
            p.SerializeToString(output.get());
        }
    } else if (cli_type == ClientType::RING) { // Ringlog
        ringclient::LogEntry ringEntry;
        ringEntry.set_allocated_entry(&entry);
        ringEntry.SerializeToString(output.get());
    }*/
    return output;
}

template <typename T>
auto Trace<T>::deserialize_str_entry(std::unique_ptr<std::string> entry, std::string cli_type) {
    std::string output = "";
    /*if (cli_type == ClientType::SIMPLE) { 
        simpleclient::Payload p;
        p.ParseFromString(*(entry.get()));
        return p;
    }
    else if (cli_type == ClientType::RING) {
        ringclient::LogEntry ringEntry;
        ringEntry.ParseFromString(*(entry.get()));
        return ringEntry;
    }*/
    return output;
}

// No need to call this TemporaryFunction() function,
// it's just to avoid link error.
void TemporaryFunction ()
{
    Trace<std::string> TempObj("default.yaml");
    (void)TempObj;
}
