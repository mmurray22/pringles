#include <fstream>
#include "trace.h"
#include "spdlog/spdlog.h"
#include "ringclient.pb.h"

/*
 * Reads in a txt file trace of the format "operation: payload"
 */
template <typename T>
Trace<T>::Trace(std::string filename) {
    // read in text file line by line and parse
    std::ifstream txt(filename, std::ios::in);
    std::string op;
    T val;
    while (txt >> op >> val) {
        spdlog::debug("Operation: {}, Val: {}", op, val);
        trace_vals.emplace(op, val);
    }
}

template <typename T>
Trace<T>::~Trace() {
}

template <typename T>
std::unique_ptr<std::string> Trace<T>::serialize_str_entry(std::string entry, uint64_t proto_type) {
    std::unique_ptr<std::string> output = NULL;
    if (proto_type == 1) { // Ringlog
        ringclient::LogEntry ringEntry;
        ringEntry.set_allocated_entry(&entry);
        ringEntry.SerializeToString(output.get());
    }
    return output;
}

template <typename T>
std::string Trace<T>::deserialize_str_entry(std::unique_ptr<std::string> entry, uint64_t proto_type) {
    std::string output = "";
    if (proto_type == 1) { // Ringlog
        ringclient::LogEntry ringEntry;
        ringEntry.ParseFromString(*(entry.get()));
        output = ringEntry.entry();
    }
    return output;
}

// No need to call this TemporaryFunction() function,
// it's just to avoid link error.
void TemporaryFunction ()
{
    Trace<std::string> TempObj("default.yaml");
    (void)TempObj;
}
