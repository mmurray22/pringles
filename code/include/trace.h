#pragma once

#include <string>
#include <map>
#include <memory>
#include "utils.h"

#include "corfuclient.pb.h"
#include "corfustorage.pb.h"
#include "corfusequencer.pb.h"

#define CORFU_APPEND_PROTO_TYPE 1
#define CORFU_READ_PROTO_TYPE 2
#define CORFU_TRIM_PROTO_TYPE 3
#define CORFU_SEAL_PROTO_TYPE 4
#define CORFU_GETTOKEN_PROTO_TYPE 5

#define CORFU_ACK_PROTO_TYPE 6
#define CORFU_SEALED_PROTO_TYPE 7
#define CORFU_UNWRITTEN_PROTO_TYPE 8
#define CORFU_WRITTEN_PROTO_TYPE 9
#define CORFU_STORE_READ_PROTO_TYPE 10
#define CORFU_STORE_SEAL_PROTO_TYPE 11
#define CORFU_DELETED_PROTO_TYPE 12

#define CORFU_GETTOKEN_REPLY_PROTO_TYPE 13

template <typename T> class Trace {
    public:
        Trace(std::string trace_file);
        ~Trace();
        
        /*String specific serialization/deserialization*/
        std::unique_ptr<std::string> serialize_str_entry(std::string entry, std::string cli_type, std::string api_call, uint64_t idx = 0);       
        
        auto deserialize_str_entry(std::unique_ptr<std::string> entry, std::string cli_type);
        
        // Map of operation to payload value
        // Created at the start of the program and should not change during runtime
        std::map<std::string, T> trace_vals;
};

std::unique_ptr<std::string> corfu_client_serialize_str_entry(std::string entry, uint64_t proto_type, uint64_t client_id, uint64_t thread_id, uint64_t log_idx, uint64_t curr_epoch);
std::unique_ptr<std::string> corfu_storage_serialize_str_entry(std::string entry, uint64_t proto_type, uint64_t highest_addr);
std::unique_ptr<std::string> corfu_sequencer_serialize_str_entry(uint64_t proto_type, uint64_t log_idx);

corfuclient::Payload corfu_client_deserialize_str_entry(std::unique_ptr<std::string> entry);
corfustorage::Payload corfu_storage_deserialize_str_entry(std::unique_ptr<std::string> entry);
corfusequencer::Payload corfu_sequencer_deserialize_str_entry(std::unique_ptr<std::string> entry);