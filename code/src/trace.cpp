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

    (void)entry;
    (void)cli_type;
    (void)api_call;
    (void)idx;
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

    (void)entry;
    (void)cli_type;
    return output;
}

// No need to call this TemporaryFunction() function,
// it's just to avoid link error.
void TemporaryFunction ()
{
    Trace<std::string> TempObj("default.yaml");
    (void)TempObj;
}

std::unique_ptr<std::string> corfu_client_serialize_str_entry(std::string entry, uint64_t proto_type, uint64_t client_id, uint64_t log_idx, uint64_t curr_epoch) {
    std::unique_ptr<std::string> output = std::make_unique<std::string>();

    corfuclient::Payload corfu_payload;
    corfu_payload.set_packet_type(proto_type);
    corfu_payload.set_clientid(std::to_string(client_id));

    if (proto_type == CORFU_APPEND_PROTO_TYPE) { // append
        corfuclient::Append* append_packet = corfu_payload.mutable_append();
        append_packet->set_idx(log_idx);
        append_packet->set_entry(entry);
        append_packet->set_currepoch(curr_epoch);

    } else if (proto_type == CORFU_READ_PROTO_TYPE) { // read
        corfuclient::Read* read_packet = corfu_payload.mutable_read();
        read_packet->set_idx(log_idx);
        read_packet->set_currepoch(curr_epoch);

    } else if (proto_type == CORFU_TRIM_PROTO_TYPE) { // trim
        corfuclient::Trim* trim_packet = corfu_payload.mutable_trim();
        trim_packet->set_idx(log_idx);
        
    } else if (proto_type == CORFU_SEAL_PROTO_TYPE) { // seal
        corfuclient::Seal* seal_packet = corfu_payload.mutable_seal();
        seal_packet->set_currepoch(curr_epoch);

    } else if (proto_type == CORFU_GETTOKEN_PROTO_TYPE) { // get token
        corfuclient::GetToken* token_packet = corfu_payload.mutable_token_req();
        token_packet->set_reqtoken(true);
    }

    corfu_payload.SerializeToString(output.get());

    return output;
}

std::unique_ptr<std::string> corfu_storage_serialize_str_entry(std::string entry, uint64_t proto_type, uint64_t highest_addr) {
    std::unique_ptr<std::string> output = std::make_unique<std::string>();

    corfustorage::Payload corfu_payload;
    corfu_payload.set_packet_type(proto_type);

    if (proto_type == CORFU_ACK_PROTO_TYPE) { // ack
        corfustorage::Ack* ack_packet = corfu_payload.mutable_ack();
        ack_packet->set_ack_code(true);

    } else if (proto_type == CORFU_SEALED_PROTO_TYPE) { // errSealed
        corfustorage::errSealed* err_sealed_packet = corfu_payload.mutable_err_sealed();
        err_sealed_packet->set_err_code(true);

    } else if (proto_type == CORFU_UNWRITTEN_PROTO_TYPE) { // errUnwritten
        corfustorage::errUnwritten* err_unwritten_packet = corfu_payload.mutable_err_unwritten();
        err_unwritten_packet->set_err_code(true);

    } else if (proto_type == CORFU_WRITTEN_PROTO_TYPE) { // errWritten
        corfustorage::errWritten* err_written_packet = corfu_payload.mutable_err_written();
        err_written_packet->set_err_code(true);
        err_written_packet->set_content(entry);
        
    } else if (proto_type == CORFU_STORE_READ_PROTO_TYPE) { // read
        corfustorage::Read* return_read_packet = corfu_payload.mutable_read();
        return_read_packet->set_content(entry);

    } else if (proto_type == CORFU_STORE_SEAL_PROTO_TYPE) { // seal
        corfustorage::Seal* return_seal_packet = corfu_payload.mutable_seal();
        return_seal_packet->set_sealed(true);
        return_seal_packet->set_highaddr(highest_addr);

    } else if (proto_type == CORFU_DELETED_PROTO_TYPE) { // errDeleted
        corfustorage::errDeleted* err_deleted_packet = corfu_payload.mutable_err_deleted();
        err_deleted_packet->set_err_code(true);
    }

    corfu_payload.SerializeToString(output.get());

    return output;
}

std::unique_ptr<std::string> corfu_sequencer_serialize_str_entry(uint64_t proto_type, uint64_t log_idx) {
    std::unique_ptr<std::string> output = std::make_unique<std::string>();

    corfusequencer::Payload corfu_payload;
    corfu_payload.set_packet_type(proto_type);

    if (proto_type == CORFU_GETTOKEN_REPLY_PROTO_TYPE) {
        corfusequencer::SendToken* token_packet = corfu_payload.mutable_send_token();
        token_packet->set_token(log_idx);
    }

    corfu_payload.SerializeToString(output.get());

    return output;
}

corfuclient::Payload corfu_client_deserialize_str_entry(std::unique_ptr<std::string> entry) {
    corfuclient::Payload corfu_payload;
    corfu_payload.ParseFromString(*(entry.get()));
    return corfu_payload;
}

corfustorage::Payload corfu_storage_deserialize_str_entry(std::unique_ptr<std::string> entry) {
    corfustorage::Payload corfu_payload;
    corfu_payload.ParseFromString(*(entry.get()));
    return corfu_payload;
}

corfusequencer::Payload corfu_sequencer_deserialize_str_entry(std::unique_ptr<std::string> entry) {
    corfusequencer::Payload corfu_payload;
    corfu_payload.ParseFromString(*(entry.get()));
    return corfu_payload;
}
