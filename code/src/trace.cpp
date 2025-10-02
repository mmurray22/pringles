#include <fstream>
#include "trace.h"
#include "spdlog/spdlog.h"
#include "ringclient.pb.h"

// CTODO: figure out best place to move these so i dont have to repeat them in individual files
#define CORFU_APPEND_PROTO_TYPE 1
#define CORFU_READ_PROTO_TYPE 2
#define CORFU_TRIM_PROTO_TYPE 3
#define CORFU_FILL_PROTO_TYPE 4
#define CORFU_SEAL_PROTO_TYPE 5
#define CORFU_GETTOKEN_PROTO_TYPE 6

#define CORFU_ACK_PROTO_TYPE 7
#define CORFU_SEALED_PROTO_TYPE 8
#define CORFU_UNWRITTEN_PROTO_TYPE 9
#define CORFU_WRITTEN_PROTO_TYPE 10
#define CORFU_STORE_READ_PROTO_TYPE 11
#define CORFU_STORE_SEAL_PROTO_TYPE 12
#define CORFU_DELETED_PROTO_TYPE 13

#define CORFU_GETTOKEN_REPLY_PROTO_TYPE 14

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
        trace_vals.emplace(val, op);
    }
    spdlog::debug("Size of map: {}", trace_vals.size());
}

template <typename T>
Trace<T>::~Trace() {
}

// CTODO: can copy this function for different corfu packets, private corfu helper func
template <typename T>
std::unique_ptr<std::string> Trace<T>::serialize_str_entry(std::string entry, uint64_t proto_type) {
    std::unique_ptr<std::string> output = NULL;
    if (proto_type == 1) { // Ringlog
        ringclient::LogEntry ringEntry;
        ringEntry.set_allocated_entry(&entry);
        ringEntry.SerializeToString(output.get());
    } else if (proto_type == 2) { // Corfu
        // 1. Initialize packet obj
        corfuclient::Payload corfuEntry;
        // 2. Fill in fields
        corfuEntry.set_packet_type(proto_type); // pretend int type
        corfuclient::Append appendPkt;
        appendPkt.set_allocated_entry(&entry);
        corfuEntry.set_allocated_append(&appendPkt); // check
        // 3. Serialize and get pointer
        corfuEntry.SerializeToString(output.get());

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
    } else if (proto_type == 2) { // Corfu
        corfuclient::Payload corfuEntry;
        corfuEntry.ParseFromString(*(entry.get())); // TODO: fix deserialization
        output = corfuEntry.append().entry();
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


std::unique_ptr<std::string> corfu_client_serialize_str_entry(std::string entry, uint64_t proto_type, uint64_t client_id, uint64_t log_idx, uint64_t curr_epoch) {
    std::unique_ptr<std::string> output = NULL;

    corfuclient::Payload corfu_payload;
    corfu_payload.set_packet_type(proto_type);
    corfu_payload.set_clientID(client_id);

    if (proto_type == CORFU_APPEND_PROTO_TYPE) { // append
        corfuclient::Append append_packet;

        append_packet.set_idx(log_idx);
        append_packet.set_allocated_entry(&entry);
        append_packet.set_currEpoch(curr_epoch);

        corfu_payload.set_allocated_append(&append_packet);
    } else if (proto_type == CORFU_READ_PROTO_TYPE) { // read
        corfuclient::Read read_packet;

        read_packet.set_idx(log_idx);
        read_packet.set_currEpoch(curr_epoch);

        corfu_payload.set_allocated_read(&read_packet);
    } else if (proto_type == CORFU_TRIM_PROTO_TYPE) { // trim
        corfuclient::Trim trim_packet;

        trim_packet.set_idx(log_idx);
        
        corfu_payload.set_allocated_trim(&trim_packet);
    } else if (proto_type == CORFU_FILL_PROTO_TYPE) { // fill
        corfuclient::Fill fill_packet;

        fill_packet.set_currEpoch(curr_epoch);
        fill_packet.set_junk(true);
        fill_packet.set_idx(log_idx);

        corfu_payload.set_allocated_fill(&fill_packet);
    } else if (proto_type == CORFU_SEAL_PROTO_TYPE) { // seal
        corfuclient::Seal seal_packet;

        seal_packet.set_currEpoch(curr_epoch);

        corfu_payload.set_allocated_seal(&seal_packet);
    } else if (proto_type == CORFU_GETTOKEN_PROTO_TYPE) { // get token
        corfuclient::GetToken token_packet;

        token_packet.set_reqToken(true);

        corfu_payload.set_allocated_token_req(&token_packet);
    }

    corfu_payload.SerializeToString(output.get());

    return output;
}

std::unique_ptr<std::string> corfu_storage_serialize_str_entry(std::string entry, uint64_t proto_type, uint64_t highest_addr) {
    std::unique_ptr<std::string> output = NULL;

    corfustorage::Payload corfu_payload;

    corfu_payload.set_packet_type(proto_type);

    if (proto_type == CORFU_ACK_PROTO_TYPE) { // ack
        corfustorage::Ack ack_packet;

        ack_packet.set_ack_code(true);

        corfu_payload.set_allocated_ack(&ack_packet);
    } else if (proto_type == CORFU_SEALED_PROTO_TYPE) { // errSealed
        corfustorage::errSealed err_sealed_packet;

        err_sealed_packet.set_err_code(true);

        corfu_payload.set_allocated_err_sealed(&err_sealed_packet);
    } else if (proto_type == CORFU_UNWRITTEN_PROTO_TYPE) { // errUnwritten
        corfustorage::errUnwritten err_unwritten_packet;

        err_unwritten_packet.set_err_code(true);

        corfu_payload.set_allocated_err_unwritten(&err_unwritten_packet);
    } else if (proto_type == CORFU_WRITTEN_PROTO_TYPE) { // errWritten
        corfustorage::errWritten err_written_packet;

        err_written_packet.set_err_code(true);
        err_written_packet.set_allocated_content(&entry);
        
        corfu_payload.set_allocated_err_written(&err_written_packet);
    } else if (proto_type == CORFU_STORE_READ_PROTO_TYPE) { // read
        corfustorage::Read return_read_packet;

        return_read_packet.set_allocated_content(&entry);

        corfu_payload.set_allocated_read(&return_read_packet);
    } else if (proto_type == CORFU_STORE_SEAL_PROTO_TYPE) { // seal
        corfustorage::Seal return_seal_packet;

        // note: i dont think we actually need to return a boolean here since we already
        // know it's a seal packet based on its proto_type, but this is part of the bigger
        // issue of whether or not there is a better way to frame all of this since a lot of
        // this is just a big repeat
        return_seal_packet.set_sealed(true);
        return_seal_packet.set_highaddr(highest_addr);

        corfu_payload.set_allocated_seal(&return_seal_packet);
    } else if (proto_type == CORFU_DELETED_PROTO_TYPE) {
        corfustorage::errDeleted err_deleted_packet;

        err_deleted_packet.set_err_code(true);

        corfu_payload.set_allocated_err_deleted(&err_deleted_packet);
    }

    corfu_payload.SerializeToString(output.get());

    return output;
}

std::unique_ptr<std::string> corfu_sequencer_serialize_str_entry(uint64_t proto_type, uint64_t log_idx) {
    std::unique_ptr<std::string> output = NULL;

    corfusequencer::Payload corfu_payload;

    corfu_payload.set_packet_type(proto_type);

    if (proto_type == CORFU_GETTOKEN_REPLY_PROTO_TYPE) {
        corfusequencer::SendToken token_packet;

        token_packet.set_token(log_idx);

        corfu_payload.set_allocated_send_token(&token_packet);
    }

    corfu_payload.SerializeToString(output.get());

    return output;
}