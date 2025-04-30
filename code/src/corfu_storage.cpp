#include "corfu_storage.h"

const uint64_t MAX_WAIT_TIME = 100;

#define WRITE 0
#define READ 1
#define DELETE 2
#define SEAL 3


void CorfuStorage::write() {
    
}

void CorfuStorage::read() {

}

void CorfuStorage::storage_delete() {

}

void CorfuStorage::seal() {

}

void CorfuStorage::server(std::shared_ptr<Network> net) {
    spdlog::info("Simple Net Server!");
    std::unique_ptr<std::string> rcv_str = NULL;
    uint64_t wait_time = 10;
    while (true) {
        if (wait_time >= MAX_WAIT_TIME) {
            spdlog::debug("!!!!!!!!!!!!!!No more packets to receive.");
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(wait_time));
        wait_time += 10;
        rcv_str = net->read_from_recv_queue();
        if (rcv_str == NULL) {
            continue;
        }
        // deserialize string, send to other functions
        msg = Trace::deserialize_str_entry(recv_str, 2);
        if (msg->type == WRITE) {
            write();
        } else if (msg->type == READ) {
            read();
        } else if (msg->type == DELETE) {
            storage_delete();
        } else if (msg->type == SEAL) {
            seal();
        }
        wait_time = 10;
    }
}