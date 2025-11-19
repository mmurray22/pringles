#include "pringles_client.h"
#include <thread>
#include "utils.h"
#include "api.h"
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#define RECEIVE_PORT 3149

LogClient::LogClient(std::string input_file, uint64_t cli_id) {
    YAML::Node config = YAML::LoadFile(input_file);
    net = std::make_shared<Network>(get_threads(config), 
                                    get_ips(config), 
                                    get_port(config), 
                                    get_protocol(config));
    client_id = cli_id;

}

LogClient::~LogClient() {
}

/* Custom function */
uint64_t LogClient::append(std::unique_ptr<std::string> entry) {
    pending_append_entries(id(entry));
    net->add_to_send_queue(entry);
    // wait until id(entry)
    return wait_for_append(entry);
}

/*** Helper functions ***/
uint64_t wait_for_append(std::unique_ptr<std::string> entry) {
    

}
