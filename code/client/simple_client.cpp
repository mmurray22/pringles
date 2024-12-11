// Simple client code
//
//


SimpleClient::SimpleClient(YAML::Node config) {
    // 
    configObj = std::make_unqiue<Config>();
    if (config["local"].as<uint8_t>() == 1) {
        config->local = true;
    } else {
        config->local = false;
    }
}

uint64_t SimpleClient::append(std::unique_ptr<std::string> entry) {
    if (config->local) {
        const std::lock_guard<std::mutex> lock(remote_log_lock);
        remote_log.push(entry);
        return remote_log.size()-1;
    }
    send_packet(entry);
    return wait_for_idx();
}

std::unique_ptr<std::string> SimpleClient::read(uint64_t idx) {
    if (config->local) {
        const std::lock_guard<std::mutex> lock(remote_log_lock);
        if (idx > remote_log.size()) 
            return NULL;
        return remote_log[idx];
    }

}

std::unique_ptr<std::string> create_entry(std::string content) {
    AppendEntry entry;
    entry.set_allocated_entry(content);
    std::unique_ptr<std::string> output;
    entry.SerializeToString(output);
    return output;
}

