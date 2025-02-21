#include "utils.h"
#include "spdlog/spdlog.h"

void set_spdlog_level(uint64_t log_level) {
    if (log_level == 1) { // Prints all log levels except trace
        spdlog::set_level(spdlog::level::debug);
        spdlog::debug("Log level set to: Debug");
    } else if (log_level == 2) { // Prints error and critical
        spdlog::set_level(spdlog::level::err);
        spdlog::error("Log level set to: Error");
    } else if (log_level == 3) { // Prints warn, error, and critical
        spdlog::set_level(spdlog::level::warn);
        spdlog::warn("Log level set to: Warn");
    } else if (log_level == 4) { // Prints all but debug and trace
        spdlog::set_level(spdlog::level::info);
        spdlog::debug("Log level set to: Info");
    } else if (log_level == 5) { // Prints all log levels
        spdlog::set_level(spdlog::level::trace);
        spdlog::debug("Log level set to: Trace");
    } else { // Prints only critical
        spdlog::set_level(spdlog::level::critical);
        spdlog::critical("Log level set to: Critical");
    }
}

/* YAML processing functions */
uint64_t get_threads(YAML::Node config) {
    return config["threads"].as<uint64_t>();
}

std::vector<std::string> get_ips(YAML::Node config) {
    std::vector<std::string> ret;
    for (auto ip_list : config["ip_list"]) {
        spdlog::debug("What value? ip: {}", ip_list["ip"].as<std::string>());
        ret.emplace_back(ip_list["ip"].as<std::string>());
    }
    return ret;
}

std::string get_send_port(YAML::Node config) {
    spdlog::debug("Port: {}", config["send_port"].as<std::string>());
    return config["send_port"].as<std::string>();
}

std::string get_recv_port(YAML::Node config) {
    spdlog::debug("Port: {}", config["recv_port"].as<std::string>());
    return config["recv_port"].as<std::string>();
}


uint64_t get_protocol(YAML::Node config) {
    spdlog::debug("Protocol level: {}", config["protocol"].as<uint64_t>());
    return config["protocol"].as<uint64_t>();
}

uint64_t get_log_level(YAML::Node config) {
    spdlog::debug("Whyyyyy log level {}", config["loglevel"].as<uint64_t>());
    return config["loglevel"].as<uint64_t>();
}

std::string get_trace_file(YAML::Node config) {
    spdlog::debug("Port: {}", config["trace_file"].as<std::string>());
    return config["trace_file"].as<std::string>();
}
