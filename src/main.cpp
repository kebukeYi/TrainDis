//
// Created by 19327 on 2025/12/31/星期三.
//

#include <iostream>
#include <csignal>
#include "server.h"
#include "config.h"
#include "config_loader.h"


namespace train_set {

    void print_usage(const char *argv0) {
        std::cout << "mini-redis usage:\n"
                  << "  " << argv0 << " [--port <port>] [--bind <ip>] [--config <file>]" << std::endl;
    }

    bool parse_args(int argc, char **argv, ServerConfig &config) {
        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            if (arg == "--config") {
                std::string file_name = argv[++i];
                std::string err;
                auto ok = load_config(file_name, config, err);
                if (!ok) {
                    std::cerr << err << std::endl;
                    return false;
                }
            } else if (arg == "--port") {
                config.port = (uint16_t)std::stoi(argv[++i]);
            } else if (arg == "--bind") {
                config.host = argv[++i];
            } else if (arg == "--help") {
                print_usage(argv[0]);
                return false;
            } else {
                std::cerr << "invalid argument: " << arg << std::endl;
                return false;
            }
        }
        return true;
    }

    static volatile std::sig_atomic_t g_should_stop = 0;

    void handle_signal(int signum) {
        (void) signum;
        g_should_stop = 1;
    }

    int run_server(ServerConfig &config) {
        Server server(config);
        std::signal(SIGINT, handle_signal);
        std::signal(SIGTERM, handle_signal);
        return server.run();
    }
}

int main(int argc, char **argv) {
    train_set::ServerConfig config;
    if (!train_set::parse_args(argc, argv, config)) {
        return -1;
    }

    train_set::run_server(config);
}