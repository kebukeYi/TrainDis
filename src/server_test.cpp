//
// Created by 19327 on 2026/01/06/星期二
//

#include <string>
#include <iostream>
#include "server.h"
#include "config.h"

int main_server_test(int argc, char **argv) {
    train_set::ServerConfig config;
    config.host = "127.0.0.1";
    config.port = 6380; // Use a different port to avoid conflicts

    std::cout << "Server config - Host: " << config.host << ", Port: " << config.port << std::endl;

    // Test AOF config
    config.aof_conf.enabled = true;
    config.aof_conf.dir = "../data_simple";
    config.aof_conf.file_name = "appendonly.aof";
    config.aof_conf.mode = train_set::AofMode::EverySec;
    std::cout << "AOF enabled: " << (config.aof_conf.enabled ? "true" : "false")
              << ", Mode: " << (config.aof_conf.mode == train_set::AofMode::EverySec ? "EverySec" :
                                config.aof_conf.mode == train_set::AofMode::Always ? "Always" : "None") << std::endl;

    // Test RDB config
    config.rdb_conf.enabled = true;
    config.rdb_conf.dir = "../data_simple";
    config.rdb_conf.file_name = "dump.rdb";
    std::cout << "RDB enabled: " << (config.rdb_conf.enabled ? "true" : "false")
              << ", Path: " << config.rdb_conf.dir << "/" << config.rdb_conf.file_name << std::endl;

    // Test replication config
    config.rep_conf.enabled = false;
    config.rep_conf.master_host = "0.0.0.0";
    config.rep_conf.master_port = 0;
    std::cout << "Replication enabled: " << (config.rep_conf.enabled ? "true" : "false")
              << ", Master: " << config.rep_conf.master_host << ":" << config.rep_conf.master_port << std::endl;

    // Create server instance (but don't run it to avoid port conflicts in test)
    train_set::Server server(config);
    std::cout << "Server created successfully" << std::endl;
    server.run();
    return 0;
}
// 测试 6
int main6() {
    return main_server_test(0, nullptr);
}
