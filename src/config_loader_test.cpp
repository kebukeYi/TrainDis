//
// Created by 19327 on 2026/01/06/星期二
//

#include <string>
#include <iostream>
#include <fstream>
#include "config_loader.h"
#include "config.h"

int main_config_loader_test(int argc, char **argv) {
    // Create a temporary config file for testing
    std::string config_content = R"(
port=6381
bind_address=127.0.0.1
aof.enabled=yes
aof.mode=everysec
aof.dir=../data_simple
aof.filename=test.aof
aof.batch_bytes=262144
aof.batch_wait_us=1500
aof.pre_alloc_bytes=15000
aof.sync_interval_ms=1500
aof.use_sync_file_range=yes
aof.sfr_min_bytes=15000
aof.fadvise_dontneed_after_sync=yes
rdb.enabled=yes
rdb.dir=../data_simple
rdb.filename=test.rdb
replicate.enabled=no
replicate.master_host=0.0.0.0
replicate.master_port=0
)";

    std::string config_file_path = "../conf/test_config.conf";
    std::ofstream config_file(config_file_path);
    config_file << config_content;
    config_file.close();

    train_set::ServerConfig config;
    std::string err;

    if (train_set::load_config(config_file_path, config, err)) {
        std::cout << "Config loaded successfully!" << std::endl;
        std::cout << "Port: " << config.port << std::endl;
        std::cout << "Host: " << config.host << std::endl;

        std::cout << "AOF Enabled: " << (config.aof_conf.enabled ? "yes" : "no") << std::endl;
        std::string aof_mode_str =
                config.aof_conf.mode == train_set::AofMode::None ? "None" :
                config.aof_conf.mode == train_set::AofMode::EverySec ? "EverySec" : "Always";
        std::cout << "AOF Mode: " << aof_mode_str << std::endl;
        std::cout << "AOF Dir: " << config.aof_conf.dir << std::endl;
        std::cout << "AOF Filename: " << config.aof_conf.file_name << std::endl;
        std::cout << "AOF Batch Bytes: " << config.aof_conf.max_write_buffer_size << std::endl;
        std::cout << "AOF Batch Wait Us: " << config.aof_conf.consume_aof_queue_us << std::endl;
        std::cout << "AOF Pre Alloc Bytes: " << config.aof_conf.file_pre_alloc_size << std::endl;
        std::cout << "AOF Sync Interval Ms: " << config.aof_conf.file_sync_interval_ms << std::endl;
        std::cout << "AOF Use Sync File Range: " << (config.aof_conf.file_use_sync_range ? "yes" : "no") << std::endl;
        std::cout << "AOF SFR Min Bytes: " << config.aof_conf.file_use_sync_range_size << std::endl;
        std::cout << "AOF Fadvise Dontneed After Sync: " << (config.aof_conf.file_advise_page_cache_after_sync ? "yes" : "no") << std::endl;

        std::cout << "RDB Enabled: " << (config.rdb_conf.enabled ? "yes" : "no") << std::endl;
        std::cout << "RDB Dir: " << config.rdb_conf.dir << std::endl;
        std::cout << "RDB Filename: " << config.rdb_conf.file_name << std::endl;

        std::cout << "Replication Enabled: " << (config.rep_conf.enabled ? "yes" : "no") << std::endl;
        std::cout << "Master Host: " << config.rep_conf.master_host << std::endl;
        std::cout << "Master Port: " << config.rep_conf.master_port << std::endl;
    } else {
        std::cout << "Failed to load config: " << err << std::endl;
    }

    // Clean up the temporary config file
    std::remove(config_file_path.c_str());

    return 0;
}

// 测试 2
int main2(int argc, char **argv) {
    return main_config_loader_test(0, nullptr);
}
