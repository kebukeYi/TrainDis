//
// Created by 19327 on 2025/12/31/星期三.
//
#pragma once

#include <string>

namespace train_set {

    enum class AofMode {
        None,
        EverySec,
        Always
    };
    struct AofConfig {
        bool enabled = false;
        AofMode mode = AofMode::None;
        std::string dir = "./data";
        std::string file_name = "appendonly.aof";
        size_t file_pre_alloc_size = 1024 * 1024 * 1024;
        size_t max_write_buffer_size = 1024 * 1024 * 1024;
        int consume_aof_queue_us = 1500;
        int file_sync_interval_ms = 1000; // everySec ms;
        bool file_use_sync_range = false;
        size_t file_use_sync_range_size = 512 * 1024;
        bool file_advise_page_cache_after_sync = false;
    };

    struct RdbConfig {
        bool enabled = false;
        std::string dir = "./data";
        std::string file_name = "dump.rdb";
    };

    struct ReplicateConfig {
        bool enabled = false;
        std::string master_host;
        uint16_t master_port = 0;
    };

    struct ServerConfig {
        std::string host = "127.0.0.1";
        uint16_t port = 6379;
        AofConfig aof_conf;
        RdbConfig rdb_conf;
        ReplicateConfig rep_conf;
    };
}