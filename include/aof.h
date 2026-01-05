//
// Created by 19327 on 2025/12/30/星期二.
//

#pragma once
#include <vector>
#include <deque>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <string>

#include "config.h"
#include "kv.h"

namespace train_set {

    class AOFManager {
    private:
        int fd = -1;
        AofConfig config;
        std::atomic<bool> running{false};
        struct AofItem{
            std::string data;
            int64_t seq;
        };
        std::thread write_deque_thread;
        std::mutex deque_mutex;
        std::condition_variable write_cond;
        std::condition_variable write_commit;

        std::deque<AofItem> write_queue;
        size_t pending_write = 0;
        std::atomic<int64_t> seq{0};
        int64_t last_seq = -1;
        std::chrono::steady_clock::time_point last_write_time{std::chrono::steady_clock::now()};

        std::atomic<bool> rewriting{false};
        std::thread rewriter_thread;
        std::mutex incr_mutex;
        std::vector<std::string> incr_cmd;

        std::atomic<bool> pause_write{false};
        std::mutex pause_mutex;
        std::condition_variable pause_cond;
        bool pause_write_flag = false;
        void writeLoop();
        void rewriteLoop(KVStorage *storage);
    public:
        AOFManager();

        ~AOFManager();
        bool init(AofConfig& config, std::string & errM);
        bool load(KVStorage &storage, std::string & errM);
        bool appendCmd(std::vector<std::string>& cmds);
        bool appendCmdRaw(std::string &cmd);
        bool isEnabled(){return config.enabled;};
        void shutdown();
        bool bgWrite(KVStorage &storage, std::string &errM);
        AofMode mode(){
            return config.mode;
        }
        std::string path();

    };

    std::string toArrayType(std::vector<std::string> &parts);
}