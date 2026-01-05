//
// Created by 19327 on 2026/01/04/星期日.
//

#pragma once

#include <thread>
#include <string>
#include "config.h"

namespace train_set {
    class ReplicateClient {
    private:
        const ServerConfig &cof;
        std::thread th;
        bool running = false;
        int64_t last_replicate_offset = 0;
    public:
        explicit ReplicateClient(ServerConfig &config) : cof(config) {};

        ~ReplicateClient();

        int start();

        int stop();

    private:
        void run_replicate();
    };
}