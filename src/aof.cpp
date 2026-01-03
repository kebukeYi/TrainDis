//
// Created by 19327 on 2025/12/31/星期三.
//
#include <string>
#include <vector>
#include <csignal>
#include <filesystem>
#include <fcntl.h>
#include "aof.h"

namespace train_set {
    // [set, kk , val]
    // *3 \r\n $3 \r\n set \r\n $2 \r\n kk \r\n $3 \r\n val \r\n
    std::string toArrayType(std::vector<std::string> &parts) {
        std::string out;
        out.reserve(parts.size() * 16);
        out.append("*").append(std::to_string(parts.size())).append("\r\n");
        for (auto &part: parts) {
            out.append("$").append(std::to_string(part.size())).append("\r\n");
            out.append(part).append("\r\n");
        }
        return out;
    }

    bool writeAllToFd(int fd, char *data, size_t len) {
        size_t offset = 0;
        while (offset < len) {
            auto w = ::write(fd, data + offset, len - offset);
            if (w > 0) {
                offset += w;
                continue;
            }
            if (w < 0 && (errno == EINTR || errno == EAGAIN)) {
                continue;
            }
            return false;
        }
        return true;
    }

    std::string joinPath(std::string &dir, std::string &fileName) {
        if (dir.empty()) {
            return fileName;
        }
        if (dir.back() == '/') {
            return dir.append(fileName);
        }
        return dir.append("/").append(fileName);
    }

    std::string AOFManager::path() {
        return joinPath(config.dir, config.file_name);
    }

    AOFManager::AOFManager() = default;

    AOFManager::~AOFManager() {
        shutdown();
    }

    bool AOFManager::init(AofConfig &conf, std::string &err) {
        config = conf;
        if (!config.enabled) {
            return true;
        }
        std::error_code ec;
        std::filesystem::create_directories(config.dir, ec);
        if (ec) {
            err = "mkdir failed: " + config.dir + ec.message();
            return false;
        }
        fd = ::open(path().c_str(), O_CREAT | O_RDWR | O_APPEND, 0644);
        if (fd < 0) {
            err = "open failed: " + path();
            return false;
        }
#ifdef __linux__
        if (config.file_pre_alloc_size) {
            posix_fallocate(fd, 0, (off_t) config.file_pre_alloc_size);
        }
#endif
        running.store(true);
        write_deque_thread = std::thread(&AOFManager::writeLoop, this);
        return true;
    }

    void AOFManager::shutdown() {
        running.store(false);
        write_cond.notify_all();
        if (write_deque_thread.joinable()) {
            write_deque_thread.join();
        }
        if (fd >= 0) {
            ::fsync(fd);
            ::close(fd);
            fd = -1;
        }
    }


    bool AOFManager::appendCmd(std::vector<std::string> &cmds) {
        if (!config.enabled || fd < 0) {
            return true;
        }
        std::string cmd = toArrayType(cmds);
        bool need_incr = rewriting.load();
        std::string incr_copy;
        if (need_incr) {
            incr_copy = cmd;
        }
        int64_t my_seq = 0;
        {
            std::unique_lock<std::mutex> lock(deque_mutex);
            pending_write += cmd.size();
            my_seq = this->seq.fetch_add(1);
            write_queue.push_back(AofItem{std::move(cmd), my_seq});
        }
        if (need_incr) {
            std::unique_lock<std::mutex> lock(incr_mutex);
            incr_cmd.emplace_back(std::move(incr_copy));
        }
        write_cond.notify_one();
        if (config.mode == AofMode::Always) {
            std::unique_lock<std::mutex> lock(deque_mutex);
            write_commit.wait(lock, [&]() {
                return last_seq >= my_seq || !running;
            });
        }
        return true;
    }

    bool AOFManager::appendCmdRaw(std::string &cmd) {
        std::vector<std::string> parts;
        parts.emplace_back(cmd);
        return appendCmd(parts);
    }

    bool AOFManager::load(KVStorage *storage, std::string &err) {
        if (!config.enabled) {
            return true;
        }
        auto fd_ = ::open(path().c_str(), O_RDONLY);
        if (fd_ < 0) {
            err = "open failed: " + path();
            return false;
        }
        std::string buf;
        buf.resize(1 << 20);
        std::string data;
        while (true) {
            ssize_t r = ::read(fd_, buf.data(), buf.size());
            if (r < 0) {
                err = "read failed: " + path();
                ::close(fd_);
                return false;
            }
            if (r == 0) {
                break;
            }
            data.append(buf.data(), r);
        }
        ::close(fd_);
        size_t pos = 0;
        auto readline = [&](std::string &out) -> bool {
            size_t end = data.find("\r\n", pos);
            if (end == std::string::npos) {
                return false;
            }
            out.assign(data.data() + pos, end - pos);
            pos = end + 2;
            return true;
        };
        while (pos < data.size()) {
            if (data[pos] != '*') {
                err = "bad bulk";
                return false;
            }
            pos++;
            std::string line;
            if (!readline(line)) {
                err = "bad bulk len";
                return false;
            }
            int token_size = std::stoi(line);
            std::vector<std::string> tokens;
            tokens.reserve(token_size);
            for (int i = 0; i < token_size; ++i) {
                if (data[pos] != '$') {
                    err = "bad bulk";
                    return false;
                }
                pos++;
                if (!readline(line)) {
                    err = "bad bulk len";
                    return false;
                }
                int bulk_len = std::stoi(line);
                if (pos + bulk_len + 2 > data.size()) {
                    err = "bad trunc bulk";
                    return false;
                }
                tokens.emplace_back(data.data() + pos, bulk_len);
                pos += bulk_len + 2;
            }
            if (tokens.empty()) {
                continue;
            }
            std::string cmd;
            cmd.reserve(tokens[0].size());
            for (char c: tokens[0]) {
                cmd.push_back((char) ::toupper(c));
            }
            if (cmd == "SET" && tokens.size() == 3) {
                storage->set(tokens[1], tokens[2]);
            } else if (cmd == "DEL" && tokens.size() >= 2) {
                std::vector<std::string> keys(tokens.begin() + 1, tokens.end());
                storage->del(keys);
            } else if (cmd == "ZADD" && tokens.size() >= 4) {
                storage->zadd(tokens[1], std::stod(tokens[2]), tokens[3]);
            } else if (cmd == "HDEL" && tokens.size() >= 3) {
                std::vector<std::string> fields(tokens.begin() + 2, tokens.end());
                storage->hdel(tokens[1], fields);
            } else if (cmd == "HSET" && tokens.size() >= 4) {
                storage->hset(tokens[1], tokens[2], tokens[3]);
            } else if (cmd == "EXPIRE" && tokens.size() == 3) {
                storage->expire(tokens[1], std::stoi(tokens[2]));
            } else {
                //
            }
        }// while read over
        return true;
    }


    bool AOFManager::bgWrite(KVStorage &storage, std::string &err) {
        if (!config.enabled) {
            err = "aod disabled";
            return true;
        }
        bool expected = false;
        if (!rewriting.compare_exchange_strong(expected, true)) {
            err = "rewrite already running";
            return false;
        }
        rewriter_thread = std::thread(&AOFManager::rewriteLoop, this, &storage);
        return true;
    }
}
namespace train_set {
    void AOFManager::writeLoop() {
        std::unique_lock<std::mutex> lock(deque_mutex);
        while (running.load()) {
        }
    }

    void AOFManager::rewriteLoop(KVStorage *storage) {
    }
}
