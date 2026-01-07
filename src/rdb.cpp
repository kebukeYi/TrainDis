//
// Created by 19327 on 2025/12/31/星期三.
//
#include <system_error>
#include <filesystem>
#include <fcntl.h>
#include <csignal>
#include "rdb.h"

namespace train_set {

    static std::string joinPath(const std::string &path, const std::string &file) {
        if (path.empty()) {
            return file;
        }
        if (path.back() == '/') {
            return path + file;
        }
        return path + "/" + file;
    }

    std::string RDBManager::path() {
        return joinPath(config_.dir, config_.file_name);
    }

    bool RDBManager::dump(KVStorage &storage, std::string &err) {
        if (!config_.enabled) {
            return true;
        }
        std::error_code ec;
        std::filesystem::create_directories(config_.dir, ec);
        int fd = ::open(path().c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (fd < 0) {
            err = "open rdb file failed";
            return false;
        }

        auto strs = storage.stringSnapshot();
        auto hash = storage.hashSnapshot();
        auto zset = storage.zSetSnapshot();

        // Header: MRDB\n
        // Strings: STR count\n then per line: klen key vlen value expire_ms\n
        // Hash: HASH count\n then per hash: klen key expire_ms num_fields\n then num_fields lines: flen field vlen value\n
        // ZSet: ZSET count\n then per zset: klen key expire_ms num_items\n then num_items lines: score member_len member\n
        std::string header = "MRDB\n";
        if (write(fd, header.data(), header.size()) < 0) {
            close(fd);
            err = "write rdb hdr failed";
            return false;
        }

        // STRING
        std::string line = std::string("STRING ") + std::to_string(strs.size()) + "\n";
        if (write(fd, line.data(), line.size()) < 0) {
            close(fd);
            err = "write string header failed";
            return false;
        }
        for (auto &item: strs) {
            auto key = item.first;
            auto r = item.second;
            std::string res;
            // 2 kk 2 vv -1
            res.append(
                    std::to_string(key.size()).append(" ").
                            append(key).append(" ").
                            append(std::to_string(r.value.size()).append(" ").
                            append(r.value).append(" ").append(std::to_string(r.expire)).append("\n")));
            if (write(fd, res.data(), res.size()) < 0) {
                close(fd);
                err = "write string data failed";
                return false;
            }
        }

        // HASH
        // HASH 1
        line = std::string("HASH ") + std::to_string(hash.size()) + "\n";
        if (write(fd, line.data(), line.size()) < 0) {
            close(fd);
            err = "write hash header failed";
            return false;
        }
        for (auto &item: hash) {
            auto key = item.first;
            auto r = item.second;
            std::string res;
            // 3 key -1 3
            res.append(
                    std::to_string(key.size()).append(" ").
                            append(key).append(" ").append(std::to_string(r.expire)).
                            append(" ").
                            append(std::to_string(r.field_vals.size())).append("\n"));
            if (write(fd, res.data(), res.size()) < 0) {
                close(fd);
                err = "write hash";
                return false;
            }
            // 2 f2 2 v2
            // 2 f1 2 v2
            // 2 f3 2 v4
            for (auto &field_val: r.field_vals) {
                std::string fv_line;
                fv_line.append(std::to_string(field_val.first.size()).append(" ").
                        append(field_val.first).append(" ").
                        append(std::to_string(field_val.second.size())).append(" ").
                        append(field_val.second).append("\n")
                );
                if (write(fd, fv_line.data(), fv_line.size()) < 0) {
                    close(fd);
                    err = "write hash field_val failed";
                    return false;
                }
            }
        }

        // ZSET section
        // ZSET 1
        line = std::string("ZSET ") + std::to_string(zset.size()) + "\n";
        if (write(fd, line.data(), line.size()) < 0) {
            close(fd);
            err = "write zset header failed";
            return false;
        }
        for (auto &item: zset) {
            auto key = item.key;
            auto zset = item.items;
            std::string res;
            // klen key expire_at_ms  fields \n
            // 3 key -1 2
            res.append(std::to_string(key.size()).append(" ").
                    append(key).append(" ").
                    append(std::to_string(item.expire)).append(" ").
                    append(std::to_string(zset.size())).append("\n"));
            if (write(fd, res.data(), res.size()) < 0) {
                close(fd);
                err = "write zset header failed";
                return false;
            }
            // 1.100000 2 m2
            // 44.000000 2 m1
            for (auto &z: zset) {
                auto score = z.first;
                // 创建引用类型, 避免拷贝;
                auto &member = z.second;
                std::string z_line;
                z_line.append(std::to_string(score).append(" ").
                        append(std::to_string(member.size())).append(" ").
                        append(member).append("\n"));
                if (write(fd, z_line.data(), z_line.size()) < 0) {
                    close(fd);
                    err = "write zset items failed";
                    return false;
                }
            }
        }
        fsync(fd);
        close(fd);
        return true;
    }

    bool RDBManager::load(KVStorage &storage, std::string &err) {
        if (!config_.enabled) {
            return true;
        }
        int fd = ::open(path().c_str(), O_RDONLY);
        if (fd < 0) {
            // 没有rdb文件也可以;
            return true;
        }
        std::string data;
        // data.reserve(1 << 20); // 只保证 capacity ≥ n，不改变 size;
        data.resize(1 << 20); // 把 size 直接改成 n，并 插入 n 个值初始化字符（char{} 即 '\0'）
        std::string file_data;
        while (true) {
            ssize_t r = read(fd, data.data(), data.size());
            if (r < 0) {
                err = "read rdb file failed";
                close(fd);
                return false;
            }
            if (r == 0) {
                break;
            }
            file_data.append(data.data(), (size_t) r);
        }// while read rdb over;

        close(fd);
        size_t file_data_pos = 0;
        auto readline = [&](std::string &out) -> bool {
            size_t end = file_data.find('\n', file_data_pos);
            if (end == std::string::npos) {
                return false;
            }
            out.assign(file_data.c_str() + file_data_pos, end - file_data_pos);
            file_data_pos = end + 1;
            return true;
        };

        std::string line;
        if (!readline(line)) {
            err = "read header failed";
            return false;
        }

        if (line != "MRDB") {
            err = "rdb invalid header";
            return false;
        }

        auto nextTok = [](std::string &line_, size_t &line_pos, std::string &out) -> bool {
            size_t start = line_pos;
            while (start < line_.size() && line_[start] == ' ') {
                start++;
            }
            size_t end = line_.find(' ', start);
            if (end == std::string::npos) {
                out = line_.substr(start);
                line_pos = line_.size();
                return true;
            }
            out = line_.substr(start, end - start);
            line_pos = end + 1;
            return true;
        };

        // STRING section
        if (!readline(line)) {
            err = "read STRING  failed";
            return false;
        }
        if (line.rfind("STRING ", 0) != 0) {
            err = "invalid STRING section header";
            return false;
        }

        int str_count = std::stoi(line.substr(7));
        for (int i = 0; i < str_count; i++) {
            if (!readline(line)) {
                err = "read STRING data failed";
                return false;
            }
            size_t line_pos = 0;
            std::string key_len_s;
            nextTok(line, line_pos, key_len_s);
            int key_len = std::stoi(key_len_s);
            std::string key = line.substr(line_pos, (size_t) key_len);
            line_pos += (size_t )key_len + 1;

            std::string val_len_s;
            nextTok(line, line_pos, val_len_s);
            int val_len = std::stoi(val_len_s);
            std::string val = line.substr(line_pos, (size_t) val_len);
            line_pos += val_len + 1;

            std::string expire_s;
            nextTok(line, line_pos, expire_s);
            int64_t expire = std::stoll(expire_s);
            storage.set(key, val, expire > 0 ? std::optional<int64_t>(expire) : std::nullopt);
        }// string over

        // HASH section
        if (!readline(line)) {
            err = "read HASH section failed";
            return false;
        }
        if (line.rfind("HASH ", 0) != 0) {
            err = "invalid HASH header";
            return false;
        }

        int hash_count = std::stoi(line.substr(5));
        for (int i = 0; i < hash_count; i++) {
            if (!readline(line)) {
                err = "read HASH data failed";
                return false;
            }
            size_t line_pos = 0;
            std::string klen_s;

            nextTok(line, line_pos, klen_s);
            int klen = std::stoi(klen_s);
            std::string key = line.substr(line_pos, (size_t)(klen));
            line_pos += (size_t)(klen) + 1;

            std::string exp_s;
            nextTok(line, line_pos, exp_s);
            int64_t exp = std::stoll(exp_s);

            std::string nfields_s;
            nextTok(line, line_pos, nfields_s);
            int nf = std::stoi(nfields_s);

            bool has_any = false;
            std::vector<std::pair<std::string, std::string>> fvs;
            fvs.reserve(nf);
            for (int j = 0; j < nf; j++) {
                if (!readline(line)) {
                    err = "trunc hash f_v field";
                    return false;
                }
                size_t line_pos = 0;
                std::string flen_s;
                nextTok(line, line_pos, flen_s);
                int flen = std::stoi(flen_s);
                std::string field = line.substr(line_pos, (size_t)(flen));
                line_pos += (size_t)(flen) + 1;

                std::string vlen_s;
                nextTok(line, line_pos, vlen_s);
                int vlen = std::stoi(vlen_s);
                std::string val = line.substr(line_pos, (size_t)(vlen));

                fvs.emplace_back(std::move(field), std::move(val));
                has_any = true;
            }// for over

            if (has_any) {
                for (auto &[field, val]: fvs) {
                    storage.hset(key, field, val);
                }
                if (exp >= 0) {
                    storage.hsetWithExpire(key, exp);
                }
            }
        }// hash over

        // ZSET section
        if (!readline(line)) {
            err = "read ZSET section failed";
            return false;
        }
        if (line.rfind("ZSET ", 0) != 0) {
            err = "read ZSET tag failed";
            return false;
        }

        int zset_count = std::stoi(line.substr(5));
        for (int i = 0; i < zset_count; i++) {
            if (!readline(line)) {
                err = "read ZSET data failed";
                return false;
            }
            size_t line_pos = 0;
            std::string klen_s;
            nextTok(line, line_pos, klen_s);
            int klen = std::stoi(klen_s);
            std::string key = line.substr(line_pos, (size_t)(klen));
            line_pos += (size_t)(klen) + 1;

            std::string exp_s;
            nextTok(line, line_pos, exp_s);
            int64_t exp = std::stoll(exp_s);

            std::string nmems_s;
            nextTok(line, line_pos, nmems_s);
            int nmems = std::stoi(nmems_s);

            for (int j = 0; j < nmems; j++) {
                if (!readline(line)) {
                    err = "read ZSET members failed";
                    return false;
                }
                size_t line_pos = 0;
                std::string score_s;
                nextTok(line, line_pos, score_s);
                double score = std::stod(score_s);

                std::string mlen_s;
                nextTok(line, line_pos, mlen_s);
                int mlen = std::stoi(mlen_s);
                std::string member = line.substr(line_pos, (size_t)(mlen));

                storage.zadd(key, score, member);
            }// zadd over

            if (exp >= 0) {
                storage.zaddWithExpire(key, exp);
            }
        }
        return true;
    }
}
