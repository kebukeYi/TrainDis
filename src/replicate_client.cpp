//
// Created by 19327 on 2026/01/05/星期一.
//
#include "replicate_client.h"
#include "config.h"
#include "aof.h"
#include "resp.h"
#include "rdb.h"
#include "kv.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>

namespace train_set {
    ReplicateClient::ReplicateClient(ServerConfig &config, KVStorage &storage) :
            cof(config), storage(storage) {
    }

    ReplicateClient::~ReplicateClient() {
        stop();
    }

    void ReplicateClient::start() {
        if (!cof.rep_conf.enabled) {
            return;
        }
        running = true;
        th = std::thread([this]() {
            run_replicate();
        });
    }


    void ReplicateClient::stop() {
        if (th.joinable()) {
            running = false;
            // 主线程 阻塞等待;
            th.join();
        }
    }

    int ReplicateClient::conn_to_master() {
        int fd_ = socket(AF_INET,SOCK_STREAM, 0);
        if (fd_ < 0) {
            return -1;
        }
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        // 向主节点连接;
        addr.sin_port = htons(cof.rep_conf.master_port);
        inet_pton(AF_INET, cof.rep_conf.master_host.c_str(), &addr.sin_addr);
        if (connect(fd_, (sockaddr *) &addr, sizeof(addr)) < 0) {
            close(fd_);
            std::cout << "connect to master error" << std::endl;
            return -1;
        }
        std::cout << "connect to master success" << std::endl;
        fd = fd_;
        return fd;
    }

    void ReplicateClient::run_replicate() {
        if (conn_to_master() < 0) {
            return;
        }
        // send SYNC/PSYNC
        std::string first;
        if (last_replicate_offset > 0) {
            first = toArrayType({std::string("PSYNC"), std::to_string(last_replicate_offset)});
        } else {
            first = toArrayType({std::string("SYNC")});
        }

        ssize_t w = send(fd, first.data(), first.size(), 0);
        if (w < 0) {
            close(fd);
            return;
        }

        // read RDB bulk;
        RespParser parser;
        std::string buf(8192, '\0');
        while (running) {
            ssize_t r = ::recv(fd, buf.data(), buf.size(), 0);
            if (r <= 0) {
                break;
            }
            parser.append(std::string_view(buf.data(), (size_t)r));

            // 获取大量命令;
            while (true) {
                auto v = parser.tryParseOne();
                if (!v.has_value()) {
                    break;
                }

                // 获取rdb文件;
                if (v->type == RespType::BulkString) { // SYNC
                    RdbConfig rdb_conf = cof.rdb_conf;
                    if (!rdb_conf.enabled) {
                        rdb_conf.enabled = true;
                    }
                    RDBManager rdb(rdb_conf);
                    std::string path = rdb.path();
                    FILE *f = fopen(path.c_str(), "wb");
                    if (!f) {
                        return;
                    }
                    fwrite(v->bulk_string.data(), 1, v->bulk_string.size(), f);
                    fclose(f);
                    std::string err;
                    // 打开文件, 重映 rdb;
                    auto ok = rdb.load(storage, err);
                    if (!ok) {
                        std::cout << "load rdb error:" << err << std::endl;
                        return;
                    }
                } else if (v->type == RespType::Array) { // PSYNC
                    if (v->array.empty()) {
                        continue;
                    }
                    std::string cmd;
                    for (char &s: v->array[0].bulk_string) {
                        cmd.push_back((char) toupper(s));
                    }

                    // SET KEY VALUE
                    if (cmd == "SET") {
                        storage.set(v->array[1].bulk_string,
                                    v->array[2].bulk_string);
                    } else if (cmd == "DEL" && v->array.size() >= 2) {
                        std::vector<std::string> keys;
                        for (size_t i = 1; i < v->array.size(); ++i) {
                            keys.emplace_back(v->array[i].bulk_string);
                        }
                        storage.del(keys);
                    } else if (cmd == "EXPIRE" && v->array.size() == 3) {
                        int64_t expire = std::stoll(v->array[2].bulk_string);
                        storage.expire(v->array[1].bulk_string, expire);
                    } else if (cmd == "HSET" && v->array.size() == 4) {
                        storage.hset(v->array[1].bulk_string, v->array[2].bulk_string, v->array[3].bulk_string);
                    } else if (cmd == "HDEL" && v->array.size() >= 3) {
                        std::vector<std::string> fvs;
                        for (size_t i = 2; i < v->array.size(); ++i) {
                            fvs.emplace_back(v->array[i].bulk_string);
                        }
                        storage.hdel(v->array[1].bulk_string, fvs);
                    } else if (cmd == "ZADD" && v->array.size() == 4) {
                        storage.zadd(v->array[1].bulk_string,
                                     std::stod(v->array[2].bulk_string),
                                     v->array[3].bulk_string);
                    } else if (cmd == "ZREM" && v->array.size() >= 3) {
                        std::vector<std::string> members;
                        for (size_t i = 2; i < v->array.size(); ++i) {
                            members.emplace_back(v->array[i].bulk_string);
                        }
                        storage.zremove(v->array[1].bulk_string, members);
                    } else {
                        // 其他命令
                    }
                } else if (v->type == RespType::SimpleString) {// +OFFSET last_offset
                    std::string offset = v->bulk_string;
                    if (offset.rfind("OFFSET ", 0) == 0) {
                        try {
                            last_replicate_offset = std::stoll(offset.substr(8));
                        } catch (...) {
                            last_replicate_offset = 0;
                        }
                    }
                }
            }
        }// while running over
        std::cout << "replicate over" << std::endl;
        close(fd);
    }
}