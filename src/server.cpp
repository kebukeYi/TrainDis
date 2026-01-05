//
// Created by 19327 on 2025/12/31/星期三.
//
#include "server.h"
#include "log.h"
#include "resp.h"
#include "aof.h"
#include "rdb.h"
#include "kv.h"


#include <csignal>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/uio.h>
#include <fcntl.h>

namespace train_set {
    Server::Server(ServerConfig &config) : config(config) {}

    Server::~Server() {
        if (listen_fd != -1) {
            close(listen_fd);
        }
        if (epoll_fd != -1) {
            close(epoll_fd);
        }
    }

    namespace {
        int set_noBlocking(int fd) {
            int flags = fcntl(fd, F_GETFL, 0);
            if (flags < 0)
                return -1;
            if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
                return -1;
            return 0;
        }

        int add_epoll(int epoll_fd, int fd, uint32_t events) {
            epoll_event ev{};
            ev.data.fd = fd;
            ev.events = events;
            return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev);
        }

        int mod_epoll(int epoll_fd, int fd, uint32_t events) {
            epoll_event ev{};
            ev.events = events;
            ev.data.fd = fd;
            return epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
        }

        struct Conn {
            int fd = -1;
            std::string in = "";
            std::vector<std::string> out_chunks = {};
            size_t out_iov_idx = 0;
            size_t out_off = 0;
            RespParser parser = {};
            bool is_replica = false;
        };
    }


    int Server::setListener() {
        listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd < 0) {
            std::perror("socket");
            return -1;
        }
        int yes = 1;
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons((uint16_t) config.port);
        if (inet_pton(AF_INET, config.host.c_str(),
                      &addr.sin_addr) != 1) {
            MR_LOG("ERROR", "Invalid bind address: " << config.host);
            return -1;
        }
        if (bind(listen_fd, (sockaddr *) &addr,
                 sizeof(addr)) < 0) {
            std::perror("bind");
            return -1;
        }
        if (set_noBlocking(listen_fd) < 0) {
            std::perror("set_noBlocking");
            return -1;
        }
        if (listen(listen_fd, 1024) < 0) {
            std::perror("listen");
            return -1;
        }
        return 1;
    }

    int Server::setEpoll() {
        epoll_fd = epoll_create1(0);
        if (epoll_fd < 0) {
            std::perror("epoll_create1");
            return -1;
        }
        if (add_epoll(epoll_fd, listen_fd, EPOLLIN | EPOLLET) < 0) {
            std::perror("add_epoll");
            return -1;
        }
        time_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        if (time_fd < 0) {
            std::perror("timerfd_create");
            return -1;
        }
        itimerspec its{};
        its.it_interval.tv_sec = 0;
        its.it_interval.tv_nsec = 200 * 1000 * 1000; // 200ms;
        its.it_value = its.it_interval;
        if (timerfd_settime(time_fd, 0, &its, nullptr) < 0) {
            std::perror("timerfd_settime");
            return -1;
        }
        if (add_epoll(epoll_fd, time_fd, EPOLLIN) < 0) {
            std::perror("add_epoll");
            return -1;
        }
        MR_LOG("INFO", "Server started at " << config.host << ":" << config.port);
        return 1;
    }


    KVStorage g_store;
    static AOFManager g_aof;
    static RDBManager g_rdb;
    std::vector<std::vector<std::string>> g_repl_queue;

    bool has_pending(Conn &conn) {
        return conn.out_iov_idx < conn.out_chunks.size() ||
               (conn.out_iov_idx == conn.out_chunks.size() && conn.out_off != 0);
    }


    void conn_queue_add(Conn &conn, std::string out) {
        if (!out.empty()) {
            conn.out_chunks.emplace_back(std::move(out));
        }
    }

    std::string g_repl_back_part_log;
    size_t g_repl_back_part_cap = 4 * 1024 * 1024;
    int64_t g_back_log_off = 0;
    int64_t g_backlog_start_offset = 0;

    void append_cmd_to_back_log(std::string &cmd) {
        if (g_repl_back_part_log.size() + cmd.size() < g_repl_back_part_cap) {
            g_repl_back_part_log.append(cmd);
        } else {
            size_t need = cmd.size();
            if (need >= g_repl_back_part_cap) {
                g_repl_back_part_log.assign(cmd.data() + (need - g_repl_back_part_cap), g_repl_back_part_cap);
            } else {
                size_t drop = (g_repl_back_part_log.size() + need) - g_repl_back_part_cap;
                g_repl_back_part_log.erase(0, drop);
                g_repl_back_part_log.append(cmd.data(), need);
            }
        }
        g_backlog_start_offset = g_back_log_off - (int64_t) cmd.size();
    }

    std::string handle_cmd(RespValue &r, std::string *raw) {
        return "";
    }

    void conn_try_flush_now(int fd, Conn &conn, uint32_t &ev) {
        while (has_pending(conn)) {
            const size_t max_iov = 64;
            iovec iov[max_iov];
            int iov_count = 0;
            size_t idx = conn.out_iov_idx;
            size_t off = conn.out_off;
            while (iov_count < (int) max_iov && idx < conn.out_chunks.size()) {
                std::string &s = conn.out_chunks[idx];
                char *base = s.data();
                size_t len = s.size();
                if (off >= len) {
                    idx++;
                    off = 0;
                    continue;
                } else {
                    iov[iov_count].iov_base = base + off;
                    iov[iov_count].iov_len = len - off;
                    off = 0;
                    iov_count++;
                    idx++;
                }
            }// while iov over
            if (iov_count == 0) {
                break;
            }
            ssize_t w = ::writev(fd, iov, iov_count);
            if (w > 0) {
                size_t write_len = size_t(w);
                while (write_len > 0 && conn.out_iov_idx < conn.out_chunks.size()) {
                    if (write_len >= conn.out_chunks[conn.out_iov_idx].size()) {
                        write_len -= conn.out_chunks[conn.out_iov_idx].size();
                        conn.out_iov_idx++;
                    } else {
                        conn.out_chunks[conn.out_iov_idx].erase(0, write_len);
                        conn.out_off = 0;
                        write_len = 0;
                    }
                }// while write over
                if (conn.out_iov_idx >= conn.out_chunks.size()) {
                    conn.out_chunks.clear();
                    conn.out_iov_idx = 0;
                    conn.out_off = 0;
                }
            } else if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                break;
            } else {
                std::perror("writev");
                ev |= EPOLLRDHUP;
                break;
            }
        }// while (has_pending(conn)) over
    }

    int Server::run() {
        if (setListener() < 0) {
            return -1;
        }
        if (setEpoll() < 0) {
            return -1;
        }
        if (config.rdb_conf.enabled) {
            g_rdb.setConfig(config.rdb_conf);
            std::string err;
            if (!g_rdb.load(&g_store, err)) {
                MR_LOG("ERROR", "RDB load failed: " << err);
                return -1;
            }
        }
        if (config.aof_conf.enabled) {
            std::string err;
            if (!g_aof.init(config.aof_conf, err)) {
                MR_LOG("ERROR", "AOF init failed: " << err);
                return -1;
            }
            if (!g_aof.load(g_store, err)) {
                MR_LOG("ERROR", "AOF load failed: " << err);
                return -1;
            }
        }
        int rc = epoll_loop();
        return rc;
    }

    int Server::epoll_loop() {
        std::unordered_map<int, Conn> conns;
        std::vector<epoll_event> events(1024);
        while (true) {
            int n = epoll_wait(epoll_fd, events.data(), (int) events.size(), -1);
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                std::perror("epoll_wait");
                return -1;
            }
            for (int i = 0; i < n; ++i) {
                auto fd_ = events[i].data.fd;
                uint32_t ev = events[i].events;
                // 连接事件;
                if (fd_ == listen_fd) {
                    while (true) {
                        sockaddr_in in{};
                        socklen_t len = sizeof(in);
                        int cfd = accept(listen_fd, (sockaddr *) &in, &len);
                        if (cfd < 0) {
                            if (errno == EWOULDBLOCK || errno == EAGAIN) {
                                break;
                            }
                            std::perror("accept err");
                            break;
                        }
                        set_noBlocking(cfd);
                        int yes = 1;
                        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
                        add_epoll(epoll_fd, cfd, EPOLLIN | EPOLLET);
                        conns.emplace(cfd, Conn{cfd, std::string(), std::vector<std::string>(),
                                                0, 0, RespParser(), false});
                    }
                    continue;
                }

                // 时间周期事件;
                if (fd_ == time_fd) {
                    uint16_t ticks;
                    ssize_t r = read(time_fd, &ticks, sizeof(ticks));
                    if (r < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break;
                        } else {
                            break;
                        }
                    }
                    if (r == 0) {
                        break;
                    }
                    g_store.expireScanStep(64);
                    continue;
                }

                auto it = conns.find(fd_);
                if (it == conns.end()) {
                    continue;
                }
                Conn &conn = it->second;

                // epollhup || epollerr
                if ((ev & EPOLLHUP) || (ev & EPOLLERR)) {
                    MR_LOG("INFO", "Connection closed: " << conn.fd);
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd_, nullptr);
                    close(fd_);
                    conns.erase(fd_);
                    continue;
                }// epollhup || epollerr over

                if (ev & EPOLLIN) {
                    char buf[4096];
                    while (true) {
                        ssize_t rn = read(fd_, buf, sizeof(buf));
                        if (rn > 0) {
                            conn.parser.append(std::string_view(buf, (size_t) rn));
                        } else if (rn == 0) {
                            ev |= EPOLLHUP;
                            break;
                        } else {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break;
                            }
                            std::perror("read");
                            ev |= EPOLLRDHUP;
                            break;
                        }
                    }// while read over

                    while (true) {
                        auto maybe = conn.parser.tryParseOneWithRaw();
                        if (!maybe.has_value()) {
                            break;
                        }
                        RespValue &rv = maybe->first;
                        std::string &raw = maybe->second;
                        if (rv.type == RespType::Error) {
                            conn_queue_add(conn, respError(rv.bulk_string));
                        } else {
                            if (rv.type == RespType::Array && rv.array.size() > 0 && (
                                    rv.array[0].type == RespType::BulkString ||
                                    rv.array[0].type == RespType::SimpleString)) {
                                std::string cmd;
                                cmd.reserve(rv.array[0].bulk_string.size());
                                for (auto &c: rv.array[0].bulk_string) {
                                    cmd.push_back((char) ::toupper(c));
                                }
                                if (cmd == "PSYNC") {
                                    if (rv.array.size() > 2 && rv.array[1].type == RespType::BulkString) {
                                        int64_t last_offset = 0;
                                        try {
                                            last_offset = std::stoll(rv.array[1].bulk_string);
                                        } catch (...) {
                                            MR_LOG("ERROR", "Invalid PSYNC offset: " << rv.array[1].bulk_string);
                                            last_offset = -1;
                                        }
                                        if (last_offset >= g_backlog_start_offset && last_offset <= g_back_log_off) {
                                            size_t start_offset = (size_t) (last_offset - g_backlog_start_offset);
                                            if (start_offset < g_repl_back_part_log.size()) {
                                                conn.is_replica = true;
                                                std::string off = "+OFFSET " + std::to_string(g_back_log_off) + "\r\n";
                                                conn_queue_add(conn, off);
                                                conn_queue_add(conn, g_repl_back_part_log.substr(start_offset));
                                                continue;
                                            }
                                        }
                                    }
                                }
                                if (cmd == "SYNC") {
                                    std::string err;
                                    RdbConfig rdb_temp = config.rdb_conf;
                                    if (!rdb_temp.enabled) {
                                        rdb_temp.enabled = true;
                                    }
                                    RDBManager rdb(rdb_temp);
                                    if (!rdb.dump(&g_store, err)) {
                                        conn_queue_add(conn, respError("ERR sync save failed"));
                                    } else {
                                        std::string path = rdb.path();
                                        FILE *f = fopen(path.c_str(), "rb");
                                        if (!f) {
                                            conn_queue_add(conn, respError("ERR open rdb file failed"));
                                        } else {
                                            std::string content;
                                            content.resize(0);
                                            char string[8192];
                                            size_t r;
                                            while ((r = fread(string, 1, sizeof(string), f)) > 0) {
                                                content.append(string, r);
                                            }
                                            fclose(f);
                                            conn_queue_add(conn, respBulk(content));
                                            conn.is_replica = true;
                                            std::string off = "+OFFSET " + std::to_string(g_back_log_off) + "\r\n";
                                            conn_queue_add(conn, std::move(off));
                                        }
                                    }
                                    continue;
                                }
                            } else {
                                //
                            }
                        }
                    }// while parse over

                    // 广播数据;
                    if (!g_repl_queue.empty()) {
                        for (auto &kv: conns) {
                            Conn &conn = kv.second;
                            if (!conn.is_replica) {
                                continue;
                            }
                            for (auto &parts: g_repl_queue) {
                                std::string cmd = toArrayType(parts);
                                int64_t next_offset = g_back_log_off + (int64_t) cmd.size();
                                g_back_log_off = next_offset;
                                std::string off = "+OFFSET " + std::to_string(next_offset) + "\r\n";
                                append_cmd_to_back_log(off);
                                append_cmd_to_back_log(cmd);
                                conn_queue_add(conn, std::move(off));
                                conn_queue_add(conn, std::move(cmd));
                            }
                            if (has_pending(conn)) {
                                mod_epoll(epoll_fd, conn.fd, EPOLLIN | EPOLLET | EPOLLOUT | EPOLLRDHUP | EPOLLHUP);
                            }
                        }// for replicas con over
                        g_repl_queue.clear();
                    }// if replication queue over

                    // If peer half-closed and nothing pending, close now
                    if ((ev & EPOLLRDHUP) && !has_pending(conn)) {
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, conn.fd, nullptr);
                        close(fd_);
                        conns.erase(it);
                        continue;
                    }
                }// if EPOLLIN over

                if (ev & EPOLLOUT) {
                    conn_try_flush_now(fd_, conn, ev);
                    if (!has_pending(conn)) {
                        mod_epoll(epoll_fd, conn.fd, EPOLLIN | EPOLLRDHUP | EPOLLET);
                        if (ev & EPOLLRDHUP) {
                            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, conn.fd, nullptr);
                            close(conn.fd);
                            conns.erase(it);
                            continue;
                        }
                    }
                }// if EPOLLOUT over
            }
        }
    }
}