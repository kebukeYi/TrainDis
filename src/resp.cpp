//
// Created by 19327 on 2025/12/31/星期三.
//
#include <charconv>
#include "resp.h"

namespace train_set {
    void RespParser::append(std::string_view data) {
        m_buffer.append(data.data(), data.size());
    }

    bool RespParser::parse_line(size_t &pos, std::string &out_line) {
        auto end = m_buffer.find("\r\n", pos);
        if (end == std::string::npos) {
            return false;
        }
        out_line.assign(m_buffer.data() + pos, end - pos);
        return true;
    }

    // +OK\r\n
    bool RespParser::parse_simple_string(size_t &pos, RespType type, RespValue &value) {
        std::string line;
        if (!parse_line(pos, line)) {
            return false;
        }
        value.type = type;
        value.bulk_string = std::move(line);
        return true;
    }

    // :12345\r\n
    bool RespParser::parse_integer(size_t &pos, int64_t &value) {
        std::string line;
        if (!parse_line(pos, line)) {
            return false;
        }
        auto first = line.data();
        auto last = line.data() + line.size();
        int64_t v = 0;
        auto [ptr, err] = std::from_chars(first, last, v);
        if (err != std::errc() || ptr != last) {
            return false;
        }
        value = v;
        return true;
    }

    // $6\r\nfoobar\r\n
    bool RespParser::parse_bulk_string(size_t &pos, RespValue &value) {
        int64_t len;
        if (!parse_integer(pos, len)) {
            return false;
        }
        if (len == -1) {
            value.type = RespType::Null;
            return true;
        }
        if (len < 0 || (m_buffer.size() < pos + len + 2)) {
            return false;
        }
        value.type = RespType::BulkString;
        value.bulk_string.assign(m_buffer.data() + pos, len);
        pos += len;
        if (!(m_buffer[pos] == '\r' && m_buffer[pos + 1] == '\n')) {
            return false;
        }
        pos += 2;
        return true;
    }

    // *3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$5\r\nbazinga\r\n
    bool RespParser::parse_array(size_t &pos, RespValue &value) {
        int64_t count;
        if (!parse_integer(pos, count)) {
            return false;
        }
        if (count == -1) {
            value.type = RespType::Null;
            return true;
        }
        if (count < 0) {
            return false;
        }
        value.type = RespType::Array;
        value.array.clear();
        value.array.reserve(count);
        for (int64_t i = 0; i < count; ++i) {
            char prefix = m_buffer[pos++];
            RespValue item;
            bool ok = false;
            switch (prefix) {
                case '+':
                    ok = parse_simple_string(pos, RespType::SimpleString, item);
                    break;
                case '-':
                    ok = parse_simple_string(pos, RespType::Error, item);
                    break;
                case ':': {
                    int64_t v = 0;
                    ok = parse_integer(pos, v);
                    if (ok) {
                        item.type = RespType::Integer;
                        item.bulk_string = std::to_string(v);
                    }
                    break;
                }
                case '$':
                    ok = parse_bulk_string(pos, item);
                    break;
                case '*':
                    ok = parse_array(pos, item);
                    break;
                default:
                    break;
            }
            if (!ok) {
                return false;
            }
            value.array.emplace_back(std::move(item));
        }
        return true;
    }

    std::optional<RespValue> RespParser::tryParseOne() {
        if (m_buffer.empty()) {
            return std::nullopt;
        }
        RespValue value;
        size_t pos = 0;
        char prefix = m_buffer[pos++];
        bool ok = false;
        switch (prefix) {
            case '+':
                ok = parse_simple_string(pos, RespType::SimpleString, value);
                break;
            case '-':
                ok = parse_simple_string(pos, RespType::Error, value);
                break;
            case ':': {
                int64_t v = 0;
                ok = parse_integer(pos, v);
                if (ok) {
                    value.type = RespType::Integer;
                    value.bulk_string = std::to_string(v);
                }
            }
            case '$':
                ok = parse_bulk_string(pos, value);
                break;
            case '*':
                ok = parse_array(pos, value);
                break;
            default:
                return RespValue{RespType::Error, std::string("protocol error"), {}};
        }
        if (!ok) {
            return std::nullopt;
        }
        m_buffer.erase(0, pos);
        return value;
    }

    std::optional<std::pair<RespValue, std::string>> RespParser::tryParseOneWithRaw() {
        if (m_buffer.empty()) {
            return std::nullopt;
        }
        RespValue value;
        size_t pos = 0;
        char prefix = m_buffer[pos++];
        bool ok = false;
        switch (prefix) {
            case '+':
                ok = parse_simple_string(pos, RespType::SimpleString, value);
                break;
            case '-':
                ok = parse_simple_string(pos, RespType::Error, value);
                break;
            case ':': {
                int64_t v = 0;
                ok = parse_integer(pos, v);
                if (ok) {
                    value.type = RespType::Integer;
                    value.bulk_string = std::to_string(v);
                }
            }
            case '$':
                ok = parse_bulk_string(pos, value);
                break;
            case '*':
                ok = parse_array(pos, value);
                break;
            default:
                return std::make_optional(
                        std::make_pair(RespValue{RespType::Error,
                                                 std::string("protocol error"), {}},
                                       std::string()));
        }
        if (!ok) {
            return std::nullopt;
        }
        // 原始字节: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n;
        std::string raw(m_buffer.data(), pos);
        m_buffer.erase(0, pos);
        return std::make_optional(std::make_pair(value, raw));
    }
}

namespace train_set {

    std::string respSimpleString(std::string_view s) {
        return std::string("+") + std::string(s) + "\r\n";
    }

    std::string respError(std::string_view s) {
        return std::string("-") + std::string(s) + "\r\n";
    }

    std::string respBulk(std::string_view s) {
        return "$" + std::to_string(s.size()) + "\r\n" + std::string(s) + "\r\n";
    }

    std::string respNullBulk() {
        return "$-1\r\n";
    }

    std::string respInteger(int64_t v) {
        return ":" + std::to_string(v) + "\r\n";
    }
}