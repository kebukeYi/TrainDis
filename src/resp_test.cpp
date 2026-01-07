//
// Created by 19327 on 2026/01/06/星期二
//

#include <map>
#include <string>
#include <iostream>
#include "resp.h"

namespace resp_test {
    std::map<train_set::RespType, std::string> respTypeMap = {
            {train_set::RespType::SimpleString, "SimpleString"},
            {train_set::RespType::BulkString,   "BulkString"},
            {train_set::RespType::Integer,      "Integer"},
            {train_set::RespType::Array,        "Array"},
            {train_set::RespType::Error,        "Error"}
    };

    // 使用示例
    std::string respTypeToString(train_set::RespType type) {
        auto it = respTypeMap.find(type);
        return (it != respTypeMap.end()) ? it->second : "UNKNOWN";
    }
}

int main_resp_test(int argc, char **argv) {
    train_set::RespParser parser;
    // Simple String
    parser.append("+OK\r\n");
    std::optional<train_set::RespValue> value = parser.tryParseOne();
    if (value.has_value()) {
        std::cout << resp_test::respTypeToString(static_cast<train_set::RespType>(value.value().type))
                  << ": " << value.value().bulk_string << std::endl;
    }
    parser.clear();

    // Error
    parser.append("-ERR message\r\n");
    value = parser.tryParseOne();
    if (value.has_value()) {
        std::cout << resp_test::respTypeToString(static_cast<train_set::RespType>(value.value().type))
                  << ": " << value.value().bulk_string << std::endl;
    }
    parser.clear();

    // Integer
    parser.append(":123\r\n");
    value = parser.tryParseOne();
    if (value.has_value()) {
        std::cout << resp_test::respTypeToString(static_cast<train_set::RespType>(value.value().type))
                  << ": " << value.value().bulk_string << std::endl;
    }
    parser.clear();

    // Bulk String
    parser.append("$6\r\nfoobar\r\n");
    value = parser.tryParseOne();
    if (value.has_value()) {
        std::cout << resp_test::respTypeToString(static_cast<train_set::RespType>(value.value().type))
                  << ": " << value.value().bulk_string << std::endl;
    }
    parser.clear();

    // Array
    // set key val
    parser.append("*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n");
    value = parser.tryParseOne();
    if (value.has_value()) {
        std::cout << resp_test::respTypeToString(static_cast<train_set::RespType>(value.value().type)) << std::endl;
        if (value.value().type == train_set::RespType::Array) {
            for (auto &item: value.value().array) {
                std::cout << "  " << resp_test::respTypeToString(static_cast<train_set::RespType>(item.type))
                          << ": " << item.bulk_string << std::endl;
            }
        }
    }
    parser.clear();
    return 0;
}
// 测试5
int main5(int argc, char **argv) {
    return main_resp_test(0, nullptr);
}
