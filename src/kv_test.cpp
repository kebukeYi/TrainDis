//
// Created by 19327 on 2026/01/06/星期二
//

#include <string>
#include <iostream>
#include "kv.h"

int main_kv_test(int argc, char **argv) {
    train_set::KVStorage storage;

    // Test SET and GET
    std::string key = "test_key";
    std::string value = "test_value";

    if (storage.set(key, value)) {
        std::cout << "SET operation successful" << std::endl;
    } else {
        std::cout << "SET operation failed" << std::endl;
    }

    auto result = storage.get(key);
    if (result.has_value()) {
        std::cout << "GET result: " << result.value() << std::endl;
    } else {
        std::cout << "Key not found" << std::endl;
    }

    // Test EXISTS
    std::cout << "Key exists: " << (storage.exists(key) ? "true" : "false") << std::endl;

    // Test Hash operations
    std::string hkey = "test_hash";
    std::string field = "field1";
    std::string hvalue = "hash_value";

    int hset_result = storage.hset(hkey, field, hvalue);
    std::cout << "HSET result (1:new, 0:overwrite): " << hset_result << std::endl;

    auto hget_result = storage.hget(hkey, field);
    if (hget_result.has_value()) {
        std::cout << "HGET result: " << hget_result.value() << std::endl;
    }

    std::cout << "HLEN result: " << storage.hlen(hkey) << std::endl;

    // Test ZSet operations
    std::string zkey = "test_zset";
    std::string member = "member1";
    double score = 95.5;

    int zadd_result = storage.zadd(zkey, score, member);
    std::cout << "ZADD result (new elements added): " << zadd_result << std::endl;

    auto zscore_result = storage.zscore(zkey, member);
    if (zscore_result.has_value()) {
        std::cout << "ZSCORE result: " << zscore_result.value() << std::endl;
    }

    // Test DEL
    std::vector<std::string> keys_to_delete = {key};
    int del_result = storage.del(keys_to_delete);
    std::cout << "DEL result (number of deleted keys): " << del_result << std::endl;

    std::cout << "Final KV Storage size: " << storage.stringKeySize() << std::endl;

    return 0;
}

// 测试 3
int main3(int argc, char **argv) {
    return main_kv_test(0, nullptr);
}
