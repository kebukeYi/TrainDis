//
// Created by 19327 on 2026/01/06/星期二
//

#include <string>
#include <iostream>
#include "rdb.h"
#include "kv.h"

int main_rdb_test(int argc, char **argv) {
    train_set::RDBManager rdbManager;
    train_set::KVStorage storage;
    train_set::RdbConfig config;
    config.enabled = true;
    config.dir = "../data_test";
    config.file_name = "test.rdb";

    rdbManager.setConfig(config);

    std::cout << "RDB Path: " << rdbManager.path() << std::endl;

    // Test basic storage operations before dump
    std::string key = "rdb_test_key";
    std::string value = "rdb_test_value";
    storage.set(key, value);

    std::cout << "KV Storage size before dump: " << storage.stringKeySize() << std::endl;

    // Test dump operation
    std::string dump_err;
    if (rdbManager.dump(storage, dump_err)) {
        std::cout << "RDB dump successful" << std::endl;
    } else {
        std::cout << "RDB dump failed: " << dump_err << std::endl;
    }

    // Create a new storage to test loading
    train_set::KVStorage newStorage;
    std::string load_err;
    if (rdbManager.load(newStorage, load_err)) {
        std::cout << "RDB load successful" << std::endl;
        std::cout << "New KV Storage size after load: " << newStorage.stringKeySize() << std::endl;

        auto result = newStorage.get(key);
        if (result.has_value()) {
            std::cout << "Loaded value: " << result.value() << std::endl;
        } else {
            std::cout << "Key not found after load" << std::endl;
        }
    } else {
        std::cout << "RDB load failed: " << load_err << std::endl;
    }
    return 0;
}

// 测试4
int main4(int argc, char **argv) {
    return main_rdb_test(0, nullptr);
}
