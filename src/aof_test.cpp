//
// Created by 19327 on 2026/01/06/星期二
//

#include <map>
#include <string>
#include <iostream>
#include "aof.h"
#include "kv.h"

namespace aof_test {
    std::map<train_set::AofMode, std::string> aofModeMap = {
            {train_set::AofMode::None,     "None"},
            {train_set::AofMode::EverySec, "EverySec"},
            {train_set::AofMode::Always,   "Always"}
    };

    // 使用示例
    std::string aofModeToString(train_set::AofMode mode) {
        auto it = aofModeMap.find(mode);
        return (it != aofModeMap.end()) ? it->second : "UNKNOWN";
    }
}

int main_aof_test(int argc, char **argv) {
    train_set::AOFManager aofManager;
    train_set::KVStorage storage;
    train_set::AofConfig config;
    config.enabled = true;
    config.mode = train_set::AofMode::EverySec;
    config.dir = "../data_test";
    config.file_name = "test.aof";

    std::string err;
    if (!aofManager.init(config, err)) {
        std::cout << "Failed to init AOF: " << err << std::endl;
        return -1;
    }

    std::cout << "AOF Mode: " << aof_test::aofModeToString(aofManager.mode()) << std::endl;
    std::cout << "AOF Enabled: " << (aofManager.isEnabled() ? "true" : "false") << std::endl;

    std::string key = "test_key";
    std::string value = "test_value";
    auto ok = storage.set(key, value);

    // Test append command
    std::vector<std::string> cmd = {"SET", key, value};
    if (aofManager.appendCmd(cmd, false)) {
        std::cout << "Command appended successfully" << std::endl;
    } else {
        std::cout << "Failed to append command" << std::endl;
    }
    aofManager.shutdown();
    // Test loading from AOF;
    std::cout << "KV Storage size: " << storage.stringKeySize() << std::endl;

    train_set::KVStorage new_storage;
    aofManager.load(new_storage, err);
    std::cout << "new KV Storage size: " << new_storage.stringKeySize() << std::endl;
    aofManager.shutdown();
    return 0;
}

// 测试 1
int main1(int argc, char **argv) {
    return main_aof_test(0, nullptr);
}
