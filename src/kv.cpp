//
// Created by 19327 on 2025/12/31/星期三.
//
#include <algorithm>
#include "kv.h"

namespace train_set {

    struct skipListNode {
        double score;
        std::string member;
        std::vector<skipListNode *> forward;

        skipListNode(int level, double score, std::string member) :
                score(score), member(std::move(member)), forward((size_t) level, nullptr) {}
    };

    skipList::skipList() : head(new skipListNode(maxLevel, 0.0, "")), level(1), length(0) {}

    skipList::~skipList() {
        skipListNode *cur = head->forward[0];
        while (cur) {
            skipListNode *next = cur->forward[0];
            delete cur;
            cur = next;
        }
        delete head;
    }

    int skipList::randomLevel() {
        int level = 1;
        while (level < maxLevel && ((std::rand() & 0xFFFFF) <= (int) (probability * 0xFFFFF))) {
            level++;
        }
        return level;
    }

    // static: 当前文件可见, 不作为公共接口, 防止和其他文件同门函数产生链接错误;
    // 判断 a 是否 小于 b;
    // 使用 内联 对于频繁调用的小函数，可提高执行效率
    static inline bool compareMember(double a_score, const std::string &a_member,
                                     double b_score, const std::string &b_member) {
        if (a_score != b_score) {
            return a_score < b_score;
        }
        return a_member < b_member;
    }

    bool skipList::insert(double score, const std::string &member) {
        // 每层的待插入点的前节点[];
        std::vector<skipListNode *> update(maxLevel);
        skipListNode *cur = head;
        for (int i = level - 1; i >= 0; i--) {
            while (cur->forward[i] && compareMember(cur->forward[i]->score, cur->forward[i]->member, score, member)) {
                cur = cur->forward[i];
            }
            update[(size_t) i] = cur;
        }

        cur = cur->forward[0];
        if (cur && cur->member == member && cur->score == score) {
            return false;
        }
        int newLevel = randomLevel();
        if (newLevel > level) {
            for (int i = level; i < newLevel; i++) {
                update[i] = head;
            }
            level = newLevel;
        }
        // 有内存泄露风险;
        // auto *nx = new SkiplistNode(lvl, score, member);
        // 使用 unique_ptr 管理新节点, 如果后面任何一步抛异常，unique_ptr 会自动析构，节点被 delete，不会泄漏;
        std::unique_ptr newNode = std::make_unique<skipListNode>(newLevel, score, member);
        for (int i = 0; i < newLevel; i++) {
            newNode->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = newNode.get();
        }
        newNode.release();
        length++;
        return true;
    }

    bool skipList::erase(double score, const std::string &member) {
        std::vector<skipListNode *> update(maxLevel);
        skipListNode *cur = head;
        for (int i = level - 1; i >= 0; i--) {
            while (cur->forward[i] && compareMember(cur->forward[i]->score, cur->forward[i]->member, score, member)) {
                cur = cur->forward[i];
            }
            update[i] = cur;
        }
        cur = cur->forward[0];
        if (!cur || cur->member != member || cur->score != score) {
            return false;
        }
        for (int i = 0; i < level; i++) {
            if (update[i]->forward[i] == cur) {
                update[i]->forward[i] = cur->forward[i];
            }
        }
        delete cur;
        // 从顶开始向下判断, 删除的节点是否在 索引中;
        while (level > 1 && head->forward[level - 1] == nullptr) {
            level--;
        }
        length--;
        return true;
    }

    void skipList::rangeByRank(int64_t start, int64_t end, std::vector<std::string> &out) {
        if (length == 0) {
            return;
        }
        auto n = (int64_t) length;
        auto format = [&](int64_t idx) {
            if (idx < 0) {
                idx = n + idx;
            }
            if (idx < 0) {
                idx = 0;
            }
            if (idx >= n) {
                idx = n - 1;
            }
            return idx;
        };
        start = format(start);
        end = format(end);
        if (start > end) {
            return;
        }
        int64_t rank = 0;
        skipListNode *next = head->forward[0];
        while (rank < start && next) {
            next = next->forward[0];
            rank++;
        }
        while (rank <= end && next) {
            out.push_back(next->member);
            next = next->forward[0];
            rank++;
        }
    }

    void skipList::allValue(std::vector<std::pair<double, std::string>> &out) {
        out.clear();
        out.reserve(length);
        skipListNode *cur = head->forward[0];
        while (cur) {
            out.emplace_back(cur->score, cur->member);
            cur = cur->forward[0];
        }
    }

    size_t skipList::size() const {
        return length;
    }

    // ---------------- KeyValueStore implementation -----------------

    int64_t KVStorage::nowMs() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    }

    bool KVStorage::set(const std::string &key, const std::string &value, std::optional<int64_t> ttl_ms) {
        std::lock_guard<std::mutex> lk(mtx);
        int64_t expire_at = -1;
        if (ttl_ms.has_value()) {
            expire_at = nowMs() + ttl_ms.value();
        }
        string_records[key] = StringRecord{value, expire_at};
        if (expire_at >= 0) {
            expires[key] = expire_at;
        } else {
            expires.erase(key);
        }
        return true;
    }

    bool KVStorage::setWithExpire(const std::string &key, const std::string &value, int64_t expire_at_ms) {
        std::lock_guard<std::mutex> lk(mtx);
        string_records[key] = StringRecord{value, expire_at_ms};
        if (expire_at_ms >= 0) {
            expires[key] = expire_at_ms;
        }
        return true;
    }

    std::optional<std::string> KVStorage::get(const std::string &key) {
        std::lock_guard<std::mutex> lk(mtx);
        int64_t now_ms = nowMs();
        clearIfExpiredOfString(key, now_ms);
        auto it = string_records.find(key);
        if (it == string_records.end()) {
            return std::nullopt;
        }
        return std::make_optional(it->second.value);
    }

    bool KVStorage::exists(const std::string &key) {
        std::lock_guard<std::mutex> lk(mtx);
        int64_t now_ms = nowMs();
        clearIfExpiredOfString(key, now_ms);
        return string_records.find(key) != string_records.end() ||
               zset_records.find(key) != zset_records.end() ||
               hash_records.find(key) != hash_records.end();
    }

    int KVStorage::del(const std::vector<std::string> &keys) {
        std::lock_guard<std::mutex> lk(mtx);
        int count = 0;
        int64_t now_ms = nowMs();
        for (auto key: keys) {
            clearIfExpiredOfString(key, now_ms);
            auto it = string_records.find(key);
            if (it != string_records.end()) {
                string_records.erase(it);
                expires.erase(key);
                count++;
            }
        }
        return count;
    }

    bool KVStorage::expire(const std::string &key, int64_t ttl_seconds) {
        std::lock_guard<std::mutex> lk(mtx);
        int64_t now_ms = nowMs();
        clearIfExpiredOfString(key, now_ms);
        auto it = string_records.find(key);
        if (it == string_records.end()) {
            return false;
        }
        if (ttl_seconds < 0) {
            it->second.expire = -1;
            expires.erase(key);
            return true;
        }
        it->second.expire = now_ms + ttl_seconds * 1000;
        expires[key] = it->second.expire;
        return true;
    }

    // 还剩多久存活时间; 单位/秒;
    int64_t KVStorage::ttl(const std::string &key) {
        std::lock_guard<std::mutex> lk(mtx);
        int64_t now_ms = nowMs();
        clearIfExpiredOfString(key, now_ms);
        auto it = string_records.find(key);
        if (it == string_records.end()) {
            return -2;
        }
        if (it->second.expire < 0) {
            return -1;
        }
        int64_t left_ms = it->second.expire - now_ms;
        if (left_ms <= 0) {
            return -2;
        }
        return left_ms / 1000;
    }

    int KVStorage::expireScanStep(int max_step) {
        std::lock_guard lock(mtx);
        if (max_step <= 0 || expires.empty()) {
            return 0;
        }
        auto it = expires.begin();
        std::advance(it, (long)(std::rand() % expires.size()));
        int removed = 0;
        int64_t now = nowMs();
        for (int i = 0; i < max_step && !expires.empty(); ++i) {
            if (it == expires.end()) {
                it = expires.begin();
            }
            auto key = it->first;
            int64_t when = it->second;
            if (when >= 0 && when <= now) {
                string_records.erase(key);
                hash_records.erase(key);
                zset_records.erase(key);
                it = expires.erase(it);
                removed++;
            } else {
                ++it;
            }
        }
        return removed;
    }

    bool KVStorage::isExpiredOfString(const StringRecord &record, int64_t now_ms) {
        return record.expire >= 0 && record.expire <= now_ms;
    }

    bool KVStorage::isExpiredOfHash(const HashRecord &record, int64_t now_ms) {
        return record.expire >= 0 && record.expire <= now_ms;
    }

    bool KVStorage::isExpiredOfZSet(const ZSetRecord &record, int64_t now_ms) {
        return record.expire >= 0 && record.expire <= now_ms;
    }

    void KVStorage::clearIfExpiredOfString(const std::string &key, int64_t now_ms) {
        auto it = string_records.find(key);
        if (it == string_records.end()) {
            return;
        }
        if (isExpiredOfString(it->second, now_ms)) {
            string_records.erase(it);
            expires.erase(key);
        }
    }

    void KVStorage::clearIfExpiredOfHash(const std::string &key, int64_t now_ms) {
        auto it = hash_records.find(key);
        if (it == hash_records.end()) {
            return;
        }
        if (isExpiredOfHash(it->second, now_ms)) {
            hash_records.erase(it);
            expires.erase(key);
        }
    }

    void KVStorage::clearIfExpiredOfZSet(const std::string &key, int64_t now_ms) {
        auto it = zset_records.find(key);
        if (it == zset_records.end()) {
            return;
        }
        if (isExpiredOfZSet(it->second, now_ms)) {
            // ZSetRecord 析构函数会释放 ~skipList();
            zset_records.erase(it);
            expires.erase(key);
        }
    }

    std::vector<std::pair<std::string, StringRecord>> KVStorage::stringSnapshot() {
        std::lock_guard<std::mutex> lock(mtx);
        std::vector<std::pair<std::string, StringRecord>> out;
        out.reserve(string_records.size());
        for (auto &item: string_records) {
            out.emplace_back(item.first, item.second);
        }
        return out;
    }

    std::vector<std::pair<std::string, HashRecord>> KVStorage::hashSnapshot() {
        std::lock_guard<std::mutex> lock(mtx);
        std::vector<std::pair<std::string, HashRecord>> out;
        out.reserve(hash_records.size());
        for (auto &item: hash_records) {
            out.emplace_back(item.first, item.second);
        }
        return out;
    }

    std::vector<KVStorage::ZSetFlat> KVStorage::zSetSnapshot() {
        std::lock_guard<std::mutex> lock(mtx);
        std::vector<KVStorage::ZSetFlat> out;
        out.reserve(zset_records.size());
        for (auto &item: zset_records) {
            KVStorage::ZSetFlat flat;
            flat.key = item.first;
            flat.expire = item.second.expire;
            if (item.second.useSkipList) {
                item.second.sl->allValue(flat.items);
            } else {
                flat.items = item.second.items;
            }
            out.emplace_back(std::move(flat));
        }
        return out;
    }

    std::vector<std::string> KVStorage::listKeys() {
        std::lock_guard<std::mutex> lockGuard(mtx);
        std::vector<std::string> out;
        out.reserve(string_records.size() + hash_records.size() + zset_records.size());
        for (auto &item: string_records) {
            if(isExpiredOfString(item.second, nowMs())){
                continue;
            }
            out.emplace_back(item.first);
        }
        for (auto &item: hash_records) {
            if(isExpiredOfHash(item.second, nowMs())){
                continue;
            }
            out.emplace_back(item.first);
        }
        for (auto &item: zset_records) {
            if(isExpiredOfZSet(item.second, nowMs())){
                continue;
            }
            out.emplace_back(item.first);
        }
        // 排序, 然后移除相邻的重复元素，保留唯一值;
        std::sort(out.begin(), out.end());
        out.erase(std::unique(out.begin(), out.end()), out.end());
        return out;
    }


    int KVStorage::hset(const std::string &key,const std::string &field,const std::string &value) {
        std::lock_guard<std::mutex> lockGuard(mtx);
        int64_t now_ms = nowMs();
        clearIfExpiredOfHash(key, now_ms);
        // 如果 key 不存在，会创建 key -> "" 的映射，并返回 "" 的引用;
        auto &rec = hash_records[key];
        auto it = rec.field_vals.find(key);
        if (it == rec.field_vals.end()) {
            rec.field_vals[field] = value;
            return 1;
        }
        it->second = value;
        return 0;
    }

    bool KVStorage::hsetWithExpire(const std::string &key, int64_t expire) {
        std::lock_guard<std::mutex> lockGuard(mtx);
        auto it = hash_records.find(key);
        if (it == hash_records.end()) {
            return false;
        }
        it->second.expire = expire;
        if (expire >= 0) {
            expires[key] = expire;
        } else {
            expires.erase(key);
        }
        return true;
    }

    std::optional<std::string> KVStorage::hget(const std::string &key, const std::string &field) {
        std::lock_guard<std::mutex> lockGuard(mtx);
        int64_t now_ms = nowMs();
        clearIfExpiredOfHash(key, now_ms);
        auto it = hash_records.find(key);
        if (it == hash_records.end()) {
            return std::nullopt;
        }
        auto itt = it->second.field_vals.find(field);
        if (itt == it->second.field_vals.end()) {
            return std::nullopt;
        }
        return itt->second;
    }

    int KVStorage::hdel(const std::string &key, const std::vector<std::string> &fields) {
        std::lock_guard<std::mutex> lockGuard(mtx);
        int64_t now_ms = nowMs();
        clearIfExpiredOfHash(key, now_ms);
        auto it = hash_records.find(key);
        if (it == hash_records.end()) {
            return 0;
        }
        int count = 0;
        for (auto &field: fields) {
            auto itt = it->second.field_vals.find(field);
            if (itt != it->second.field_vals.end()) {
                it->second.field_vals.erase(itt);
                count++;
            }
        }
        if (it->second.field_vals.empty()) {
            hash_records.erase(it);
        }
        return count;
    }

    bool KVStorage::hexists(const std::string &key, const std::string &field) {
        std::lock_guard<std::mutex> lockGuard(mtx);
        int64_t now_ms = nowMs();
        clearIfExpiredOfHash(key, now_ms);
        auto it = hash_records.find(key);
        if (it == hash_records.end()) {
            return false;
        }
        auto itt = it->second.field_vals.find(field);
        return itt != it->second.field_vals.end();
    }

    std::vector<std::string> KVStorage::hgetAllByKey(const std::string &key) {
        std::lock_guard<std::mutex> lockGuard(mtx);
        int64_t now_ms = nowMs();
        clearIfExpiredOfHash(key, now_ms);
        std::vector<std::string> out;
        auto it = hash_records.find(key);
        if (it == hash_records.end()) {
            return out;
        }
        out.reserve(it->second.field_vals.size() * 2);
        for (auto &item: it->second.field_vals) {
            out.push_back(item.first);
            out.push_back(item.second);
        }
        return out;
    }

    int KVStorage::hlen(const std::string &key) {
        std::lock_guard<std::mutex> lockGuard(mtx);
        int64_t now_ms = nowMs();
        clearIfExpiredOfHash(key, now_ms);
        auto it = hash_records.find(key);
        if (it == hash_records.end()) {
            return 0;
        }
        return (int)it->second.field_vals.size();
    }


    int KVStorage::zadd(const std::string &key, double score,const std::string &member) {
        std::lock_guard<std::mutex> lockGuard(mtx);
        int64_t now_ms = nowMs();
        clearIfExpiredOfZSet(key, now_ms);
        auto &rec = zset_records[key];
        auto mit = rec.member_to_score.find(member);
        if (mit == rec.member_to_score.end()) {
            // 插入逻辑;
            // 非跳表, 只是容器;
            if (!rec.useSkipList) {
                auto &vec = rec.items;
                // 通过自定义函数, 找到要插入的位置;
                auto vec_index = std::lower_bound(vec.begin(), vec.end(), std::make_pair(score, member),
                                                  [](auto &a, auto &b) {
                                                      if (a.first != b.first) {
                                                          return a.first < b.first;
                                                      }
                                                      return a.second < b.second;
                                                  });
                // 执行插入;
                vec.insert(vec_index, std::make_pair(score, member));
                if (vec.size() > ZSetVectorThreshold) {
                    rec.useSkipList = true;
                    rec.sl = std::make_unique<skipList>();
                    for (auto &item: vec) {
                        rec.sl->insert(item.first, item.second);
                    }
                    // 释放空间;
                    std::vector<std::pair<double, std::string>>().swap(rec.items);
                }
            } else {
                rec.sl->insert(score, member);
            }
            // 更新 member_to_score 容器映射关系;
            rec.member_to_score.emplace(member, score);
            return 1;
        } else {
            // 更新逻辑;
            double old_score = mit->second;
            if (old_score == score) {
                return 0;
            }
            if (rec.useSkipList) {
                rec.sl->erase(old_score, member);
                rec.sl->insert(score, member);
            } else {
                auto vec = rec.items;
                for (auto vit = vec.begin(); vit != vec.end(); ++vit) {
                    if (vit->first == old_score && vit->second == member) {
                        vec.erase(vit);
                        break;
                    }
                }
                auto vec_index = std::lower_bound(vec.begin(), vec.end(), std::make_pair(score, member),
                                                  [](auto &a, auto &b) {
                                                      if (a.first != b.first) {
                                                          return a.first < b.first;
                                                      }
                                                      return a.second < b.second;
                                                  });
                vec.insert(vec_index, std::make_pair(score, member));
                if (vec.size() > ZSetVectorThreshold) {
                    rec.useSkipList = true;
                    rec.sl = std::make_unique<skipList>();
                    for (auto &item: vec) {
                        rec.sl->insert(item.first, item.second);
                    }
                    // 释放原来 vector 的空间;
                    std::vector<std::pair<double, std::string>>().swap(rec.items);
                }
            }

            mit->second = score;
            return 0;
        }
    }

    bool KVStorage::zaddWithExpire(const std::string &key, int64_t expire_at_ms) {
        std::lock_guard<std::mutex> lockGuard(mtx);
        auto it = zset_records.find(key);
        if (it == zset_records.end()) {
            return false;
        }
        it->second.expire = expire_at_ms;
        if (expire_at_ms >= 0) {
            expires[key] = expire_at_ms;
        } else {
            expires.erase(key);
        }
        return true;
    }

    std::vector<std::string> KVStorage::zrange(const std::string &key, int64_t start, int64_t end) {
        std::lock_guard<std::mutex> lockGuard(mtx);
        int64_t now_ms = nowMs();
        clearIfExpiredOfZSet(key, now_ms);
        std::vector<std::string> out;
        auto it = zset_records.find(key);
        if (it == zset_records.end()) {
            return out;
        }
        if (it->second.useSkipList) {
            it->second.sl->rangeByRank(start, end, out);
        } else {
            auto &vec = it->second.items;
            int64_t n = (int64_t )vec.size();
            if (n == 0) {
                return out;
            }
            auto norm = [&](int64_t idx) {
                if (idx < 0) idx = n + idx;
                if (idx < 0) idx = 0;
                if (idx >= n) idx = n - 1;
                return idx;
            };
            int64_t s = norm(start), e = norm(end);
            if (s > e) {
                return out;
            }
            for (int64_t i = s; i <= e; ++i) {
                auto &item = vec[(size_t)i];
                out.push_back(item.second);
            }
        }
        return out;
    }


    std::optional<double> KVStorage::zscore(const std::string &key, const std::string &member) {
        std::lock_guard<std::mutex> lockGuard(mtx);
        int64_t now_ms = nowMs();
        clearIfExpiredOfZSet(key, now_ms);
        auto it = zset_records.find(key);
        if (it == zset_records.end()) {
            return std::nullopt;
        }
        auto mit = it->second.member_to_score.find(member);
        if (mit == it->second.member_to_score.end()) {
            return std::nullopt;
        }
        return mit->second;
    }

    int KVStorage::zremove(const std::string &key,const std::vector<std::string> &members) {
        std::lock_guard<std::mutex> lk(mtx);
        int64_t now = nowMs();
        clearIfExpiredOfZSet(key, now);
        auto it = zset_records.find(key);
        if (it == zset_records.end()) {
            return 0;
        }
        int removed = 0;
        for (auto &m: members) {
            auto mit = it->second.member_to_score.find(m);
            if (mit == it->second.member_to_score.end()) {
                continue;
            }
            double sc = mit->second;
            it->second.member_to_score.erase(mit);
            if (!it->second.useSkipList) {
                auto &vec = it->second.items;
                for (auto vit = vec.begin(); vit != vec.end(); ++vit) {
                    if (vit->first == sc && vit->second == m) {
                        vec.erase(vit);
                        ++removed;
                        break;
                    }
                }
            } else {
                if (it->second.sl->erase(sc, m))
                    ++removed;
            }
        }
        // 如果删除后容器为空, 则删除该 key;
        if (!it->second.useSkipList) {
            if (it->second.items.empty())
                zset_records.erase(it);
        } else {
            if (it->second.sl->size() == 0)
                zset_records.erase(it);
        }
        return removed;
    }

}