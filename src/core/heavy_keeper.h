// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <xxhash.h>

#include <cmath>
#include <algorithm>
#include <functional>
#include <iostream>
#include <string>
#include <optional>
#include <unordered_map>


#include "base/flags.h"
#include "base/hash.h"


ABSL_FLAG(float, hk_b, 1.08, "For the estimation of exponential-weakening decay.");
ABSL_FLAG(int32_t, hk_threshold, 1000,
    "If estimate is equal to hk_threshold, we record the key in the LRU.");

namespace dfly {

using namespace std;
using absl::GetFlag;

constexpr uint32_t hk_wide = 2;
// https://planetmath.org/goodhashtableprimes
// 1. hk_length selects prime numbers to avoid hash conflicts to the maximum extent possible.
// 2. The size of hk is close to 80kb, and the estimation accuracy of hotkey is near-optimal.
constexpr uint32_t hk_length = 3079;

struct DLinkedNode {
    std::string key;
    int value;
    DLinkedNode* prev;
    DLinkedNode* next;
    DLinkedNode(): value(0), prev(nullptr), next(nullptr) {}
    DLinkedNode(std::string _key, int _value): key(_key), value(_value), prev(nullptr), next(nullptr) {}
};

class LRUCache {
private:
    DLinkedNode* head;
    DLinkedNode* tail;
    int size;
    int capacity;
    std::unordered_map<std::string, DLinkedNode*> cache;
public:

    explicit LRUCache(int _capacity): size(0), capacity(_capacity) {
        head = new DLinkedNode();
        tail = new DLinkedNode();
        head->next = tail;
        tail->prev = head;
    }

    // TODO: Need to destructure LRU.
    // TODO: A two-way array linked table might be a better choice.
    ~LRUCache() {

    }
    
    int Length() const {
        return size;
    }

    std::optional<int> RawGet(std::string key) {
        if (!cache.count(key)) {
            return std::nullopt;
        }
        return cache[key]->value;
    }

    void Put(std::string key, int value) {
        if (!cache.count(key)) {
            DLinkedNode* node = new DLinkedNode(key, value);
            cache[key] = node;
            addToHead(node);
            ++size;
            if (size > capacity) {
                DLinkedNode* removed = removeTail();
                cache.erase(removed->key);
                delete removed;
                --size;
            }
        } else {
            DLinkedNode* node = cache[key];
            node->value = value;
            moveToHead(node);
        }
    }

    const std::unordered_map<std::string, DLinkedNode*>& Cache() const {
        return cache;
    }

private:
    void addToHead(DLinkedNode* node) {
        node->prev = head;
        node->next = head->next;
        head->next->prev = node;
        head->next = node;
    }
    
    void removeNode(DLinkedNode* node) {
        node->prev->next = node->next;
        node->next->prev = node->prev;
    }

    void moveToHead(DLinkedNode* node) {
        removeNode(node);
        addToHead(node);
    }

    DLinkedNode* removeTail() {
        DLinkedNode* node = tail->prev;
        removeNode(node);
        return node;
    }
};

// https://www.usenix.org/system/files/conference/atc18/atc18-gong.pdf
class HeavyKeeper {
private:
    struct hkNode {
        int count;
        uint64_t fingerprint;
    } HK[hk_wide][hk_length];

    struct showNode {
        std::string key;
        int count;
    };

    int topK;
    std::unique_ptr<struct showNode[]> hotkey;
    // Least recently used hotkeys will be eliminated when the
    // actual number of hotkeys is greater than the set topk.
    std::unique_ptr<LRUCache> lru;
public:
    explicit HeavyKeeper(int _topK) : topK(_topK) { 
        lru = std::make_unique<LRUCache>(topK);
        hotkey = std::make_unique<showNode[]>(topK);
    }

    void Clear() {
        for (size_t i = 0; i < hk_wide; i++) {
            for (size_t j = 0; j < hk_length; j++) {
                HK[i][j].count = 0;
                HK[i][j].fingerprint = 0;
            }
        }
        // memset(HK, 0, sizeof(HK));
    }

    uint64_t Hash(const std::string& key) {
        return XXH3_64bits_withSeed(key.data(), key.size(), 24061983);
    }

    void Insert(const std::string& key) {
        bool exist = false;
        // RawGet does not make changes to the key in the LRU.
        std::optional<int> key_count = lru->RawGet(key);
        if (key_count) {
            exist = true;
        }

        int estimate = 0;
        uint64_t FP = Hash(key);
        for (size_t j = 0; j < hk_wide; j++) {
            int hash_code = FP%(hk_length-2*(j+hk_wide)+3);
            int count = HK[j][hash_code].count;
            if (HK[j][hash_code].fingerprint == FP) {
                // The key is not found in the LRU, but the hash collision causes the 
                // FP to be the same, and the count is not increment at this time.
                if (exist || count <= GetFlag(FLAGS_hk_threshold)) {
                    HK[j][hash_code].count++;
                }
                estimate = std::max(estimate, HK[j][hash_code].count);
            } else {
                // exponential-weakening decay.
                if (!(rand()%int(std::pow(GetFlag(FLAGS_hk_b), HK[j][hash_code].count)))) {
                    HK[j][hash_code].count--;
                    if (HK[j][hash_code].count <= 0) {
                        HK[j][hash_code].fingerprint = FP;
                        HK[j][hash_code].count = 1;
                        estimate = std::max(estimate, 1);
                    }
                }
            }
        }

        if (!exist) {
            // Eliminate the oldest hotkey, always the newest hotkey in LRU.
            if (estimate - GetFlag(FLAGS_hk_threshold) == 1) {
                lru->Put(key, estimate);
            }
            // estimate greater than hk_threshold but the key is not in LRU
            // does not add this key.
        } else if (estimate > key_count) {
            lru->Put(key, estimate);
        }
    }

    // Time complexity is topK*log(topK), used when aggregating data.
    void Work() {
        static auto compare = [](showNode lhs, showNode rhs) {
            return lhs.count > rhs.count;
        };
        int sum = 0;
        auto& cache = lru->Cache();
        for (const auto& item : cache) {
            hotkey[sum].key = item.second->key;
            hotkey[sum].count = item.second->value;
            sum++; 
        }
        std::sort(hotkey.get(), hotkey.get()+sum, compare);
    }

    std::pair<std::string, int> Query(int index) {
        return std::make_pair(hotkey[index].key, hotkey[index].count);
    }
};

}