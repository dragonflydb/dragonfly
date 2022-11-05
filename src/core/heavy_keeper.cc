// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <xxhash.h>
#include <absl/container/flat_hash_map.h>

#include <cmath>
#include <optional>

#include "core/heavy_keeper.h"

namespace dfly {

using namespace std;

uint64_t HeavyKeeper::Hash(const std::string& key) {
    return XXH3_64bits_withSeed(key.data(), key.size(), 24061983);
}

HeavyKeeper::HeavyKeeper(int _topK, int32_t _hk_threshold, float _hk_b)
    : topK(_topK), hk_threshold(_hk_threshold), hk_b(_hk_b) { 
    lru = std::make_unique<LRUCache>(topK);
    hotkey = std::make_unique<showNode[]>(topK);
}

void HeavyKeeper::Clear() {
    memset(HK, 0, sizeof(HK));
}

void HeavyKeeper::Insert(const std::string& key) {
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
            if (exist || count <= hk_threshold) {
                HK[j][hash_code].count++;
            }
            estimate = std::max(estimate, HK[j][hash_code].count);
        } else {
            // exponential-weakening decay.
            if (!(rand()%int(std::pow(hk_b, HK[j][hash_code].count)))) {
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
        if (estimate - hk_threshold == 1) {
            lru->Put(key, estimate);
        }
        // estimate greater than hk_threshold but the key is not in LRU
        // does not add this key.
    } else if (estimate > key_count) {
        lru->Put(key, estimate);
    }
}

// Time complexity is topK*log(topK), used when aggregating data.
void HeavyKeeper::SortHotKey() {
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

std::optional<std::pair<std::string, int>> HeavyKeeper::Query(int index) {
    if (index >= topK) {
        return std::nullopt;
    }
    return std::make_pair(hotkey[index].key, hotkey[index].count);
}

};