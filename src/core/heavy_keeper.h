// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <string>
#include <memory>

#include "core/lru.h"

namespace dfly {

using namespace std;

inline constexpr uint32_t hk_wide = 2;
// https://planetmath.org/goodhashtableprimes
// 1. hk_length selects prime numbers to avoid hash conflicts to the maximum extent possible.
// 2. The size of hk is close to 80kb, and the estimation accuracy of hotkey is near-optimal.
inline constexpr uint32_t hk_length = 3079;

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

    const int topK;               // We want to track the top hotkeys in terms of number of visitors.
    const int32_t hk_threshold;   // If estimate is equal to hk_threshold, we record the key in the LRU.
    const float hk_b;             // For the estimation of exponential-weakening decay.

    // Used to aggregate hotkey.
    std::unique_ptr<struct showNode[]> hotkey;
    // Least recently used hotkeys will be eliminated when the
    // actual number of hotkeys is greater than the set topk.
    std::unique_ptr<LRUCache> lru;

    uint64_t Hash(const std::string& key);
public:
    explicit HeavyKeeper(int _topK, int32_t _hk_threshold = 1000, float _hk_b = 1.08);
    void Clear();
    void Insert(const std::string& key);
    void SortHotKey();
    std::optional<std::pair<std::string, int>> Query(int index);
};

}