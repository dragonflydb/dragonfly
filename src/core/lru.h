// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>

namespace dfly {

using namespace std;

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
    absl::flat_hash_map<std::string, DLinkedNode*> cache;
public:

    explicit LRUCache(int _capacity);
    ~LRUCache();

    std::optional<int> RawGet(std::string key);
    void Put(std::string key, int value);
    const absl::flat_hash_map<std::string, DLinkedNode*>& Cache() const {
         return cache;
    }
    int Length() const { return size;}

private:
    void addToHead(DLinkedNode* node);
    void removeNode(DLinkedNode* node);
    void moveToHead(DLinkedNode* node);
    DLinkedNode* removeTail();
};

};