// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/lru.h"

namespace dfly {

using namespace std;

LRUCache::LRUCache(int _capacity): size(0), capacity(_capacity) {
    head = new DLinkedNode();
    tail = new DLinkedNode();
    head->next = tail;
    tail->prev = head;
}

// TODO: A two-way array linked table might be a better choice.
LRUCache::~LRUCache() {
    for (auto& item : cache) {
        delete item.second;
    }
    delete head;
    delete tail;
}

std::optional<int> LRUCache::RawGet(std::string key) {
    if (!cache.count(key)) {
        return std::nullopt;
    }
    return cache[key]->value;
}

void LRUCache::Put(std::string key, int value) {
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

void LRUCache::addToHead(DLinkedNode* node) {
    node->prev = head;
    node->next = head->next;
    head->next->prev = node;
    head->next = node;
}

void LRUCache::removeNode(DLinkedNode* node) {
    node->prev->next = node->next;
    node->next->prev = node->prev;
}

void LRUCache::moveToHead(DLinkedNode* node) {
    removeNode(node);
    addToHead(node);
}

DLinkedNode* LRUCache::removeTail() {
    DLinkedNode* node = tail->prev;
    removeNode(node);
    return node;
}

};