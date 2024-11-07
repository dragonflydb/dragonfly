// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/qlist.h"

extern "C" {
#include "redis/zmalloc.h"
}

#include "base/logging.h"

using namespace std;

namespace dfly {

QList::QList() : fill_(-2), compress_(0), bookmark_count_(0) {
}

QList::QList(int fill, int compress) : fill_(fill), compress_(compress), bookmark_count_(0) {
}

QList::~QList() {
  unsigned long len;
  quicklistNode *current, *next;

  current = head_;
  len = len_;
  while (len--) {
    next = current->next;

    zfree(current->entry);
    count_ -= current->count;

    zfree(current);

    len_--;
    current = next;
  }
}

void QList::Push(string_view value, Where where) {
  /* The head and tail should never be compressed (we don't attempt to decompress them) */
  if (head_)
    DCHECK(head_->encoding != QUICKLIST_NODE_ENCODING_LZF);
  if (tail_)
    DCHECK(tail_->encoding != QUICKLIST_NODE_ENCODING_LZF);

  if (where == HEAD) {
    PushHead(value);
  } else {
    DCHECK_EQ(TAIL, where);
    PushTail(value);
  }
}

void QList::AppendListpack(unsigned char* zl) {
}

void QList::AppendPlain(unsigned char* zl) {
}

void QList::Insert(std::string_view pivot, std::string_view elem, InsertOpt opt) {
}

size_t QList::MallocUsed() const {
  // Approximation since does not account for listpacks.
  size_t res = len_ * sizeof(quicklistNode) + znallocx(sizeof(quicklist));
  return res + count_ * 16;  // we account for each member 16 bytes.
}

string QList::Peek(Where where) const {
  return {};
}

optional<string> QList::Get(long index) const {
  return nullopt;
}

void QList::Iterate(IterateFunc cb, long start, long end) const {
}

bool QList::PushHead(string_view value) {
  return false;
}

// Returns false if used existing head, true if new head created.
bool QList::PushTail(string_view value) {
  return false;
}

void InsertPlainNode(quicklistNode* old_node, string_view, bool after) {
}

void InsertNode(quicklist* quicklist, quicklistNode* old_node, quicklistNode* new_node,
                bool after) {
}

}  // namespace dfly
