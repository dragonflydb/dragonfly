// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/detail/listpack.h"

#include "base/logging.h"

namespace dfly {
namespace detail {

using namespace std;

QList::Entry ListPack::GetEntry(uint8_t* pos) {
  unsigned int slen;
  long long lval;
  uint8_t* vstr = lpGetValue(pos, &slen, &lval);
  return vstr ? QList::Entry(reinterpret_cast<char*>(vstr), slen) : QList::Entry(lval);
}

string ListPack::Pop(QList::Where where) {
  uint8_t* pos = GetFirst(where);
  DCHECK(pos);

  string res = GetEntry(pos).to_string();
  lp_ = lpDelete(lp_, pos, nullptr);
  return res;
}

void ListPack::Push(string_view value, QList::Where where) {
  if (where == QList::HEAD) {
    lp_ = lpPrepend(lp_, (unsigned char*)value.data(), value.size());
  } else {
    lp_ = lpAppend(lp_, (unsigned char*)value.data(), value.size());
  }
}

string ListPack::First(QList::Where where) const {
  uint8_t* pos = GetFirst(where);
  DCHECK(pos);

  return GetEntry(pos).to_string();
}

std::optional<string> ListPack::At(long index) const {
  uint8_t* pos = lpSeek(lp_, index);
  if (!pos)
    return nullopt;

  return GetEntry(pos).to_string();
}

vector<uint32_t> ListPack::Pos(string_view element, uint32_t rank, uint32_t count, uint32_t max_len,
                               QList::Where where) const {
  DCHECK_GT(rank, 0u);
  vector<uint32_t> matches;

  uint8_t* p = GetFirst(where);
  unsigned index = 0;
  while (p && (max_len == 0 || index < max_len)) {
    if (GetEntry(p) == element) {
      if (rank == 1) {
        size_t sz = lpLength(lp_);
        auto k = (where == QList::HEAD) ? index : sz - index - 1;
        matches.push_back(k);
        if (count && matches.size() >= count)
          break;
      } else {
        rank--;
      }
    }
    index++;
    p = (where == QList::HEAD) ? lpNext(lp_, p) : lpPrev(lp_, p);
  }
  return matches;
}

bool ListPack::Insert(string_view pivot, string_view elem, QList::InsertOpt insert_opt) {
  uint8_t* p = lpFirst(lp_);
  while (p) {
    if (GetEntry(p) == pivot) {
      int where = (insert_opt == QList::BEFORE) ? LP_BEFORE : LP_AFTER;
      lp_ = lpInsertString(lp_, (unsigned char*)elem.data(), elem.size(), p, where, nullptr);
      return true;
    }
    p = lpNext(lp_, p);
  }
  return false;
}

unsigned ListPack::Remove(string_view elem, unsigned count, QList::Where where) {
  unsigned removed = 0;
  int64_t ival;

  // try parsing the element into an integer.
  int is_int = lpStringToInt64(elem.data(), elem.size(), &ival);

  auto is_match = [&](const QList::Entry& entry) {
    return is_int ? entry.is_int() && entry.ival() == ival : entry == elem;
  };

  uint8_t* p = GetFirst(where);

  while (p) {
    if (is_match(GetEntry(p))) {
      // lpDelete returns pointer to the element AFTER the deleted one (toward tail)
      lp_ = lpDelete(lp_, p, &p);

      if (where == QList::TAIL) {
        // Iterating backward (from TAIL): need to get the previous element
        if (p) {
          p = lpPrev(lp_, p);
        } else {
          // Deleted the tail element, lpDelete returned nullptr (no element after tail).
          // We need to continue from the new tail to keep moving towards HEAD.
          p = lpLast(lp_);
        }
      }
      // For HEAD direction, 'p' already points to the next element to check

      removed++;
      if (count && removed == count)
        break;
      continue;
    }

    p = (where == QList::HEAD) ? lpNext(lp_, p) : lpPrev(lp_, p);
  }

  return removed;
}

bool ListPack::Replace(long index, string_view elem) {
  uint8_t* p = lpSeek(lp_, index);
  if (!p)
    return false;
  lp_ = lpReplace(lp_, &p, (unsigned char*)elem.data(), elem.size());
  return true;
}

}  // namespace detail
}  // namespace dfly
