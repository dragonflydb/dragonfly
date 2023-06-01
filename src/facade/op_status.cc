#include "facade/op_status.h"

namespace facade {

const char* DebugString(OpStatus op) {
  switch (op) {
    case OpStatus::OK:
      return "OK";
    case OpStatus::KEY_EXISTS:
      return "KEY EXISTS";
    case OpStatus::KEY_NOTFOUND:
      return "KEY NOTFOUND";
    case OpStatus::SKIPPED:
      return "SKIPPED";
    case OpStatus::INVALID_VALUE:
      return "INVALID VALUE";
    case OpStatus::OUT_OF_RANGE:
      return "OUT OF RANGE";
    case OpStatus::WRONG_TYPE:
      return "WRONG TYPE";
    case OpStatus::TIMED_OUT:
      return "TIMED OUT";
    case OpStatus::OUT_OF_MEMORY:
      return "OUT OF MEMORY";
    case OpStatus::INVALID_FLOAT:
      return "INVALID FLOAT";
    case OpStatus::INVALID_INT:
      return "INVALID INT";
    case OpStatus::SYNTAX_ERR:
      return "INVALID SYNTAX";
    case OpStatus::BUSY_GROUP:
      return "BUSY GROUP";
    case OpStatus::STREAM_ID_SMALL:
      return "STREAM ID TO SMALL";
    case OpStatus::ENTRIES_ADDED_SMALL:
      return "ENTRIES ADDED IS TO SMALL";
    case OpStatus::INVALID_NUMERIC_RESULT:
      return "INVALID NUMERIC RESULT";
    case OpStatus::CANCELLED:
      return "CANCELLED";
  }
  return "Unknown Error Code";  // we should not be here, but this is how enums works in c++
}
const char* OpResultBase::DebugFormat() const {
  return DebugString(st_);
}

}  // namespace facade
