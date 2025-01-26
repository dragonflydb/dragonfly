#include "facade/op_status.h"

#include "base/logging.h"
#include "facade/error.h"
#include "facade/resp_expr.h"

namespace facade {

std::string_view StatusToMsg(OpStatus status) {
  switch (status) {
    case OpStatus::OK:
      return "OK";
    case OpStatus::KEY_NOTFOUND:
      return kKeyNotFoundErr;
    case OpStatus::WRONG_TYPE:
      return kWrongTypeErr;
    case OpStatus::WRONG_JSON_TYPE:
      return kWrongJsonTypeErr;
    case OpStatus::OUT_OF_RANGE:
      return kIndexOutOfRange;
    case OpStatus::INVALID_FLOAT:
      return kInvalidFloatErr;
    case OpStatus::INVALID_INT:
      return kInvalidIntErr;
    case OpStatus::SYNTAX_ERR:
      return kSyntaxErr;
    case OpStatus::OUT_OF_MEMORY:
      return kOutOfMemory;
    case OpStatus::BUSY_GROUP:
      return "-BUSYGROUP Consumer Group name already exists";
    case OpStatus::INVALID_NUMERIC_RESULT:
      return kInvalidNumericResult;
    case OpStatus::AT_LEAST_ONE_KEY:
      return "at least 1 input key is needed for this command";
    case OpStatus::MEMBER_NOTFOUND:
      return kKeyNotFoundErr;
    case OpStatus::INVALID_JSON_PATH:
      return kInvalidJsonPathErr;
    case OpStatus::INVALID_JSON:
      return kJsonParseError;
    default:
      LOG(ERROR) << "Unsupported status " << status;
      return "Internal error";
  }
}

}  // namespace facade
