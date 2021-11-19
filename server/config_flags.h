// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <string.h>
#include <absl/base/macros.h>

#include "base/flags.h"

namespace dfly {

// DashStr - replaces all underscores to dash characters and keeps the rest as is.
template <unsigned N> class DashStr {
 public:
  DashStr(const char* s) {
    memcpy(str_, s, N);
    for (unsigned i = 0; i < N; ++i) {
      if (str_[i] == '_')
        str_[i] = '-';
    }
  }

  const char* str() const {
    return str_;
  }

 private:
  char str_[N];
};

using ConfigEnum = std::pair<const char*, int>;

bool ValidateConfigEnum(const char* nm, const std::string& val, const ConfigEnum* ptr, unsigned len,
                        int* dest);

}  // namespace dfly

inline bool TrueValidator(const char* nm, const std::string& val) {
  return true;
}

#define DEFINE_CONFIG_VAR(type, shorttype, name, value, help, validator)                          \
  namespace fL##shorttype {                                                                       \
    type FLAGS_##name = value;                                                                    \
    static type FLAGS_no##name = value;                                                           \
    static ::dfly::DashStr<sizeof(#name)> _dash_##name(#name);                                   \
    static GFLAGS_NAMESPACE::FlagRegisterer o_##name(                                             \
        _dash_##name.str(), MAYBE_STRIPPED_HELP(help), __FILE__, &FLAGS_##name, &FLAGS_no##name); \
    static const bool name##_val_reg =                                                            \
                         GFLAGS_NAMESPACE::RegisterFlagValidator(&FLAGS_##name, validator);       \
  }                                                                                               \
  using fL##shorttype::FLAGS_##name

#define BIND_CONFIG(var) [](const char* nm, auto val) { \
          var = val;                                    \
          return true;}


#define BIND_ENUM_CONFIG(enum_arr, dest_var) [](const char* nm, const std::string& val) { \
         return ::dfly::ValidateConfigEnum(nm, val, enum_arr, ABSL_ARRAYSIZE(enum_arr),  \
                         &(dest_var));}

#define CONFIG_uint64(name,val, txt, validator) \
   DEFINE_CONFIG_VAR(GFLAGS_NAMESPACE::uint64, U64, name, val, txt, validator)


#define CONFIG_string(name, val, txt, validator)                                       \
  namespace fLS {                                                           \
    using ::fLS::clstring;                                                  \
    using ::fLS::StringFlagDestructor;                                      \
    static union { void* align; char s[sizeof(clstring)]; } s_##name[2];    \
    clstring* const FLAGS_no##name = ::fLS::                                \
                                   dont_pass0toDEFINE_string(s_##name[0].s, \
                                                             val);          \
    static ::dfly::DashStr<sizeof(#name)> _dash_##name(#name);             \
    static GFLAGS_NAMESPACE::FlagRegisterer o_##name(                       \
        _dash_##name.str(), MAYBE_STRIPPED_HELP(txt), __FILE__,             \
        FLAGS_no##name, new (s_##name[1].s) clstring(*FLAGS_no##name));     \
    static StringFlagDestructor d_##name(s_##name[0].s, s_##name[1].s);     \
    extern GFLAGS_DLL_DEFINE_FLAG clstring& FLAGS_##name;                   \
    using fLS::FLAGS_##name;                                                \
    clstring& FLAGS_##name = *FLAGS_no##name;                               \
    static const bool name##_val_reg =                                      \
      GFLAGS_NAMESPACE::RegisterFlagValidator(&FLAGS_##name, validator);    \
  }                                                                         \
  using fLS::FLAGS_##name

#define CONFIG_enum(name, val, txt, enum_arr, dest_var)    \
    CONFIG_string(name, val, txt, BIND_ENUM_CONFIG(enum_arr, dest_var))

