#!/usr/bin/env bash
# Flag matrix: prove which UBSan/compiler config catches which class of bug.
# Each row prints PASS (caught) or MISS (silent) per (compiler, flag-set, case).
set -u
SRC="$(dirname "$0")/repro_all.cc"
CC_GCC="g++"
CC_CLANG="clang++"

# (label, flags)
CONFIGS=(
  "clang+ubsan-default::$CC_CLANG:-fsanitize=undefined"
  "gcc+ubsan-default::$CC_GCC:-fsanitize=undefined"
  "clang+impl-conversion::$CC_CLANG:-fsanitize=undefined,implicit-conversion"
  "clang+integer-group::$CC_CLANG:-fsanitize=undefined,integer"
  "clang+full-recommended::$CC_CLANG:-fsanitize=undefined,implicit-conversion,integer,local-bounds,nullability,float-divide-by-zero"
)
CASES=(1 2 3 4 5 6)
LABELS=(
  "1 unsigned-trunc (PR#7562)"
  "2 signed-trunc"
  "3 sign-change"
  "4 signed-overflow"
  "5 unsigned-overflow"
  "6 char-overflow"
)

printf "\n%-30s | " "config"
for L in "${LABELS[@]}"; do printf "%-26s " "$L"; done
echo
printf -- "-%.0s" {1..200}; echo

for spec in "${CONFIGS[@]}"; do
  label="${spec%%::*}"; rest="${spec#*::}"
  compiler="${rest%%:*}";  flags="${rest#*:}"
  bin="/tmp/ubsan_poc/build_${label// /_}"
  if ! "$compiler" -O0 -g $flags -fno-sanitize-recover=all "$SRC" -o "$bin" 2>/dev/null; then
    printf "%-30s | %s\n" "$label" "(build failed: unsupported flag)"
    continue
  fi
  printf "%-30s | " "$label"
  for c in "${CASES[@]}"; do
    out="$("$bin" "$c" 2>&1)"
    if echo "$out" | grep -q 'runtime error'; then
      printf "%-26s " "PASS  (caught)"
    else
      printf "%-26s " "MISS  (silent)"
    fi
  done
  echo
done
