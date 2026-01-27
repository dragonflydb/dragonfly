BUILD_ARCH := $(shell uname -m)
RELEASE_NAME := "dragonfly-${BUILD_ARCH}"
HELIO_RELEASE_FLAGS = -DHELIO_RELEASE_FLAGS="-g"
HELIO_USE_STATIC_LIBS = ON
HELIO_OPENSSL_USE_STATIC_LIBS = ON
HELIO_ENABLE_GIT_VERSION = ON
HELIO_WITH_UNWIND ?= OFF
RELEASE_DIR=build-release
WITH_SIMSIMD ?= ON

# Some distributions (old fedora) have incorrect dependencies for crypto
# so we add -lz for them.
LINKER_FLAGS=-lz

# equivalent to: if $(uname_m) == x86_64 || $(uname_m) == amd64
# Override HELIO_MARCH_OPT via environment: make HELIO_MARCH_OPT="-march=native"
#
# Universal cloud-optimized compiler flags:
# - x86-64-v3: Modern baseline for all current cloud instances (2013+ Haswell)
#              Includes: SSE4.2, POPCNT, AVX, AVX2, FMA, BMI1, BMI2, F16C
# - neoverse-n1: Optimized for all current ARM cloud instances (ARMv8.2-A)
#                 Includes: fp16, dotprod, crypto, CRC32 (great for vector ops)
#                 Compatible with Graviton 2/3/4, Azure Ampere, GCloud T2A
ifneq (, $(filter $(BUILD_ARCH),x86_64 amd64))
HELIO_MARCH_OPT ?= -march=x86-64-v3 -mtune=generic
endif

# ARM/aarch64 optimization for cloud deployments (AWS Graviton, Azure Ampere, GCloud T2A)
ifneq (, $(filter $(BUILD_ARCH),aarch64 arm64))
HELIO_MARCH_OPT ?= -march=neoverse-n1 -mtune=neoverse-n1
endif

# For release builds we link statically libstdc++ and libgcc. Currently,
# all the release builds are performed by gcc.
LINKER_FLAGS += -static-libstdc++ -static-libgcc

HELIO_FLAGS = -DHELIO_RELEASE_FLAGS="-g" \
			  -DCMAKE_EXE_LINKER_FLAGS="$(LINKER_FLAGS)" \
              -DBoost_USE_STATIC_LIBS=$(HELIO_USE_STATIC_LIBS) \
              -DOPENSSL_USE_STATIC_LIBS=$(HELIO_OPENSSL_USE_STATIC_LIBS) \
              -DENABLE_GIT_VERSION=$(HELIO_ENABLE_GIT_VERSION) \
              -DWITH_SIMSIMD=$(WITH_SIMSIMD) \
              -DWITH_UNWIND=$(HELIO_WITH_UNWIND) -DMARCH_OPT="$(HELIO_MARCH_OPT)"

.PHONY: default

configure:
	cmake -L -B $(RELEASE_DIR) -DCMAKE_BUILD_TYPE=Release -GNinja $(HELIO_FLAGS)

build:
	cd $(RELEASE_DIR); \
	ninja dfly_bench dragonfly && ldd dragonfly

package:
	cd $(RELEASE_DIR); \
	tar cvfz $(RELEASE_NAME)-dbgsym.tar.gz dragonfly ../LICENSE.md; \
	objcopy \
		--remove-section=".debug_*" \
		--remove-section="!.debug_line" \
		--compress-debug-sections \
		dragonfly \
		$(RELEASE_NAME); \
	tar cvfz $(RELEASE_NAME).tar.gz $(RELEASE_NAME) ../LICENSE.md; \
	objcopy \
		--remove-section=".debug_*" \
		--remove-section="!.debug_line" \
		--compress-debug-sections \
		dfly_bench \
		dfly_bench-$(BUILD_ARCH); \
	tar cvfz dfly_bench-$(BUILD_ARCH).tar.gz dfly_bench-$(BUILD_ARCH)

release: configure build

default: release
