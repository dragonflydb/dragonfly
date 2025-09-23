BUILD_ARCH := $(shell uname -m)
RELEASE_NAME := "dragonfly-${BUILD_ARCH}"
HELIO_RELEASE_FLAGS = -DHELIO_RELEASE_FLAGS="-g"
HELIO_USE_STATIC_LIBS = ON
HELIO_OPENSSL_USE_STATIC_LIBS = ON
HELIO_ENABLE_GIT_VERSION = ON
HELIO_WITH_UNWIND = OFF
RELEASE_DIR=build-release

# Some distributions (old fedora) have incorrect dependencies for crypto
# so we add -lz for them.
LINKER_FLAGS=-lz

# equivalent to: if $(uname_m) == x86_64 || $(uname_m) == amd64
ifneq (, $(filter $(BUILD_ARCH),x86_64 amd64))
HELIO_MARCH_OPT := -march=core2 -msse4.1 -mpopcnt -mtune=skylake
endif

# For release builds we link statically libstdc++ and libgcc. Currently,
# all the release builds are performed by gcc.
LINKER_FLAGS += -static-libstdc++ -static-libgcc

HELIO_FLAGS = -DHELIO_RELEASE_FLAGS="-g" \
			  -DCMAKE_EXE_LINKER_FLAGS="$(LINKER_FLAGS)" \
              -DBoost_USE_STATIC_LIBS=$(HELIO_USE_STATIC_LIBS) \
              -DOPENSSL_USE_STATIC_LIBS=$(HELIO_OPENSSL_USE_STATIC_LIBS) \
              -DENABLE_GIT_VERSION=$(HELIO_ENABLE_GIT_VERSION) \
		      -DWITH_UNWIND=$(HELIO_WITH_UNWIND) -DMARCH_OPT="$(HELIO_MARCH_OPT)"

.PHONY: default

configure:
	cmake -L -B $(RELEASE_DIR) -DCMAKE_BUILD_TYPE=Release -GNinja $(HELIO_FLAGS)

# paired config/build: with and without SimSIMD
RELEASE_DIR_SIMD=build-release-simd

configure-simd:
	cmake -L -B $(RELEASE_DIR_SIMD) -DCMAKE_BUILD_TYPE=Release -GNinja $(HELIO_FLAGS) -DUSE_SIMSIMD=ON

build:
	cd $(RELEASE_DIR); \
	ninja search_test dfly_bench dragonfly && ldd dragonfly

build-simd:
	cd $(RELEASE_DIR_SIMD); \
	ninja search_test dfly_bench dragonfly && ldd dragonfly

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

release-simd: configure-simd build-simd

default: release

# --- Multi-arch release targets (ARM only) ---
.PHONY: configure-armv82 build-armv82 release-armv82 \
		configure-armv9 build-armv9 release-armv9

configure-armv82:
	cmake -L -B build-armv82 -DCMAKE_BUILD_TYPE=Release -GNinja $(HELIO_FLAGS) -DMARCH_OPT="-march=armv8.2-a+fp16+dotprod+rcpc+crypto"

configure-armv82-simd:
	cmake -L -B build-armv82-simd -DCMAKE_BUILD_TYPE=Release -GNinja $(HELIO_FLAGS) -DUSE_SIMSIMD=ON -DMARCH_OPT="-march=armv8.2-a+fp16+dotprod+rcpc+crypto"

build-armv82:
	cd build-armv82; \
	ninja search_test dfly_bench dragonfly && ldd dragonfly

build-armv82-simd:
	cd build-armv82-simd; \
	ninja search_test dfly_bench dragonfly && ldd dragonfly
release-armv82: configure-armv82 build-armv82

release-armv82-simd: configure-armv82-simd build-armv82-simd

configure-armv9:
	cmake -L -B build-armv9 -DCMAKE_BUILD_TYPE=Release -GNinja $(HELIO_FLAGS) -DMARCH_OPT="-march=armv9-a+fp16+dotprod+bf16+i8mm+sve2+crypto"

configure-armv9-simd:
	cmake -L -B build-armv9-simd -DCMAKE_BUILD_TYPE=Release -GNinja $(HELIO_FLAGS) -DUSE_SIMSIMD=ON -DMARCH_OPT="-march=armv9-a+fp16+dotprod+bf16+i8mm+sve2+crypto"

build-armv9:
	cd build-armv9; \
	ninja search_test dfly_bench dragonfly && ldd dragonfly

build-armv9-simd:
	cd build-armv9-simd; \
	ninja search_test dfly_bench dragonfly && ldd dragonfly
release-armv9: configure-armv9 build-armv9

release-armv9-simd: configure-armv9-simd build-armv9-simd
