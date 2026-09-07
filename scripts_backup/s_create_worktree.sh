#!/usr/bin/env bash
set -euo pipefail

show_help() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS] [<branch_name>] <target_dir>

Automates Git worktree creation, submodule initialization, and optional build folder copying/scrubbing.

Omit <branch_name> to create a detached worktree at the current HEAD.

Options:
    -s                   Initialize and update submodules recursively.
    -b, --copy-build <path|default>
                         Copy and scrub a build folder. Use 'default' for './build-dbg'
                         (or './build-opt' with -r).
    -B, --build          Configure and build Dragonfly after creating the worktree.
    -r                   Build in Release mode. Default is Debug mode; implies --build.
    -h, --help           Show this help message.

Examples:
  # Basic worktree creation:
  $(basename "$0") my-feature ../wt-feature

    # Create a detached worktree at the current HEAD:
    $(basename "$0") ../wt-detached

    # Copy a specific build folder without building:
  $(basename "$0") -s -b ./build-opt my-feature ../wt-feature

    # Copy the default Debug build folder without building:
    $(basename "$0") -s -b default my-feature ../wt-feature

    # Build a new Debug worktree:
    $(basename "$0") -s --build my-feature ../wt-feature

    # Copy the default Release build folder and rebuild it in Release mode:
    $(basename "$0") -s -b default -r my-feature ../wt-feature
EOF
}

# Default flag states
SUBMODULES=false
COPY_BUILD=false
EXPLICIT_BUILD_PATH=""
BUILD=false
RELEASE_MODE=false
BRANCH_NAME=""
TARGET_DIR=""
POSITIONAL_ARGS=()

# Parse command line options
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            show_help
            exit 0
            ;;
        -s|--submodule|--submodules)
            SUBMODULES=true
            shift
            ;;
        -b|--copy-build)
            COPY_BUILD=true
            if [[ -z "${2:-}" || "$2" == -* ]]; then
                echo "Error: '$1' requires a build directory path or 'default'." >&2
                show_help
                exit 1
            fi
            EXPLICIT_BUILD_PATH="$2"
            shift 2
            ;;
        -B|--build)
            BUILD=true
            shift
            ;;
        -r|--release)
            BUILD=true
            RELEASE_MODE=true
            shift
            ;;
        -*)
            echo "Error: Unknown option '$1'" >&2
            show_help
            exit 1
            ;;
        *)
            POSITIONAL_ARGS+=("$1")
            shift
            ;;
    esac
done

# Validate required positional arguments
case "${#POSITIONAL_ARGS[@]}" in
    1)
        TARGET_DIR="${POSITIONAL_ARGS[0]}"
        ;;
    2)
        BRANCH_NAME="${POSITIONAL_ARGS[0]}"
        TARGET_DIR="${POSITIONAL_ARGS[1]}"
        ;;
    *)
        echo "Error: Provide <target_dir> and optionally <branch_name>." >&2
        show_help
        exit 1
        ;;
esac

if [[ -e "$TARGET_DIR" ]]; then
    echo "Error: Destination '$TARGET_DIR' already exists. Choose a new path." >&2
    exit 1
fi

if [[ "$TARGET_DIR" == "." || "$TARGET_DIR" == ".." || "$TARGET_DIR" == */. || "$TARGET_DIR" == */.. ]]; then
    echo "Error: Refusing unsafe destination '$TARGET_DIR'." >&2
    exit 1
fi

if [[ -n "$BRANCH_NAME" ]] && ! git check-ref-format --branch "$BRANCH_NAME" >/dev/null 2>&1; then
    echo "Error: '$BRANCH_NAME' is not a valid Git branch name." >&2
    exit 1
fi

# Determine build commands. Keep these explicit instead of relying on aliases
# from an interactive shell; aliases stored in a variable do not expand here.
if [[ "$RELEASE_MODE" == true ]]; then
    BUILD_CONFIG_ARGS=(-release -DUSE_MOLD=ON -DWITH_AWS=OFF)
    BUILD_DIR="build-opt"
else
    BUILD_CONFIG_ARGS=(-DUSE_MOLD=ON -DWITH_AWS=OFF)
    BUILD_DIR="build-dbg"
fi
# Resolve source build directory before changing folders
ABS_BUILD_SRC=""
TARGET_BUILD_DIR_NAME=""

if [[ "$COPY_BUILD" == true ]]; then
    if [[ "$EXPLICIT_BUILD_PATH" == "default" ]]; then
        if [[ "$RELEASE_MODE" == true ]]; then
            EXPLICIT_BUILD_PATH="./build-opt"
        else
            EXPLICIT_BUILD_PATH="./build-dbg"
        fi
    fi

    if [[ ! -d "$EXPLICIT_BUILD_PATH" ]]; then
        echo "Error: Source build directory '$EXPLICIT_BUILD_PATH' does not exist." >&2
        exit 1
    fi

    ABS_BUILD_SRC=$(cd "$EXPLICIT_BUILD_PATH" && pwd)
    TARGET_BUILD_DIR_NAME=$(basename "$ABS_BUILD_SRC")
    if [[ "$TARGET_BUILD_DIR_NAME" == "." || "$TARGET_BUILD_DIR_NAME" == ".." ]]; then
        echo "Error: Build path '$EXPLICIT_BUILD_PATH' has an unsafe directory name." >&2
        exit 1
    fi
fi

echo "🌱 Preparing worktree creation"
if [[ -n "$BRANCH_NAME" ]]; then
    echo "   Branch:       $BRANCH_NAME"
else
    echo "   Branch:       detached at HEAD"
fi
echo "   Destination:  $TARGET_DIR"
echo "   Submodules:   $SUBMODULES"
echo "   Copy build:   $COPY_BUILD"
echo "   Build:        $BUILD"
if [[ "$COPY_BUILD" == true ]]; then
    echo "   Build source: $ABS_BUILD_SRC"
fi

if [[ -z "$BRANCH_NAME" ]]; then
    echo "🔎 Creating detached worktree at the current HEAD."
    git worktree add --detach "$TARGET_DIR" HEAD
elif git rev-parse --verify --quiet "$BRANCH_NAME" >/dev/null 2>&1 || \
   git rev-parse --verify --quiet "origin/$BRANCH_NAME" >/dev/null 2>&1; then
    echo "🔎 Existing branch found; attaching the worktree to '$BRANCH_NAME'."
    git worktree add "$TARGET_DIR" "$BRANCH_NAME"
else
    echo "🌿 Branch not found; creating '$BRANCH_NAME' from the current HEAD."
    git worktree add -b "$BRANCH_NAME" "$TARGET_DIR"
fi

echo "📂 Worktree created. Entering '$TARGET_DIR'."
cd "$TARGET_DIR"

if [[ "$SUBMODULES" == true ]]; then
    echo "📦 Initializing and updating submodules recursively."
    git submodule update --init --recursive
else
    echo "📦 Submodule initialization skipped."
fi

if [[ "$COPY_BUILD" == true && -n "$ABS_BUILD_SRC" ]]; then
    echo "🗄️ Copying build folder '$TARGET_BUILD_DIR_NAME' from '$ABS_BUILD_SRC'."
    cp -r "$ABS_BUILD_SRC" "./$TARGET_BUILD_DIR_NAME"

    echo "🧹 Removing stale CMake and Ninja state from the copied build folder."
    find "./$TARGET_BUILD_DIR_NAME" -name "CMakeCache.txt" -delete
    find "./$TARGET_BUILD_DIR_NAME" -type d -name "CMakeFiles" -exec rm -rf {} +
    rm -f "./$TARGET_BUILD_DIR_NAME/.ninja_log" \
          "./$TARGET_BUILD_DIR_NAME/.ninja_deps" \
          "./$TARGET_BUILD_DIR_NAME/build.ninja"
else
    echo "🗄️ Build folder copy skipped."
fi

if [[ "$BUILD" == true ]]; then
    echo "🚀 Configuring $BUILD_DIR with './helio/blaze.sh ${BUILD_CONFIG_ARGS[*]}'."
    ./helio/blaze.sh "${BUILD_CONFIG_ARGS[@]}"

    echo "🔨 Building the dragonfly target with 'ninja -C $BUILD_DIR -j8 dragonfly'."
    ninja -C "$BUILD_DIR" -j8 dragonfly
else
    echo "🔨 Build skipped."
fi

echo "✅ Worktree setup complete at '$TARGET_DIR'."
