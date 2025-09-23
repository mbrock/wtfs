#!/usr/bin/env bash
set -Exeuo pipefail
trap 'echo "💥 Error on line $LINENO"; exit 1' ERR

# --- Config -------------------------------------------------------------------
DEST="${HOME}/src/darling"
REPO="https://github.com/darlinghq/darling.git"
CCACHE_SIZE="20G"

# CMake configuration flags (safe to tweak)
CMAKE_COMMON_ARGS=(
  -G Ninja
  -DCMAKE_BUILD_TYPE=RelWithDebInfo
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
  -DCMAKE_COLOR_DIAGNOSTICS=ON

  # Use clang-20 + ccache persistently in the CMake cache:
  -DCMAKE_C_COMPILER=clang-20
  -DCMAKE_CXX_COMPILER=clang++-20
  -DCMAKE_C_COMPILER_LAUNCHER=ccache
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache

  -DDSYMUTIL_EXECUTABLE=/usr/bin/dsymutil-20

  # Perf + debuginfo sweet spot:
  '-DCMAKE_C_FLAGS_RELWITHDEBINFO=-O2 -g -fno-omit-frame-pointer'
  '-DCMAKE_CXX_FLAGS_RELWITHDEBINFO=-O2 -g -fno-omit-frame-pointer'
  '-DCMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO='
  '-DCMAKE_SHARED_LINKER_FLAGS_RELWITHDEBINFO='

  # Darling knobs you mentioned:
  -DTARGET_i386=OFF
  -DCOMPONENTS=system
)

# --- Helpers ------------------------------------------------------------------
need() { command -v "$1" >/dev/null 2>&1; }

# --- Packages (safe to re-run) ------------------------------------------------
sudo apt-get install -y \
  cmake automake bison flex pkg-config git git-lfs ninja-build ccache \
  clang-20 lld-20 \
  libbsd-dev libfuse-dev libudev-dev libxml2-dev libelf-dev libssl-dev libdbus-1-dev \
  g++-14 libstdc++-14-dev libgcc-14-dev

# --- ccache persistent config -------------------------------------------------
ccache --set-config max_size="${CCACHE_SIZE}"
ccache --set-config compression=true
# (Optional) show stats each run
ccache -s || true

# --- Clone or update repo -----------------------------------------------------
mkdir -p "$(dirname "$DEST")"
if [ -d "$DEST/.git" ]; then
  # ensure it's the repo we expect
  current_remote="$(git -C "$DEST" config --get remote.origin.url || true)"
  if [ "$current_remote" != "$REPO" ]; then
    echo "❌ $DEST is a git repo but remote is '$current_remote' (expected '$REPO')." >&2
    exit 1
  fi
#  echo "➡️  Updating existing repo in $DEST"
#  git -C "$DEST" fetch --recurse-submodules --tags --prune --jobs=0
  # rebase fast-forward if possible; autostash for local changes
#  git -C "$DEST" pull --rebase --autostash
#  git -C "$DEST" submodule update --init --recursive --jobs=8
else
  # allow cloning into empty dir; if non-empty and not a git repo, bail
  if [ -d "$DEST" ] && [ "$(ls -A "$DEST")" ]; then
    echo "❌ $DEST exists and is not a git repo; refusing to overwrite." >&2
    exit 1
  fi
  echo "⬇️  Cloning $REPO into $DEST"
  GIT_CLONE_PROTECTION_ACTIVE=false git clone --recursive "$REPO" "$DEST" --jobs=0
fi

# --- Configure (idempotent with -S/-B) ----------------------------------------
BUILD_DIR="$DEST/build"
cmake -S "$DEST" -B "$BUILD_DIR" "${CMAKE_COMMON_ARGS[@]}"

# --- Build --------------------------------------------------------------------
cmake --build "$BUILD_DIR" -j -- -d stats

echo "✅ Build complete. Compile commands: $BUILD_DIR/compile_commands.json"
