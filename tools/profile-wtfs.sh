#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<USAGE
Usage: $(basename "$0") [--release] [--binary ./zig-out/bin/wtfs] [--] [wtfs args...]

Runs wtfs under Linux perf sampling. Captures perf.data and emits perf report hints.

  --release        Build the release-fast variant before profiling.
  --binary PATH    Path to the wtfs executable (default: ./zig-out/bin/wtfs).
  --help           Show this help message.

Examples:
  $(basename "$0") --release -- /var/log
USAGE
}

build_mode=debug
binary="./zig-out/bin/wtfs"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --release)
            build_mode=release
            shift
            ;;
        --binary)
            binary="$2"
            shift 2
            ;;
        --help)
            usage
            exit 0
            ;;
        --)
            shift
            break
            ;;
        -* )
            echo "Unknown option: $1" >&2
            usage
            exit 1
            ;;
        * )
            break
            ;;
    esac
done

wtfs_args=("$@")

if [[ ! -x "$binary" || "$build_mode" == "release" ]]; then
    if [[ "$build_mode" == "release" ]]; then
        zig build --release=fast
        binary="./zig-out/bin/wtfs"
    else
        zig build
    fi
fi

if [[ ! -x "$binary" ]]; then
    echo "error: wtfs binary not found at $binary" >&2
    exit 1
fi

perf_data="perf.data"
rm -f "$perf_data"

echo "[wtfs-profile] Running perf record..."
perf record --call-graph dwarf --output "$perf_data" "$binary" "${wtfs_args[@]}"

cat <<REPORT

[wtfs-profile] perf.data captured.
Suggested next steps:
  perf report --input $perf_data
  perf script --input $perf_data > perf.txt
REPORT
