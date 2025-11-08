"""
Build hook to compile the wtfs binary during package build.

This runs 'zig build' and copies the binary into the package.
"""

import subprocess
import shutil
from pathlib import Path


def build_binary():
    """Build the wtfs binary with Zig."""
    print("Building wtfs binary with Zig...")

    # Run zig build
    result = subprocess.run(
        ['zig', 'build', '-Doptimize=ReleaseFast'],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"Zig build failed:\n{result.stderr}")
        raise RuntimeError("Failed to build wtfs binary")

    print("✓ Zig build completed")

    # Copy binary to package
    binary_name = 'wtfs.exe' if Path.cwd().drive else 'wtfs'
    source = Path('zig-out/bin') / binary_name
    dest_dir = Path('wtfs/_bin')
    dest_dir.mkdir(exist_ok=True)
    dest = dest_dir / binary_name

    if not source.exists():
        raise RuntimeError(f"Built binary not found at {source}")

    shutil.copy2(source, dest)
    dest.chmod(0o755)

    print(f"✓ Binary copied to {dest}")


if __name__ == '__main__':
    build_binary()

