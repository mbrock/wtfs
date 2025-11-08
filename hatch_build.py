"""
Hatchling build hook to bundle ALL cross-compiled wtfs binaries.

Run 'zig build python' first to cross-compile for all platforms.
This creates one universal wheel that works on all platforms!
"""

import shutil
from pathlib import Path
from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    """Build hook that bundles all platform-specific wtfs binaries."""

    def initialize(self, version, build_data):
        """Run before building the package."""
        print("="*60)
        print("Bundling ALL wtfs binaries (cross-platform wheel)...")
        print("="*60)

        source_dir = Path('zig-out/bin')
        dest_dir = Path('wtfs/_bin')
        dest_dir.mkdir(exist_ok=True)

        # Copy all wtfs-* binaries
        platform_binaries = list(source_dir.glob('wtfs-*-*'))

        if not platform_binaries:
            raise RuntimeError(
                "No platform binaries found!\n"
                "Run 'zig build python' to cross-compile for all platforms"
            )

        total_size = 0
        for source in platform_binaries:
            # Skip test binaries
            if 'test' in source.name:
                continue

            dest = dest_dir / source.name
            shutil.copy2(source, dest)
            dest.chmod(0o755)

            size_mb = dest.stat().st_size / 1024 / 1024
            total_size += size_mb
            print(f"  ✓ {source.name:30} ({size_mb:.1f} MB)")

        print(f"\nTotal: {total_size:.1f} MB across {len(platform_binaries)} binaries")
        print("="*60)

