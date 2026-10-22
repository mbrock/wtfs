# Building

Requires Zig 0.17.0 (or `nix develop`).

```bash
zig build                      # debug build: zig-out/bin/wtfs
zig build -Doptimize=safe      # optimized, with safety checks (what releases use)
zig build test                 # unit tests
zig build release              # zig-out/bin/wtfs-{linux,macos}-{x86_64,aarch64}
```

`zig build release` cross-compiles every release target from any host. The
Linux binaries use raw syscalls and no libc, so they are fully static and run
on any Linux kernel from 4.11 (statx) on, including NixOS. The macOS binaries
link libSystem, as all macOS programs must.

## Releases

Pushing a tag `vX.Y.Z` runs `.github/workflows/release.yml`, which builds the
release binaries, smoke-tests them on Linux x86_64, Linux aarch64 and macOS,
and attaches them with a `SHA256SUMS` file to a GitHub release. The tag must
match `.version` in `build.zig.zon`.

## Nix

```bash
nix build              # packages.default: the native wtfs binary
nix build .#static     # Linux: the same binary, fully static
nix flake check        # builds, runs the unit tests and a du comparison
nix run . -- -x /home
```

## Python wheel

The Python TUI in `wtfs/` runs the binary through `--binary-output`. To build
one wheel containing all four binaries:

```bash
zig build release
uv build --wheel
```

The build hook (`hatch_build.py`) copies `zig-out/bin/wtfs-*-*` into the wheel,
and `wtfs/scanner.py` picks the right one at runtime.
