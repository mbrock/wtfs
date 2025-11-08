# Design Notes

## Python Integration Approach

### The Right Way: Subprocess

The `wtfs.Scanner` class uses the compiled `wtfs` binary via subprocess. This is the recommended approach because:

1. **I/O Bound Work**: Filesystem scanning is I/O bound, so subprocess overhead (~1-5ms) is negligible
2. **Full Threading**: The binary uses Zig's thread pool without shared library issues
3. **Process Isolation**: Can't crash the Python interpreter
4. **Simplicity**: Clean interface, no C ABI complexity
5. **Battle-Tested**: Uses the same code as the CLI

### The Explored Path: Native Library

We explored creating a shared library (`.so`) with a C ABI for direct calls from Python:

- ✅ Created `src/scanner_lib.zig` with C-compatible exports
- ✅ Built with both ctypes and CFFI
- ✅ Made `performScan()` public for library use
- ❌ **Threading Issue**: Zig's `Thread.Pool` has issues when used in a shared library loaded by Python

#### The Threading Problem

When the scanner library was loaded by Python and created worker threads:

- **Crash Location**: `Thread.Condition.FutexImpl.wait()` with SIGSEGV
- **Fault Address**: `0x20` (32 bytes), suggesting near-null pointer dereference
- **What We Found**: The `Thread.Pool` on the stack in `gatherPhase()` had timing issues where worker threads would start before full initialization
- **Why .so Failed**: Different execution environment (stack layout, timing, scheduling) in dynamically loaded library vs standalone executable

#### Solution: Single-Threaded Mode

Setting `.single_threaded = true` in the build configuration works perfectly, but then we lose the parallel scanning benefit, making the native library approach less compelling than just using the multi-threaded binary via subprocess.

### Files

- `wtfs/scanner.py` - Subprocess-based Scanner (recommended)
- `src/scanner_lib.zig` - Native C ABI (kept for reference)
- `test_scanner_cffi.py` - CFFI test (works with single-threaded)
- `test_cffi_crash.py` - Demonstrates the threading crash
- `docs/INTEGRATION_ROADMAP.md` - Original native library plan

### Lessons Learned

1. Don't overcomplicate: subprocess is fine for I/O-bound work
2. Shared libraries have different constraints than executables
3. Zig's threading works great, but shared library contexts are tricky
4. The "clever" solution isn't always the right solution
5. Sometimes the simple answer is the best answer 🎯
