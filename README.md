# wtfs

Find out where disk space went. `wtfs` walks a directory tree in parallel and
either prints a plain-text summary or writes **every entry as one row of a
table** (Parquet or Arrow) that you can explore with SQL in DuckDB.

It was built for the case where a shared build server fills up (`/home` and
`/nix` at 100%) and you, or an AI agent, need to find out why quickly and
reliably.

- **Fast:** parallel `getdents64` + `statx` on Linux, `getattrlistbulk` on macOS.
  A million files take about 0.2 s warm on 8 cores; a cold 82,000-entry
  `/nix` took 0.14 s against 0.43 s for `du`.
- **Agrees with `du`:** sizes are bytes allocated on disk, and hardlinked
  files are counted once, so the total equals `du -sB1` (`du -sxB1` with `-x`).
- **Complete data:** full paths, never truncated; inode, link count, owner,
  mode and mtime for every entry.
- **One static binary:** the Linux builds have no libc dependency.

## Install

```bash
# Static binary from the latest GitHub release (linux-x86_64, linux-aarch64, macos-aarch64, macos-x86_64)
curl -fsSLo wtfs https://github.com/mbrock/wtfs/releases/latest/download/wtfs-linux-x86_64
chmod +x wtfs

# Nix
nix run github:mbrock/wtfs -- -x /home
nix profile install github:mbrock/wtfs

# From source (Zig 0.17.0)
zig build -Doptimize=safe && ./zig-out/bin/wtfs --help
```

## Quick start

```bash
wtfs -x /                          # summary of the root filesystem
sudo wtfs -x --deleted /           # also deleted-but-open files, for all users
wtfs -o /tmp/home.parquet /home    # summary + full table for SQL
duckdb -c "FROM '/tmp/home.parquet' WHERE kind = 'dir' ORDER BY tree_du DESC LIMIT 20"
```

Run as root to see into every user's directories; unreadable directories are
listed under `## errors` and in the `error` column, and the totals then
undercount just like `du` would.

## Text output

Header lines start with `#`. Data lines are `DU_BYTES  HUMAN  [ENTRIES]  PATH`
with the full path last, so `awk '{print $1}'`, `sort -n` and `grep` work.
Control characters and invalid UTF-8 in paths are escaped as `\xNN` (and `\`
as `\\`) so that one entry is always one line.

```
$ sudo wtfs -x --deleted -n 5 /
# wtfs /
# du 13606984192 bytes (12.7GiB); apparent size 11192082480 bytes (10.4GiB); hardlinked copies not counted 4955936768 bytes (4.6GiB)
# 1295739 entries: 1214396 files, 72271 dirs, 9071 symlinks, 1 other; 0 errors; 5 mount points not scanned; 0.33s

## children of / by du (top 5 of 25)
#          du_bytes   du_human     entries  path
         4560932352     4.2GiB      180982  /nix
         4230305792     3.9GiB     1020258  /tmp
         2843454464     2.6GiB       54921  /usr
         1764178944     1.6GiB       34900  /home
          115085824   109.8MiB         132  /root

## largest directories by du, at any depth (top 5)
#          du_bytes   du_human     entries  path
         4560932352     4.2GiB      180982  /nix
         4421660160     4.1GiB      180931  /nix/store
         4230305792     3.9GiB     1020258  /tmp
         3988893696     3.7GiB     1020201  /tmp/big
         2843454464     2.6GiB       54921  /usr

## largest files by du (top 5)
#          du_bytes   du_human  path
          300847104   286.9MiB  /home/user/.agent-browser/browsers/chrome-156.0.8078.47/chrome
          237826048   226.8MiB  /nix/store/34wy21hkax0k707757pbgd77fjxdv6v5-zig-0.17.0/bin/zig
          203038720   193.6MiB  /nix/store/nsgmh9gxbbpg3cla9njjh9qr3akh3f98-llvm-22.1.8-lib/lib/libLLVM.so.22.1
          192081920   183.2MiB  /nix/store/mr6cbi3rdy6w5y4cd6875gf4y0pqn0jf-llvm-21.1.8-lib/lib/libLLVM.so.21.1
          185516032   176.9MiB  /home/user/opt/zig-x86_64-linux-0.17.0/zig

## deleted files still held open: 1, 67108864 bytes (64.0MiB)
#          du_bytes   du_human  path  <-  open by
           67108864    64.0MiB  /tmp/agent-log.txt  <-  pid 13953 (sleep) fd 4
```

## Options

```
-o, --output FILE     also write every entry as a table: *.parquet or *.arrow
-n, --top N           lines per summary section (default 20; 0 = totals only)
-x, --one-file-system do not descend into other filesystems
    --deleted         also list deleted files still held open on the scanned filesystems (Linux, /proc)
-j, --threads N       worker threads (default: number of CPUs)
-q, --quiet           no progress display on stderr
```

Pseudo filesystems (`/proc`, `/sys`, cgroup, devpts, ...) are never descended
into; they show up as rows with `error = 'not scanned: pseudo filesystem'`.

## Table schema

One row per filesystem entry: the root, every directory, file, symlink, FIFO,
socket and device below it, plus (with `--deleted`) one row per deleted file
still held open.

| column | type | meaning |
|---|---|---|
| `path` | VARCHAR | full path, starting with the root exactly as given on the command line |
| `kind` | VARCHAR | `file`, `dir`, `symlink`, `fifo`, `socket`, `block`, `char`, `unknown`, or `deleted` |
| `size` | BIGINT | apparent size in bytes (`st_size`) |
| `alloc` | BIGINT | bytes allocated on disk (`st_blocks * 512`) |
| `du` | BIGINT | bytes charged to this path: `alloc`, or 0 if the same inode was already charged to another path |
| `tree_du` | BIGINT | sum of `du` over this entry and everything below it (for files, equals `du`) |
| `tree_count` | BIGINT | number of entries in this subtree, including itself |
| `id` | BIGINT | row number; the root is 0 and every parent has a smaller id than its children |
| `parent` | BIGINT | `id` of the containing directory; NULL for the root and for `deleted` rows |
| `depth` | UINTEGER | 0 for the root, 1 for its children, ... |
| `dev` | BIGINT | device id (Linux: `major << 32 \| minor`) |
| `ino` | BIGINT | inode number |
| `nlink` | UINTEGER | hard link count |
| `uid`, `gid` | UINTEGER | owner |
| `mode` | UINTEGER | `st_mode` (type and permission bits) |
| `mtime` | TIMESTAMPTZ | modification time |
| `error` | VARCHAR | why a directory was not (fully) read, e.g. `permission denied`, `not scanned: other filesystem (-x)`; else NULL |
| `open_by` | VARCHAR | for `deleted` rows: `pid N (comm) fd M, ...` |

Invariants: `sum(du) WHERE kind <> 'deleted'` equals the root's `tree_du`, which
equals `du -sB1 PATH`. When an inode has several paths, its space is charged to
one of them deterministically: a path outside `/nix/store/.links` (where
`nix-store --optimise` keeps its pool) if there is one, otherwise the
lexicographically smallest path. `du` charges whichever it reaches first, so
per-directory numbers can differ from `du`, but the totals do not. `size` and
`alloc` are never deduplicated.

The `path` column is the raw path when it is valid UTF-8; otherwise invalid
bytes are written as `\xNN` and backslashes as `\\`.

## Questions answered with DuckDB

These ran on a scan of an orb's root filesystem
(`sudo wtfs -x --deleted -o root.parquet /`). `format_bytes` prints sizes in
binary units. `sum()` returns HUGEINT, so cast it with `::BIGINT` before
formatting.

**Biggest directories at any depth**

```sql
SELECT tree_du, format_bytes(tree_du) AS human, tree_count, path
FROM 'root.parquet' WHERE kind = 'dir'
ORDER BY tree_du DESC LIMIT 8;
```
```
┌─────────────┬──────────┬────────────┬────────────┐
│   tree_du   │  human   │ tree_count │    path    │
├─────────────┼──────────┼────────────┼────────────┤
│ 13569083904 │ 12.6 GiB │    1295725 │ /          │
│  4560932352 │ 4.2 GiB  │     180982 │ /nix       │
│  4421660160 │ 4.1 GiB  │     180931 │ /nix/store │
│  4192581632 │ 3.9 GiB  │    1020255 │ /tmp       │
│  3988893696 │ 3.7 GiB  │    1020201 │ /tmp/big   │
│  2843454464 │ 2.6 GiB  │      54921 │ /usr       │
│  1764002816 │ 1.6 GiB  │      34889 │ /home      │
│  1763990528 │ 1.6 GiB  │      34884 │ /home/user │
└─────────────┴──────────┴────────────┴────────────┘
```

**Nix store versus home versus everything else**

```sql
SELECT CASE WHEN path LIKE '/nix/%' THEN 'nix'
            WHEN path LIKE '/home/%' THEN 'home'
            WHEN kind = 'deleted' THEN 'deleted, still open'
            ELSE 'other' END AS area,
       format_bytes(sum(du)::BIGINT) AS du, count(*) AS entries
FROM 'root.parquet' GROUP BY area ORDER BY sum(du) DESC;
```
```
┌─────────────────────┬──────────┬─────────┐
│        area         │    du    │ entries │
├─────────────────────┼──────────┼─────────┤
│ other               │ 6.7 GiB  │ 1079856 │
│ nix                 │ 4.2 GiB  │  180981 │
│ home                │ 1.6 GiB  │   34888 │
│ deleted, still open │ 64.0 MiB │       1 │
└─────────────────────┴──────────┴─────────┘
```

**Largest Nix store paths**

```sql
SELECT format_bytes(tree_du) AS du, path FROM 'root.parquet'
WHERE parent = (SELECT id FROM 'root.parquet' WHERE path = '/nix/store')
ORDER BY tree_du DESC LIMIT 5;
```
```
┌───────────┬──────────────────────────────────────────────────────────────┐
│    du     │                             path                             │
├───────────┼──────────────────────────────────────────────────────────────┤
│ 840.6 MiB │ /nix/store/5ajx1np9zlq7z6ljqq18sibpzhnjakdw-clang-22.1.8-lib │
│ 571.9 MiB │ /nix/store/nsgmh9gxbbpg3cla9njjh9qr3akh3f98-llvm-22.1.8-lib  │
│ 550.7 MiB │ /nix/store/mr6cbi3rdy6w5y4cd6875gf4y0pqn0jf-llvm-21.1.8-lib  │
│ 443.4 MiB │ /nix/store/34wy21hkax0k707757pbgd77fjxdv6v5-zig-0.17.0       │
│ 337.9 MiB │ /nix/store/87lfqfz7mc5dm4w75qikxhd6lkr9jisr-source           │
└───────────┴──────────────────────────────────────────────────────────────┘
```

Whether a store path is still referenced is a question for Nix:
`nix-store --query --roots /nix/store/...` and `nix-collect-garbage --dry-run`.

**What was written in the last 24 hours, by two-level directory**

```sql
SELECT regexp_extract(path, '^/[^/]+/[^/]+') AS dir,
       format_bytes(sum(du)::BIGINT) AS du_recent, count(*) AS files
FROM 'root.parquet'
WHERE kind = 'file' AND mtime > now() - INTERVAL 24 HOUR
GROUP BY dir ORDER BY sum(du) DESC LIMIT 6;
```
```
┌────────────────┬───────────┬─────────┐
│      dir       │ du_recent │  files  │
├────────────────┼───────────┼─────────┤
│ /tmp/big       │ 3.6 GiB   │ 1000000 │
│ /home/user     │ 1.1 GiB   │   11740 │
│ /usr/local     │ 309.1 MiB │    2530 │
│ /tmp/big.arrow │ 166.3 MiB │       1 │
│ /nix/var       │ 132.8 MiB │      28 │
│ /root/.npm     │ 105.8 MiB │      34 │
└────────────────┴───────────┴─────────┘
```

**What grew between two scans** (keep the older one around)

```sql
SELECT format_bytes(coalesce(new.tree_du, 0) - coalesce(old.tree_du, 0)) AS growth, path
FROM 'yesterday.parquet' old FULL JOIN 'today.parquet' new USING (path)
WHERE coalesce(new.kind, old.kind) = 'dir' AND coalesce(new.depth, old.depth) <= 3
ORDER BY coalesce(new.tree_du, 0) - coalesce(old.tree_du, 0) DESC LIMIT 10;
```

**Space by file extension**

```sql
SELECT lower(regexp_extract(path, '\.([A-Za-z0-9]{1,8})$', 1)) AS ext,
       format_bytes(sum(du)::BIGINT) AS du, count(*) AS files
FROM 'root.parquet' WHERE kind = 'file'
GROUP BY ext ORDER BY sum(du) DESC LIMIT 6;
```
```
┌─────────┬───────────┬─────────┐
│   ext   │    du     │  files  │
├─────────┼───────────┼─────────┤
│ js      │ 3.6 GiB   │ 1001428 │
│         │ 3.0 GiB   │   51022 │
│ a       │ 1.7 GiB   │     721 │
│ 1       │ 655.5 MiB │     210 │
│ h       │ 490.2 MiB │   38091 │
│ so      │ 381.5 MiB │    1783 │
└─────────┴───────────┴─────────┘
```

**Deleted files still held open** (space `df` counts but no walk can find;
freed when the process closes them or exits)

```sql
SELECT format_bytes(du) AS du, path, open_by FROM 'root.parquet' WHERE kind = 'deleted';
```
```
┌──────────┬────────────────────┬────────────────────────┐
│    du    │        path        │        open_by         │
├──────────┼────────────────────┼────────────────────────┤
│ 64.0 MiB │ /tmp/agent-log.txt │ pid 13953 (sleep) fd 4 │
└──────────┴────────────────────┴────────────────────────┘
```

`--deleted` reports files on any filesystem the scan visited, wherever they
used to live, since they take space from the same `df` total. Without root it
only sees your own processes.

**More**

```sql
-- Biggest files
SELECT du, path FROM 'scan.parquet' WHERE kind = 'file' ORDER BY du DESC LIMIT 20;
-- Who owns the space (join uid against getent passwd yourself)
SELECT uid, format_bytes(sum(du)::BIGINT) FROM 'scan.parquet' GROUP BY uid ORDER BY sum(du) DESC;
-- Immediate children of one directory
SELECT tree_du, kind, path FROM 'scan.parquet'
WHERE parent = (SELECT id FROM 'scan.parquet' WHERE path = '/home/user') ORDER BY tree_du DESC;
-- Directories that are mostly small files (node_modules, caches)
SELECT tree_count, format_bytes(tree_du), path FROM 'scan.parquet'
WHERE kind = 'dir' AND depth <= 4 ORDER BY tree_count DESC LIMIT 20;
-- Sparse or compressed files: apparent size far above allocation
SELECT size, alloc, path FROM 'scan.parquet' WHERE kind = 'file' AND size > 4 * alloc + 1e6 ORDER BY size DESC;
-- What could not be read
SELECT error, count(*), any_value(path) FROM 'scan.parquet' WHERE error IS NOT NULL GROUP BY error;
```

## Parquet or Arrow

Both files hold the same table. Use **Parquet** unless you have a reason not to.

- DuckDB reads Parquet natively. As of DuckDB 2.0.0, reading Arrow IPC files
  still needs the `arrow` community extension: `INSTALL arrow FROM community;
  LOAD arrow;`, which requires network access the first time.
- Parquet files are about 20 times smaller (one million entries: 7.5 MB
  against 174 MB), because they are compressed and Arrow files are not.
- Writing Parquet is slower because of compression, which runs on all cores:
  for a million entries, 0.58 s for the whole run against 0.45 s with Arrow.

Arrow is still useful if you load the file with pyarrow, polars or another
Arrow-native tool and want zero-copy, uncompressed columns.

## Python TUI (optional)

`wtfs/` holds an older interactive browser (Textual) with archive and delete
workflows. It runs the binary with `--binary-output`, which still writes the
legacy `wtfsdumpv1` snapshot of directories and large files. See
[README-python.md](README-python.md) and [BUILDING.md](BUILDING.md).

```bash
zig build release && uv run wtfs -i .
```

## License

MIT; see [LICENSE](LICENSE). Author: Mikael Brockman ([@mbrock](https://github.com/mbrock)).
