#!/usr/bin/env bash
# Smoke-test a wtfs binary: scan a small tree with a hardlink, a sparse file
# and a symlink, check the total against du, and (if duckdb is on PATH or
# importable from python3) check that the table loads and sums to the same.
set -euo pipefail

bin=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cd "$work"

mkdir -p t/a/b t/c "t/sp ace"
head -c 100000 /dev/zero > t/a/b/big
ln t/a/b/big t/c/big-link
dd if=/dev/zero of=t/sparse bs=1 count=1 seek=100000000 2>/dev/null
ln -s a t/link
for i in $(seq 1 200); do echo "$i" > "t/sp ace/f$i"; done

"$bin" --version
"$bin" -q -o scan.parquet t | tee summary.txt
"$bin" -q -n 0 -o scan.arrow t > /dev/null
got=$(sed -n 's/^# du \([0-9]*\) bytes.*/\1/p' summary.txt)

if [ "$(uname)" = Darwin ]; then
  # BSD du has no -B1; -k sums 512-byte blocks and rounds up once at the end.
  expected=$(( $(du -sk t | cut -f1) * 1024 ))
  diff=$(( expected - got ))
  echo "du -sk*1024=$expected wtfs=$got"
  [ "$diff" -ge 0 ] && [ "$diff" -lt 1024 ]
else
  expected=$(du -sB1 t | cut -f1)
  echo "du -sB1=$expected wtfs=$got"
  [ "$expected" = "$got" ]
fi

grep -q "/sp ace/f1$" <("$bin" -q -n 1000 t)

query="SELECT sum(du), count(*) FILTER (WHERE kind = 'file') FROM 'scan.parquet'"
if command -v duckdb > /dev/null; then
  result=$(duckdb -csv -noheader -c "$query")
elif python3 -c 'import duckdb' 2> /dev/null; then
  result=$(python3 -c "import duckdb,sys; print(','.join(map(str, duckdb.sql(sys.argv[1]).fetchone())))" "$query")
else
  echo "duckdb not available; skipping table check"
  exit 0
fi
echo "duckdb: sum(du),files = $result"
[ "$result" = "$got,203" ]
echo OK
