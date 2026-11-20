#!/usr/bin/env sh
# Prints statement coverage per package from a Go cover profile.
# Blocks are de-duplicated by position, so profiles merged from several test binaries with
# -coverpkg (unit + integration) are not double counted: a block is covered if any binary hit it.
set -eu
profile="${1:-coverage.out}"
awk '
  NR == 1 && /^mode:/ { next }
  {
    block = $1; stmts = $2; count = $3
    if (!(block in size)) { size[block] = stmts }
    if (count > 0) { hit[block] = 1 }
  }
  END {
    for (block in size) {
      file = block; sub(/:.*/, "", file)
      pkg = file; sub(/\/[^\/]*$/, "", pkg)
      total[pkg] += size[block]
      if (block in hit) { covered[pkg] += size[block] }
      all += size[block]; if (block in hit) { allhit += size[block] }
    }
    for (pkg in total) {
      printf "%-70s %6.1f%%\n", pkg, (total[pkg] ? 100 * covered[pkg] / total[pkg] : 0)
    }
    printf "%-70s %6.1f%%\n", "TOTAL", (all ? 100 * allhit / all : 0) > "/dev/stderr"
  }
' "$profile" | sort
