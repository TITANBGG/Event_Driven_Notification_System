#!/usr/bin/env bash
# Renders every ```mermaid block in README.md with mermaid-cli, so a diagram that GitHub would show as
# "Unable to render rich display" fails CI instead of the reader's first look at the architecture.
# (A `;` inside a sequenceDiagram message is a statement separator, which broke one diagram unnoticed.)
#
#   ./scripts/check-diagrams.sh            needs only Docker
set -euo pipefail

MERMAID_CLI_IMAGE="minlag/mermaid-cli:11.17.1" # pinned; CI runs this script, so this is the only place to bump
cd "$(dirname "$0")/.."
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
chmod 777 "$WORK" # the image runs as a non-root user

# Split README.md into one .mmd file per mermaid block, named after its starting line.
awk -v dir="$WORK" '
  /^```mermaid[[:space:]]*$/ { inblock = 1; file = sprintf("%s/line-%04d.mmd", dir, NR + 1); next }
  inblock && /^```[[:space:]]*$/ { inblock = 0; close(file); next }
  inblock { print > file }
' README.md

shopt -s nullglob
files=("$WORK"/*.mmd)
if ((${#files[@]} == 0)); then
  echo "no mermaid blocks found in README.md" >&2
  exit 1
fi

src="$(cd "$WORK" && (pwd -W 2>/dev/null || pwd))"
failed=0
for f in "${files[@]}"; do
  name="$(basename "$f")"
  line="$((10#${name//[^0-9]/}))"
  if MSYS_NO_PATHCONV=1 docker run --rm -v "${src}:/data" "$MERMAID_CLI_IMAGE" \
    -i "/data/${name}" -o "/data/${name%.mmd}.svg" >"$WORK/${name}.log" 2>&1; then
    echo "OK    README.md diagram at line ${line}"
  else
    echo "FAIL  README.md diagram at line ${line}:"
    grep -E '^Error|Parse error|Expecting|got ' "$WORK/${name}.log" | head -n 4 | sed 's/^/      /'
    failed=$((failed + 1))
  fi
done
if ((failed)); then
  echo "${failed} of ${#files[@]} diagram(s) failed to render" >&2
  exit 1
fi
echo "all ${#files[@]} diagrams render"
