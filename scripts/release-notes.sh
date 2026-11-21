#!/usr/bin/env bash
# Prints the CHANGELOG.md section for one version, used as the GitHub Release body.
#   ./scripts/release-notes.sh 1.0.0      (a leading "v" is accepted: v1.0.0)
# Exits 1 when the section is missing or empty, so a tag without release notes fails the release.
set -euo pipefail

version="${1:?usage: $0 <version>}"
version="${version#v}"
changelog="${CHANGELOG:-CHANGELOG.md}"

notes="$(awk -v ver="$version" '
  /^## \[/ { if (found) exit; if (index($0, "## [" ver "]") == 1) { found = 1; next } }
  /^\[[^]]+\]: / { if (found) exit }
  found { print }
' "$changelog")"

# Drop leading and trailing blank lines.
notes="$(printf '%s\n' "$notes" | sed -e '/./,$!d' | sed -e ':a' -e '/^\n*$/{$d;N;ba' -e '}')"

if [[ -z "${notes//[[:space:]]/}" ]]; then
  echo "no release notes for version ${version} in ${changelog}" >&2
  exit 1
fi
printf '%s\n' "$notes"
