#!/bin/bash
# NOTE: This script is entirely LLM generated, so please exercise caution
# archive_build.sh -- snapshot a SUCCESSFUL build into git: P4 + both pipes' .bfa + control-plane files.
#
# Usage (run it only after the build succeeds, so a failed build never gets archived):
#     make install && ./archive_build.sh "pipe1 batch changes ported"
#   or put it in your build shell function:  build() { (cd $SDE/p4-build-9.4.0 && make install) && archive_build.sh "$@"; }
#
# What it does
#   1. Refuses if a .bfa is OLDER than the P4 (a failed compile leaves the previous .bfa behind).
#   2. Creates   <DEST_ROOT>/<YYYYmmdd-HHMMSS>_<p4 hash>/   containing
#        full_test.p4, pipe0.bfa, pipe1.bfa, your control-plane files, and build_info.txt
#        (date, source hash, run_id and ingress/egress stage counts of each pipe, your note).
#   3. git add + commit in REPO with the stage counts in the message (skipped if REPO is not a git repo).
#
# Edit the paths below, or override them with environment variables.

set -euo pipefail

P4="${P4:-/root/pringles/code/p4/pktgen_tests/full_test/full_test.p4}"
BUILD="${BUILD:-/root/bf-sde-9.4.0/p4-build-9.4.0/tofino/viewchange}"     # contains pipe0/ and pipe1/
REPO="${REPO:-/root/pringles}"                                            # the git repository
DEST_ROOT="${DEST_ROOT:-$REPO/builds}"
BFA_NAME="${BFA_NAME:-full_test.bfa}"
# extra files to keep alongside the build (missing ones are skipped)
EXTRA=(
  "/root/pringles/code/p4/controlplane_libs/tables.py"
  "/root/pringles/code/p4/controlplane_libs/headers.py"
  "/root/pringles/code/p4/controlplane_libs/view_change.py"
  "/root/pringles/code/p4/pktgen_tests/full_test/test.py"
  "/root/bf-sde-9.4.0/install/share/p4/targets/tofino/viewchange.conf"
)
NOTE="${*:-}"

die() { echo "archive_build: $*" >&2; exit 1; }

[ -f "$P4" ] || die "P4 not found: $P4"
for pipe in 0 1; do
  bfa="$BUILD/pipe$pipe/$BFA_NAME"
  [ -f "$bfa" ]   || die "missing $bfa (did the build fail?)"
  [ "$bfa" -nt "$P4" ] || die "STALE: $bfa is older than $P4 -- the last compile probably failed; nothing archived"
done

hash=$(sha1sum "$P4" | cut -c1-6)
dest="$DEST_ROOT/$(date +%Y%m%d-%H%M%S)_$hash"
mkdir "$dest"

cp "$P4" "$dest/full_test.p4"
for pipe in 0 1; do cp "$BUILD/pipe$pipe/$BFA_NAME" "$dest/pipe$pipe.bfa"; done
for f in "${EXTRA[@]}"; do [ -f "$f" ] && cp "$f" "$dest/" || true; done

{
  echo "date        : $(date -Is)"
  echo "p4 source   : $P4"
  echo "p4 sha1     : $(sha1sum "$P4" | cut -d' ' -f1)"
  echo "note        : $NOTE"
  for pipe in 0 1; do
    f="$dest/pipe$pipe.bfa"
    echo "pipe$pipe      : run_id $(grep -m1 run_id "$f" | tr -d ' "' | cut -d: -f2)  ingress $(grep -c '^stage .* ingress:' "$f") stages, egress $(grep -c '^stage .* egress:' "$f") stages"
  done
  if git -C "$REPO" rev-parse --git-dir >/dev/null 2>&1; then
    echo "repo HEAD   : $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo none)  (uncommitted source changes: $(git -C "$REPO" status --porcelain -- "$P4" 2>/dev/null | wc -l))"
  fi
} > "$dest/build_info.txt"

summary="build $hash: pipe0 ingress $(grep -c '^stage .* ingress:' "$dest/pipe0.bfa"), pipe1 ingress $(grep -c '^stage .* ingress:' "$dest/pipe1.bfa") stages"
[ -n "$NOTE" ] && summary="$summary -- $NOTE"

if git -C "$REPO" rev-parse --git-dir >/dev/null 2>&1; then
  git -C "$REPO" add "$dest"
  git -C "$REPO" commit -q -m "$summary" -- "$dest" && echo "committed: $summary"
else
  echo "(not a git repo; files saved only)"
fi
echo "archived to: $dest"
cat "$dest/build_info.txt"
