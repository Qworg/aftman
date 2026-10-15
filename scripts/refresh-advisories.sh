#!/usr/bin/env bash
#
# Move the pinned RustSec advisory snapshot in `advisories/` to upstream's current tip.
#
# The snapshot is a git submodule pinned to one commit, which is what makes the `audit`
# gate a function of this repository rather than of whatever the advisory database looked
# like at the moment CI happened to run. That is the point of it, so moving the pin is a
# deliberate act with a reviewable one-line diff — which is this script.
#
# Two things are checked when the pin moves, and they answer different questions:
#
#   cargo audit --db advisories --no-fetch --deny warnings   # is our tree clean, as of now?
#   cargo audit --deny warnings                              # has something been published?
#
# The second is also what the `audit-live` scheduled job runs, so if a refresh turns up
# nothing new, the schedule was already covering it.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ ! -e advisories/.git ]; then
  echo "advisories/ is not checked out; run: git submodule update --init" >&2
  exit 1
fi

old=$(git -C advisories rev-parse HEAD)
echo "current pin: $old"
echo "             $(git -C advisories log -1 --format='%ci  %s' | cut -c1-70)"

git -C advisories fetch --quiet origin main
git -C advisories checkout --quiet origin/main
new=$(git -C advisories rev-parse HEAD)

if [ "$old" = "$new" ]; then
  echo "already at upstream's tip; nothing to do"
  exit 0
fi

echo "new pin:     $new"
echo "             $(git -C advisories log -1 --format='%ci  %s' | cut -c1-70)"
echo
echo "Advisories added by this refresh:"
git -C advisories log --oneline --no-merges "$old..$new" -- crates | sed 's/^/  /'
echo
echo "Next:"
echo "  cargo audit --db advisories --no-fetch --deny warnings   # must pass"
echo "  cargo audit --deny warnings                              # may not; fix or note it"
echo "  git add advisories && git commit                         # record the new pin"
echo
echo "The commit body should say which advisories arrived and what was done about them —"
echo "a refresh that pulls in an advisory nobody acts on is just a moved pin."
