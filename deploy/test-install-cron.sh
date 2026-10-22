#!/usr/bin/env bash
# Exercises install-cron.sh against a fake crontab, never the real one. Run: deploy/test-install-cron.sh
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
store="$tmp/crontab.txt"; : > "$store"

# Fake crontab: "-l" prints the store (exit 1 when empty, like the real one), "-" replaces it.
cat > "$tmp/crontab" <<SHIM
#!/usr/bin/env bash
case "\$1" in
  -l) [ -s "$store" ] && cat "$store" || { echo "no crontab for test" >&2; exit 1; } ;;
  -)  cat > "$store" ;;
esac
SHIM
chmod +x "$tmp/crontab"
printf '#!/bin/sh\n' > "$tmp/flock"; chmod +x "$tmp/flock"

app="$tmp/app dir"; mkdir -p "$app"
printf '#!/bin/sh\n' > "$app/nt-sync"; chmod +x "$app/nt-sync"
printf 'NT_API_KEY=x\n' > "$app/.env"; chmod 600 "$app/.env"
run() { CRONTAB_BIN="$tmp/crontab" FLOCK_BIN="$tmp/flock" "$here/install-cron.sh" --dir "$app" "$@"; }

fail() { echo "FAIL: $*" >&2; exit 1; }
count() { grep -c "$1" "$store" || true; }

echo "# keep me" > "$store"
run --dry-run --every 10 | grep -q '^\*/10 ' || fail "dry-run must print the cron line"
[ "$(cat "$store")" = "# keep me" ] || fail "dry-run must not change the crontab"

run --every 5 >/dev/null
[ "$(count '^\*/5 ')" = 1 ] || fail "install must add one entry"
grep -q '# keep me' "$store" || fail "install must keep unrelated lines"
grep -q "cd '\?.*app dir'\? &&\|cd .*app\\\\ dir" "$store" || fail "paths with spaces must be quoted"

run --every 15 >/dev/null
[ "$(count 'BEGIN nt-sync')" = 1 ] && [ "$(count '^\*/15 ')" = 1 ] && [ "$(count '^\*/5 ')" = 0 ] || fail "re-running must replace the entry, not duplicate it"

run --remove >/dev/null
: > "$store"
run --every 5 >/dev/null
[ "$(count '^\*/5 ')" = 1 ] || fail "install into an empty crontab must work"
run --remove >/dev/null
echo "# keep me" > "$store"
run --every 5 >/dev/null
run --remove >/dev/null
[ "$(count 'nt-sync')" = 0 ] && grep -q '# keep me' "$store" || fail "remove must delete only the nt-sync block"
run --remove | grep -q 'nothing to remove' || fail "remove without an entry must say so"

for bad in 0 60 abc 1.5 ""; do
  if run --every "$bad" >/dev/null 2>&1; then fail "--every '$bad' must be rejected"; fi
done
rm "$app/.env"
if run >/dev/null 2>&1; then fail "install without .env must be rejected"; fi

echo "OK"
