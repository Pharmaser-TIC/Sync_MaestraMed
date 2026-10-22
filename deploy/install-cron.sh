#!/usr/bin/env bash
# Installs, previews or removes the cron entry that runs `nt-sync -once` periodically.
#
#   deploy/install-cron.sh [--every MINUTES] [--dir DIR] [--user USER] [--dry-run] [--remove]
#
# The entry is idempotent: it lives between "# BEGIN nt-sync" / "# END nt-sync" markers, so running
# the script again replaces it (for example to change the interval) and never touches other lines.
set -euo pipefail

EVERY=5
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CRON_USER=""
MODE=install
CRONTAB_BIN="${CRONTAB_BIN:-crontab}"
FLOCK_BIN="${FLOCK_BIN:-$(command -v flock || true)}"
BEGIN="# BEGIN nt-sync (managed by deploy/install-cron.sh; do not edit)"
END="# END nt-sync"

usage() {
  cat <<USAGE
Usage: $0 [--every MINUTES] [--dir DIR] [--user USER] [--dry-run] [--remove]

  --every N    run every N minutes, 1-59 (default: $EVERY; a divisor of 60 keeps the gaps even)
  --dir DIR    folder with the nt-sync binary and its .env (default: $DIR)
  --user USER  install in that user's crontab (needs root; default: the current user)
  --dry-run    print the cron block and change nothing
  --remove     remove the nt-sync block from the crontab
USAGE
}

die() { echo "error: $*" >&2; exit 1; }

while [ $# -gt 0 ]; do
  case "$1" in
    --every)   [ $# -ge 2 ] || die "--every needs a value"; EVERY="$2"; shift 2 ;;
    --dir)     [ $# -ge 2 ] || die "--dir needs a value"; DIR="$2"; shift 2 ;;
    --user)    [ $# -ge 2 ] || die "--user needs a value"; CRON_USER="$2"; shift 2 ;;
    --dry-run) MODE=print; shift ;;
    --remove)  MODE=remove; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "unknown option: $1" ;;
  esac
done

cron() { if [ -n "$CRON_USER" ]; then "$CRONTAB_BIN" -u "$CRON_USER" "$@"; else "$CRONTAB_BIN" "$@"; fi; }

# Current crontab without our block and without trailing blank lines.
crontab_without_block() {
  { cron -l 2>/dev/null || true; } \
    | awk -v b="$BEGIN" -v e="$END" '$0==b{skip=1;next} $0==e{skip=0;next} !skip' \
    | awk 'NF{last=NR} {line[NR]=$0} END{for(i=1;i<=last;i++)print line[i]}'
}

if [ -n "$CRON_USER" ] && [ "$(id -u)" -ne 0 ]; then die "--user needs root"; fi
if [ "$MODE" != print ] && ! command -v "$CRONTAB_BIN" >/dev/null 2>&1; then
  die "crontab not found (minimal Ubuntu images do not ship it). Install it: sudo apt install cron && sudo systemctl enable --now cron"
fi

if [ "$MODE" = remove ]; then
  if ! { cron -l 2>/dev/null || true; } | grep -qxF "$BEGIN"; then echo "no nt-sync entry found; nothing to remove"; exit 0; fi
  remaining="$(crontab_without_block)"   # build it fully before replacing the crontab
  printf '%s\n' "$remaining" | cron -
  echo "nt-sync entry removed"
  exit 0
fi

case "$EVERY" in ''|*[!0-9]*) die "--every must be a whole number of minutes (1-59)" ;; esac
[ "$EVERY" -ge 1 ] && [ "$EVERY" -le 59 ] || die "--every must be between 1 and 59"
[ -d "$DIR" ] || die "directory not found: $DIR"
DIR="$(cd "$DIR" && pwd)"
LOCK="$DIR/.nt-sync.lock"
LOG="logs/nt-sync.log"

problems=0
warn_or_die() { if [ "$MODE" = print ]; then echo "warning: $*" >&2; else die "$*"; fi; }
[ -x "$DIR/nt-sync" ] || warn_or_die "$DIR/nt-sync is missing or not executable (build it: GOOS=linux GOARCH=amd64 go build -o nt-sync ./cmd/nt-sync)"
[ -f "$DIR/.env" ]    || warn_or_die "$DIR/.env is missing: cron has no environment variables of its own, so the settings must be in that file"
[ -n "$FLOCK_BIN" ]   || warn_or_die "flock not found (Ubuntu: it ships with util-linux)"
if [ -f "$DIR/.env" ] && [ -n "$(find "$DIR/.env" -perm -004 2>/dev/null)" ]; then
  echo "warning: $DIR/.env is readable by every user; it holds the API key. Run: chmod 600 $DIR/.env" >&2
fi
if [ $((60 % EVERY)) -ne 0 ]; then
  echo "warning: $EVERY does not divide 60, so the gap is uneven at the top of each hour" >&2
fi

printf -v qdir '%q' "$DIR"
printf -v qlock '%q' "$LOCK"
block="$BEGIN
*/$EVERY * * * * cd $qdir && ${FLOCK_BIN:-flock} -n $qlock ./nt-sync -once >> $LOG 2>&1
$END"

if [ "$MODE" = print ]; then printf '%s\n' "$block"; exit 0; fi

mkdir -p "$DIR/logs"
if [ -n "$CRON_USER" ]; then chown "$CRON_USER" "$DIR/logs"; fi
existing="$(crontab_without_block)"   # build it fully before replacing the crontab
{ [ -n "$existing" ] && printf '%s\n' "$existing"; printf '%s\n' "$block"; } | cron -
echo "installed: nt-sync runs every $EVERY minute(s); log: $DIR/$LOG"
echo "check it with: crontab -l${CRON_USER:+ -u $CRON_USER}"
