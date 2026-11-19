#!/usr/bin/env bash
# lib/compat.sh — portable shell helpers for Linux (any distro), macOS and FreeBSD.
# Source from other scripts:  . "$here/lib/compat.sh"

_COMPAT_OS=$(uname -s)

# ── disk ──────────────────────────────────────────────────────────────────────

free_bytes() {
  df -Pk "$1" | awk 'NR==2 { print $4 * 1024 }'
}

used_pct() {
  df -Pk "$1" | awk 'NR==2 { gsub(/%/,""); print $5 }'
}

# ── memory and load ──────────────────────────────────────────────────────────

mem_total_kb() {
  case $_COMPAT_OS in
    Linux)   awk '/MemTotal/ { print $2 }' /proc/meminfo ;;
    Darwin)  sysctl -n hw.memsize | awk '{ printf "%.0f\n", $1 / 1024 }' ;;
    FreeBSD) sysctl -n hw.physmem | awk '{ printf "%.0f\n", $1 / 1024 }' ;;
    *) echo 0 ;;
  esac
}

mem_avail_kb() {
  case $_COMPAT_OS in
    Linux)   awk '/MemAvailable/ { print $2 }' /proc/meminfo ;;
    Darwin)  local ps; ps=$(sysctl -n hw.pagesize)
             vm_stat | awk -v ps="$ps" '
               /Pages free/     { f=$3 }
               /Pages inactive/ { i=$3 }
               END { gsub(/\./,"",f); gsub(/\./,"",i); printf "%.0f\n",(f+i)*ps/1024 }' ;;
    FreeBSD) sysctl -n vm.stats.vm.v_free_count \
               | awk -v ps="$(sysctl -n hw.pagesize)" '{ printf "%.0f\n",$1*ps/1024 }' ;;
    *) echo 0 ;;
  esac
}

mem_summary() {
  local t a; t=$(mem_total_kb); a=$(mem_avail_kb)
  awk -v t="$t" -v a="$a" 'BEGIN { printf "%.0f GB used of %.0f GB",(t-a)/1e6,t/1e6 }'
}

loadavg() {
  case $_COMPAT_OS in
    Linux)   cut -d' ' -f1-3 /proc/loadavg ;;
    Darwin)  sysctl -n vm.loadavg 2>/dev/null | tr -d '{}' | awk '{ print $1,$2,$3 }' ;;
    FreeBSD) sysctl -n vm.loadavg 2>/dev/null | tr -d '{}' | awk '{ print $1,$2,$3 }' ;;
    *)       uptime | sed 's/.*load average[s]*: //' | cut -d, -f1-3 | tr -d ' ' | tr ',' ' ' ;;
  esac
}

# ── dates ─────────────────────────────────────────────────────────────────────

# Parse a UTC date/time string to epoch seconds.  Falls back to 0.
date_to_epoch() {
  # GNU date
  date -ud "$1" +%s 2>/dev/null && return 0
  # BSD date (macOS, FreeBSD)
  local fmt
  for fmt in '%Y-%m-%dT%H:%M:%SZ' '%Y-%m-%dT%H:%M:%S' '%Y-%m-%d %H:%M:%S' '%Y-%m-%d'; do
    date -juf "$fmt" "$1" +%s 2>/dev/null && return 0
  done
  # Python fallback (python3 is a prerequisite for the project)
  python3 -c "
from datetime import datetime,timezone
import sys
s = sys.argv[1]
if s.endswith('Z'): s = s[:-1] + '+00:00'
d = datetime.fromisoformat(s)
if d.tzinfo is None: d = d.replace(tzinfo=timezone.utc)
print(int(d.timestamp()))
" "$1" 2>/dev/null && return 0
  echo 0
}

# ── numbers ───────────────────────────────────────────────────────────────────

num() { printf "%'d" "$1" 2>/dev/null || printf '%s' "$1"; }

# ── service management ────────────────────────────────────────────────────────
# Abstracts systemd (Linux), launchctl (macOS), and sysvinit/openrc fallbacks.

_svc() { echo "blockline-usdt-$1"; }

# Single systemd property, works with or without --value (systemd 230+).
_svc_prop() {
  local svc="$(_svc "$1").service" prop=$2 val
  val=$(systemctl show -p "$prop" --value "$svc" 2>/dev/null) && [ -n "$val" ] && { echo "$val"; return; }
  systemctl show -p "$prop" "$svc" 2>/dev/null | cut -d= -f2-
}

svc_active() {
  case $_COMPAT_OS in
    Linux)
      if command -v systemctl >/dev/null 2>&1; then
        [ "$(_svc_prop "$1" ActiveState)" = active ]
      else
        service "$(_svc "$1")" status >/dev/null 2>&1
      fi ;;
    Darwin)
      launchctl list "com.blockline-usdt.$1" >/dev/null 2>&1 ;;
    *)
      service "$(_svc "$1")" status >/dev/null 2>&1 ;;
  esac
}

svc_installed() {
  case $_COMPAT_OS in
    Linux)
      if command -v systemctl >/dev/null 2>&1; then
        [ "$(_svc_prop "$1" LoadState)" = loaded ]
      else
        [ -e "/etc/init.d/$(_svc "$1")" ]
      fi ;;
    Darwin)
      [ -e "/Library/LaunchDaemons/com.blockline-usdt.$1.plist" ] ;;
    *)
      [ -e "/etc/init.d/$(_svc "$1")" ] ;;
  esac
}

svc_ctl() {
  local action=$1 unit=$2
  case $_COMPAT_OS in
    Linux)
      if command -v systemctl >/dev/null 2>&1; then
        sudo -n /usr/bin/systemctl "$action" "$(_svc "$unit")"
      else
        sudo -n service "$(_svc "$unit")" "$action"
      fi ;;
    Darwin)
      local plist="/Library/LaunchDaemons/com.blockline-usdt.$unit.plist"
      case $action in
        start)   sudo launchctl load -w "$plist" ;;
        stop)    sudo launchctl unload "$plist" ;;
        restart) sudo launchctl unload "$plist" 2>/dev/null; sudo launchctl load -w "$plist" ;;
      esac ;;
    *)
      sudo -n service "$(_svc "$unit")" "$action" ;;
  esac
}

# Returns load|act|sub|since|mem for the status display.
svc_info() {
  local u=$1
  case $_COMPAT_OS in
    Linux)
      if command -v systemctl >/dev/null 2>&1; then
        local props load act sub since mem
        props=$(systemctl show "$(_svc "$u").service" \
          -p LoadState -p ActiveState -p SubState -p ActiveEnterTimestamp -p MemoryCurrent 2>/dev/null)
        load=$( awk -F= '/^LoadState=/           { print $2 }' <<<"$props")
        act=$(  awk -F= '/^ActiveState=/          { print $2 }' <<<"$props")
        sub=$(  awk -F= '/^SubState=/             { print $2 }' <<<"$props")
        since=$(awk -F= '/^ActiveEnterTimestamp=/ { $1=""; sub(/^ /,""); print }' <<<"$props")
        mem=$(  awk -F= '/^MemoryCurrent=/        { print $2 }' <<<"$props")
        printf '%s|%s|%s|%s|%s' "$load" "$act" "$sub" "$since" "$mem"
      else
        local load=not-found act=inactive sub=dead
        [ -e "/etc/init.d/$(_svc "$u")" ] && load=loaded
        if service "$(_svc "$u")" status >/dev/null 2>&1; then act=active; sub=running; fi
        printf '%s|%s|%s||' "$load" "$act" "$sub"
      fi ;;
    Darwin)
      local load=not-found act=inactive sub=dead
      [ -e "/Library/LaunchDaemons/com.blockline-usdt.$u.plist" ] && load=loaded
      if launchctl list "com.blockline-usdt.$u" >/dev/null 2>&1; then act=active; sub=running; fi
      printf '%s|%s|%s||' "$load" "$act" "$sub" ;;
    *)
      local load=not-found act=inactive sub=dead
      [ -e "/etc/init.d/$(_svc "$u")" ] && load=loaded
      if service "$(_svc "$u")" status >/dev/null 2>&1; then act=active; sub=running; fi
      printf '%s|%s|%s||' "$load" "$act" "$sub" ;;
  esac
}

# ── packages (bootstrap) ─────────────────────────────────────────────────────

pkg_install() {
  case $_COMPAT_OS in
    Linux)
      if   command -v apt-get >/dev/null 2>&1; then apt-get update -q && DEBIAN_FRONTEND=noninteractive apt-get install -y -q "$@"
      elif command -v dnf     >/dev/null 2>&1; then dnf install -y "$@"
      elif command -v yum     >/dev/null 2>&1; then yum install -y "$@"
      elif command -v pacman  >/dev/null 2>&1; then pacman -Sy --noconfirm "$@"
      elif command -v apk     >/dev/null 2>&1; then apk add "$@"
      elif command -v zypper  >/dev/null 2>&1; then zypper install -y "$@"
      else echo "no supported package manager; install manually: $*" >&2; return 1; fi ;;
    Darwin)
      if command -v brew >/dev/null 2>&1; then brew install "$@"
      else echo "install Homebrew (https://brew.sh), then: brew install $*" >&2; return 1; fi ;;
    FreeBSD) pkg install -y "$@" ;;
    *) echo "unsupported OS ($_COMPAT_OS); install manually: $*" >&2; return 1 ;;
  esac
}

# ── users (bootstrap) ────────────────────────────────────────────────────────

create_user() {
  local name=$1
  id "$name" >/dev/null 2>&1 && return 0
  case $_COMPAT_OS in
    Linux)
      if command -v useradd >/dev/null 2>&1; then useradd --create-home --shell /bin/bash "$name"
      else adduser -D -s /bin/bash "$name"; fi ;;  # Alpine
    Darwin)  sysadminctl -addUser "$name" -shell /bin/bash -home "/Users/$name" ;;
    FreeBSD) pw useradd "$name" -m -s /bin/bash ;;
    *) useradd -m -s /bin/bash "$name" 2>/dev/null || adduser "$name" ;;
  esac
}

run_as() {
  local user=$1; shift
  if [ "$(id -un)" = "$user" ]; then "$@"
  elif command -v sudo >/dev/null 2>&1; then sudo -n -u "$user" "$@"
  elif command -v runuser >/dev/null 2>&1; then runuser -u "$user" -- "$@"
  else su -s /bin/bash "$user" -c "$(printf '%q ' "$@")"
  fi
}
