#!/usr/bin/env bash
# One-time root setup for blockline-usdt (PLAN.md decision 4). Run once:
#   sudo bash /home/debian/projects/blockline-usdt/install/bootstrap.sh
#   sudo bash /home/debian/projects/blockline-usdt/install/bootstrap.sh --disk /dev/disk/by-uuid/<uuid>
# Idempotent: safe to run again. Installs no node, downloads no chain data, starts nothing.
#
#   0. --disk only: mounts an already formatted ext4 or xfs filesystem at /srv/blockline-usdt (by UUID in /etc/fstab).
#      It never formats anything, and refuses if the device is mounted elsewhere or /srv/blockline-usdt holds files.
#   1. creates the unprivileged user usdt (no sudo of its own)
#   2. creates /srv/blockline-usdt (node, ETL output, Postgres instance, logs) owned by usdt
#   3. lets `debian` work on usdt's files (ACLs) and run commands as usdt (sudo -u usdt, no password).
#      This grants debian the usdt user's rights only, never root.
#   4. clones /home/debian/projects/blockline-usdt into /home/usdt/blockline-usdt (origin = the draft repo, so
#      `git pull` as usdt brings later changes)
# Plus the packages the install stages need (git, curl, wget for the snapshot, gnupg, acl, logrotate, python3).
#
# Not done here (later root step, as for the forks): systemd units and the
# guard's sudoers rule. The usdt Postgres instance needs no root: it uses the installed PostgreSQL binaries.
set -euo pipefail

USER_NAME=usdt
DATA=/srv/blockline-usdt
SRC=/home/debian/projects/blockline-usdt
REPO=/home/$USER_NAME/blockline-usdt
PEAK_BYTES=$((4800 * 1000 ** 3))   # full-phase disk peak, PLAN.md 10.2 (upper estimate)

disk=""
while [ $# -gt 0 ]; do
  case "$1" in
    --disk) disk=${2:?--disk needs a device, e.g. /dev/disk/by-uuid/<uuid>}; shift ;;
    *) sed -n '2,19p' "$0"; exit 2 ;;
  esac
  shift
done
[[ $EUID -eq 0 ]] || { echo "run with sudo"; exit 1; }
[ -d "$SRC/.git" ] || { echo "missing $SRC (git repo)"; exit 1; }

echo "== packages"
apt-get update -q
DEBIAN_FRONTEND=noninteractive apt-get install -y -q git curl wget gnupg acl ca-certificates logrotate python3

if [ -n "$disk" ]; then
  echo "== disk $disk -> $DATA"
  [ -b "$disk" ] || { echo "   $disk is not a block device"; exit 1; }
  uuid=$(blkid -s UUID -o value "$disk" || true)
  fstype=$(blkid -s TYPE -o value "$disk" || true)
  [ -n "$uuid" ] || { echo "   $disk has no filesystem. Format it yourself first (this script never formats)"; exit 1; }
  case "$fstype" in ext4|xfs) ;; *) echo "   $disk is $fstype: only ext4 or xfs"; exit 1 ;; esac
  current=$(findmnt -rn -S "UUID=$uuid" -o TARGET | head -1 || true)
  if [ "$current" = "$DATA" ]; then
    echo "   already mounted at $DATA"
  else
    [ -z "$current" ] || { echo "   $disk is mounted at $current: refusing"; exit 1; }
    if [ -d "$DATA" ] && [ -n "$(ls -A "$DATA")" ]; then
      echo "   $DATA already holds files: refusing to mount over them"; exit 1
    fi
    mkdir -p "$DATA"
    if ! grep -q "^UUID=$uuid " /etc/fstab; then
      cp -a /etc/fstab "/etc/fstab.before-blockline-usdt.$(date -u +%Y%m%dT%H%M%SZ)"
      echo "UUID=$uuid $DATA $fstype defaults,noatime,nofail 0 2" >> /etc/fstab
    fi
    systemctl daemon-reload
    mount "$DATA"
    echo "   mounted ($fstype, UUID=$uuid), added to /etc/fstab"
  fi
fi

echo "== user $USER_NAME"
id "$USER_NAME" >/dev/null 2>&1 || useradd --create-home --shell /bin/bash "$USER_NAME"
chmod 750 "/home/$USER_NAME"

echo "== $DATA"
install -d -o "$USER_NAME" -g "$USER_NAME" -m 750 "$DATA"
chown "$USER_NAME:$USER_NAME" "$DATA"   # also when it is a fresh mount point
chmod 750 "$DATA"

echo "== repo $REPO"
if [ -d "$REPO/.git" ]; then
  echo "   exists: left as it is (git pull as $USER_NAME to update)"
else
  git -c safe.directory='*' clone -q "$SRC" "$REPO"
  chown -R "$USER_NAME:$USER_NAME" "$REPO"
  runuser -u "$USER_NAME" -- git -C "$REPO" config user.name "Nodestors Tech"
  runuser -u "$USER_NAME" -- git -C "$REPO" config user.email "tech@nodestors.com"
  echo "   cloned"
fi
# usdt pulls from debian's draft repo: read access to it, and git's ownership check relaxed for it only
setfacl -m "u:$USER_NAME:x" /home/debian /home/debian/projects
setfacl -R -m "u:$USER_NAME:rX" "$SRC"
runuser -u "$USER_NAME" -- git config --global --get-all safe.directory | grep -qxF "$SRC" \
  || runuser -u "$USER_NAME" -- git config --global --add safe.directory "$SRC"

echo "== access for debian"
for d in "/home/$USER_NAME" "$DATA"; do
  setfacl -R -m u:debian:rwX "$d"
  find "$d" -type d -exec setfacl -m d:u:debian:rwX -m "d:u:$USER_NAME:rwX" {} +
done

tmp=$(mktemp); trap 'rm -f "$tmp"' EXIT
cat > "$tmp" <<RULES
# blockline-usdt development: debian may run commands as the unprivileged usdt user (not as root)
debian ALL=($USER_NAME) NOPASSWD: ALL
RULES
visudo -cf "$tmp"
install -m 0440 "$tmp" /etc/sudoers.d/blockline-usdt-dev

echo "== checks"
[ -r /srv/blockline-shared/bandwidth.json ] && runuser -u "$USER_NAME" -- test -r /srv/blockline-shared/bandwidth.json \
  && echo "   shared traffic ledger readable by $USER_NAME" \
  || echo "   WARNING: $USER_NAME cannot read /srv/blockline-shared/bandwidth.json"
avail=$(df -B1 --output=avail "$DATA" | tail -1 | tr -d ' ')
echo "   free on $DATA: $((avail / 1000 ** 3)) GB; full-phase peak up to $((PEAK_BYTES / 1000 ** 3)) GB (PLAN.md 10.2)"
[ "$avail" -ge "$PEAK_BYTES" ] || echo "   WARNING: less free space than the full-phase peak: do not start the snapshot download"
[ "$(uname -m)" = x86_64 ] && echo "   x86_64: JDK 8 with the LevelDB snapshot" || echo "   WARNING: not x86_64: PLAN.md 2.4 needs JDK 17 and RocksDB"

echo "== done. Nothing installed or started. Next: sudo -u $USER_NAME -i; cd ~/blockline-usdt"
