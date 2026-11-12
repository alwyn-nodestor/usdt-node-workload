#!/usr/bin/env bash
# One-time root setup for blockline-usdt. Run once:  sudo bash /home/debian/projects/blockline-usdt/bootstrap/usdt-bootstrap.sh
# Idempotent: safe to run again. Downloads no chain data and starts no node.
#
#   1. packages: curl, acl, gnupg (java-tron release signatures), python3, git
#   2. the unprivileged user `usdt` (no sudo of its own) and the data root /srv/blockline-usdt owned by it
#   3. lets `debian` work on usdt's files (ACLs) and run commands as usdt (sudo -u usdt, no password) — usdt's rights
#      only, never root — and lets debian and usdt start/stop/restart the blockline-usdt units, nothing else
#   4. clones this repository into /home/usdt/blockline-usdt (origin = the debian working copy, so `git pull` updates)
#   5. installs the systemd units (snapshot, node, dashboard), enables the node at boot and starts the status page on
#      127.0.0.1:8288. The node unit refuses to start until the snapshot is verified. No host firewall: the AceCloud security group blocks inbound (PLAN.md 2.4).
#
# Next (as debian):  sudo -u usdt /home/usdt/blockline-usdt/bin/blockline-usdt install
#                    sudo systemctl start blockline-usdt-snapshot     # ~1.5 days; starts the node when verified
set -euo pipefail
[[ $EUID -eq 0 ]] || { echo "run with sudo"; exit 1; }
SRC=/home/debian/projects/blockline-usdt
ROOT=/srv/blockline-usdt
REPO=/home/usdt/blockline-usdt

echo "== packages"
apt-get update -q
DEBIAN_FRONTEND=noninteractive apt-get install -y -q git curl gnupg acl ca-certificates python3

echo "== user and data root"
id usdt >/dev/null 2>&1 || useradd --create-home --shell /bin/bash usdt
chmod 750 /home/usdt
install -d -o usdt -g usdt -m 750 "$ROOT"
for d in node state downloads app clickhouse; do install -d -o usdt -g usdt -m 750 "$ROOT/$d"; done
free_tb=$(df --output=avail -B1 "$ROOT" | tail -1 | awk '{printf "%.2f", $1/1e12}')
echo "   free disk at $ROOT: $free_tb TB (the extracted snapshot needs about 4.1 TB plus a 0.2 TB reserve)"

echo "== repository"
if [ -d "$REPO/.git" ]; then
  echo "   $REPO exists: left as it is (git pull as usdt to update)"
else
  git -c safe.directory='*' clone -q "$SRC" "$REPO"
  chown -R usdt:usdt "$REPO"
  runuser -u usdt -- git -C "$REPO" config user.name "Nodestors Tech"
  runuser -u usdt -- git -C "$REPO" config user.email "tech@nodestors.com"
  echo "   cloned into $REPO"
fi
setfacl -m u:usdt:x /home/debian /home/debian/projects
setfacl -R -m u:usdt:rX "$SRC"
# both forms: `git pull` from the clone checks the .git path of the source
for p in "$SRC" "$SRC/.git"; do
  runuser -u usdt -- git config --global --get-all safe.directory | grep -qxF "$p" \
    || runuser -u usdt -- git config --global --add safe.directory "$p"
done

echo "== debian may act as usdt"
for d in /home/usdt "$ROOT"; do
  setfacl -R -m u:debian:rwX "$d"
  find "$d" -type d -exec setfacl -m d:u:debian:rwX -m d:u:usdt:rwX {} +
done

echo "== systemd units"
for unit in blockline-usdt-snapshot.service blockline-usdt-node.service blockline-usdt-dashboard.service \
            blockline-usdt-clickhouse.service blockline-usdt-etl.service; do
  install -m 644 "$REPO/systemd/$unit" "/etc/systemd/system/$unit"
done
systemctl daemon-reload
systemctl enable blockline-usdt-node.service    # starts at boot once the snapshot is verified; not started now
systemctl enable blockline-usdt-dashboard.service
systemctl restart blockline-usdt-dashboard.service   # read-only status page, 127.0.0.1:8288
systemctl enable blockline-usdt-clickhouse.service    # Tier 2 store; started below once installed
systemctl enable blockline-usdt-etl.service           # live ETL (Phase 1); started below once its venv exists

sudoers_tmp=$(mktemp); trap 'rm -f "$sudoers_tmp"' EXIT
cat > "$sudoers_tmp" <<'RULES'
# blockline-usdt: debian may run commands as the unprivileged usdt user (not as root)
debian ALL=(usdt) NOPASSWD: ALL
# debian and usdt may operate the blockline-usdt units, and nothing else as root
Cmnd_Alias USDT_UNITS = /usr/bin/systemctl start blockline-usdt-snapshot.service, \
  /usr/bin/systemctl stop blockline-usdt-snapshot.service, \
  /usr/bin/systemctl start blockline-usdt-node.service, \
  /usr/bin/systemctl stop blockline-usdt-node.service, \
  /usr/bin/systemctl restart blockline-usdt-node.service, \
  /usr/bin/systemctl start blockline-usdt-snapshot, /usr/bin/systemctl stop blockline-usdt-snapshot, \
  /usr/bin/systemctl start blockline-usdt-node, /usr/bin/systemctl stop blockline-usdt-node, \
  /usr/bin/systemctl restart blockline-usdt-node, \
  /usr/bin/systemctl restart blockline-usdt-dashboard.service, /usr/bin/systemctl restart blockline-usdt-dashboard, \
  /usr/bin/systemctl start blockline-usdt-etl.service, /usr/bin/systemctl stop blockline-usdt-etl.service, \
  /usr/bin/systemctl restart blockline-usdt-etl.service, /usr/bin/systemctl start blockline-usdt-etl, \
  /usr/bin/systemctl stop blockline-usdt-etl, /usr/bin/systemctl restart blockline-usdt-etl, \
  /usr/bin/systemctl start blockline-usdt-clickhouse.service, /usr/bin/systemctl stop blockline-usdt-clickhouse.service, \
  /usr/bin/systemctl restart blockline-usdt-clickhouse.service, /usr/bin/systemctl start blockline-usdt-clickhouse, \
  /usr/bin/systemctl stop blockline-usdt-clickhouse, /usr/bin/systemctl restart blockline-usdt-clickhouse
debian, usdt ALL=(root) NOPASSWD: USDT_UNITS
RULES
# checked before it is installed: a broken file in /etc/sudoers.d would break sudo for everyone
visudo -cf "$sudoers_tmp"
install -m 0440 "$sudoers_tmp" /etc/sudoers.d/blockline-usdt

if [ -x "$ROOT/clickhouse/clickhouse" ]; then systemctl restart blockline-usdt-clickhouse.service; fi
if [ -x "$REPO/.venv/bin/python" ] && [ -x "$ROOT/clickhouse/clickhouse" ]; then systemctl restart blockline-usdt-etl.service; fi

echo "== done. Status page: ssh -L 8288:127.0.0.1:8288 debian@<server>, then http://localhost:8288"
echo "   Next, as debian:"
echo "   sudo -u usdt $REPO/bin/blockline-usdt install"
echo "   sudo systemctl start blockline-usdt-snapshot"
