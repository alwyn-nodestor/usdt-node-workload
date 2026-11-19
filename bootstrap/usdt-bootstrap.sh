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
[ "$(uname -s)" = Linux ] || { echo "the bootstrap is Linux-only (the operational scripts work on other platforms)"; exit 1; }
SRC=/home/debian/projects/blockline-usdt
ROOT=/srv/blockline-usdt
REPO=/home/usdt/blockline-usdt
# shellcheck source=../lib/compat.sh
. "$SRC/lib/compat.sh"

echo "== packages"
pkg_install git curl gnupg acl ca-certificates python3

echo "== user and data root"
create_user usdt
chmod 750 /home/usdt
install -d -o usdt -g usdt -m 750 "$ROOT"
for d in node state downloads app clickhouse; do install -d -o usdt -g usdt -m 750 "$ROOT/$d"; done
free_tb=$(awk -v b="$(free_bytes "$ROOT")" 'BEGIN { printf "%.2f", b/1e12 }')
echo "   free disk at $ROOT: $free_tb TB (the extracted snapshot needs about 4.1 TB plus a 0.2 TB reserve)"

echo "== repository"
if [ -d "$REPO/.git" ]; then
  echo "   $REPO exists: left as it is (git pull as usdt to update)"
else
  git -c safe.directory='*' clone -q "$SRC" "$REPO"
  chown -R usdt:usdt "$REPO"
  run_as usdt git -C "$REPO" config user.name "Nodestors Tech"
  run_as usdt git -C "$REPO" config user.email "tech@nodestors.com"
  echo "   cloned into $REPO"
fi
if command -v setfacl >/dev/null 2>&1; then
  setfacl -m u:usdt:x /home/debian /home/debian/projects
  setfacl -R -m u:usdt:rX "$SRC"
else
  chmod o+x /home/debian /home/debian/projects
  chmod -R o+rX "$SRC"
fi
# both forms: `git pull` from the clone checks the .git path of the source
for p in "$SRC" "$SRC/.git"; do
  run_as usdt git config --global --get-all safe.directory | grep -qxF "$p" \
    || run_as usdt git config --global --add safe.directory "$p"
done

echo "== debian may act as usdt"
if command -v setfacl >/dev/null 2>&1; then
  # The node database (about 900,000 files) and ClickHouse's data inherit these through the default ACLs set on their
  # parents, so a re-run skips them instead of rewriting every file's ACL on a slow disk.
  for d in /home/usdt "$ROOT"; do
    find "$d" \( -path "$ROOT/node/output-directory" -o -path "$ROOT/clickhouse/data" \) -prune \
      -o -exec setfacl -m u:debian:rwX {} + 2>/dev/null || echo "   (some files changed during the ACL scan; skipped)"
    find "$d" \( -path "$ROOT/node/output-directory" -o -path "$ROOT/clickhouse/data" \) -prune \
      -o -type d -exec setfacl -m d:u:debian:rwX -m d:u:usdt:rwX {} + 2>/dev/null || true
  done
else
  echo "   setfacl not available; using group permissions (install acl package for fine-grained ACLs)"
  usermod -aG usdt debian 2>/dev/null || true
  for d in /home/usdt "$ROOT"; do
    find "$d" \( -path "$ROOT/node/output-directory" -o -path "$ROOT/clickhouse/data" \) -prune \
      -o -exec chmod g+rwX {} + 2>/dev/null || true
  done
fi

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
