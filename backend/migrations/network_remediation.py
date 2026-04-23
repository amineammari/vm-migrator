from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any


class NetworkRemediationError(Exception):
    """Raised when guest network remediation cannot be applied."""


_SCRIPT_PATH = "/usr/local/sbin/vm-migrator-network-heal"
_UNIT_PATH = "/etc/systemd/system/vm-migrator-network-heal.service"
_WANTS_PATH = "/etc/systemd/system/multi-user.target.wants/vm-migrator-network-heal.service"
_CLOUD_INIT_DISABLE_TMP_PATH = "/tmp/vm-migrator-disable-cloud-init-network.cfg"
_CLOUD_INIT_DISABLE_PATH = "/etc/cloud/cloud.cfg.d/99-disable-network-config.cfg"


def render_network_heal_script() -> str:

    return """#!/bin/sh
set -u
set -o pipefail

PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

LOG_FILE=/var/log/vmigrate-network-fix.log
CLOUD_CFG_FILE=/etc/cloud/cloud.cfg.d/99-disable-network-config.cfg
RESOLVED_CONF=/etc/systemd/resolved.conf
NETPLAN_DIR=/etc/netplan
NETPLAN_FILE=/etc/netplan/99-vmigrate-dns.yaml

DNS_PRIMARY_1=8.8.8.8
DNS_PRIMARY_2=1.1.1.1
DNS_FALLBACK=8.8.4.4

FAIL_COUNT=0
PRIMARY_IFACE=
mkdir -p "$(dirname \"$LOG_FILE\")"
touch "$LOG_FILE"
chmod 0644 "$LOG_FILE" || true
exec >>"$LOG_FILE" 2>&1

timestamp() {
  date '+%Y-%m-%d %H:%M:%S%z'
}

log() {
  printf '[%s] %s\\n' "$(timestamp)" "$*"
  if command -v logger >/dev/null 2>&1; then
    logger -t vm-migrator-network-heal "$*" || true
  fi
}

warn() {
  log "WARN: $*"
}
"""

    return """#!/bin/sh
set -u
set -o pipefail

PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

LOG_FILE=/var/log/vmigrate-network-fix.log
CLOUD_CFG_FILE=/etc/cloud/cloud.cfg.d/99-disable-network-config.cfg
RESOLVED_CONF=/etc/systemd/resolved.conf
NETPLAN_DIR=/etc/netplan
NETPLAN_FILE=/etc/netplan/99-vmigrate-dns.yaml

DNS_PRIMARY_1=8.8.8.8
DNS_PRIMARY_2=1.1.1.1
DNS_FALLBACK=8.8.4.4

FAIL_COUNT=0
PRIMARY_IFACE=

mkdir -p "$(dirname "$LOG_FILE")"
touch "$LOG_FILE"
chmod 0644 "$LOG_FILE" || true
exec >>"$LOG_FILE" 2>&1

timestamp() {
  date '+%Y-%m-%d %H:%M:%S%z'
}

log() {
  printf '[%s] %s\\n' "$(timestamp)" "$*"
  if command -v logger >/dev/null 2>&1; then
    logger -t vm-migrator-network-heal "$*" || true
  fi
}

warn() {
  log "WARN: $*"
}

# --- AGGRESSIVE NETWORK CONFIG CLEANUP ---
cleanup_cloud_init_net_configs() {
  log "Removing old cloud-init and netplan network configs"
  # Remove cloud-init generated network configs
  rm -f /etc/cloud/cloud.cfg.d/*network* || true
  rm -f /etc/cloud/cloud.cfg.d/subiquity* || true
  rm -f /etc/cloud/cloud.cfg.d/50-curtin-networking.cfg || true
  # Remove netplan configs
  rm -f /etc/netplan/*.yaml || true
  # Remove interfaces.d configs
  rm -f /etc/network/interfaces.d/* || true
  # Remove legacy ifupdown config
  [ -f /etc/network/interfaces ] && mv /etc/network/interfaces /etc/network/interfaces.vmigrate.bak.$(date +%s) || true
}

# Call cleanup before any other network steps
cleanup_cloud_init_net_configs
error() {
  log "ERROR: $*"
}

run_step() {
  step_name="$1"
  shift
  log "STEP START: $step_name"
  if "$@"; then
    log "STEP OK: $step_name"
    return 0
  fi
  error "STEP FAILED: $step_name"
  FAIL_COUNT=$((FAIL_COUNT + 1))
  return 1
}

write_if_changed() {
  target="$1"
  content="$2"
  tmp="$(mktemp)"

  printf '%s\\n' "$content" > "$tmp"
  if [ -f "$target" ] && cmp -s "$tmp" "$target"; then
    rm -f "$tmp"
    log "No change needed: $target"
    return 0
  fi

  mkdir -p "$(dirname "$target")"
  install -m 0644 "$tmp" "$target"
  rm -f "$tmp"
  log "Updated: $target"
  return 0
}

set_or_append_kv() {
  file="$1"
  key="$2"
  value="$3"

  touch "$file"
  if grep -Eq "^[[:space:]]*#?[[:space:]]*${key}=" "$file"; then
    sed -i -E "s|^[[:space:]]*#?[[:space:]]*${key}=.*|${key}=${value}|g" "$file"
  else
    printf '%s=%s\\n' "$key" "$value" >> "$file"
  fi
}

candidate_ifaces() {
  ip -o link show | awk -F': ' '{print $2}' | cut -d@ -f1 | while read -r iface; do
    case "$iface" in
      lo|docker*|br-*|virbr*|veth*|vnet*|tap*|tun*|ovs-system)
        continue
        ;;
    esac
    printf '%s\\n' "$iface"
  done
}

try_network_manager() {
  iface="$1"
  command -v nmcli >/dev/null 2>&1 || return 0
  nmcli device set "$iface" managed yes >/dev/null 2>&1 || true
  nmcli device connect "$iface" >/dev/null 2>&1 || true
  nmcli device reapply "$iface" >/dev/null 2>&1 || true
}

try_systemd_networkd() {
  iface="$1"
  command -v networkctl >/dev/null 2>&1 || return 0
  networkctl reconfigure "$iface" >/dev/null 2>&1 || true
  networkctl renew "$iface" >/dev/null 2>&1 || true
}

try_dhcp() {
  iface="$1"
  if command -v dhclient >/dev/null 2>&1; then
    dhclient -4 -1 "$iface" >/dev/null 2>&1 || dhclient -1 "$iface" >/dev/null 2>&1 || true
    return 0
  fi
  if command -v dhcpcd >/dev/null 2>&1; then
    dhcpcd -n "$iface" >/dev/null 2>&1 || true
    return 0
  fi
  if command -v udhcpc >/dev/null 2>&1; then
    udhcpc -n -q -i "$iface" >/dev/null 2>&1 || true
    return 0
  fi
  if command -v wicked >/dev/null 2>&1; then
    wicked ifup "$iface" >/dev/null 2>&1 || true
  fi
}

recover_interface_if_needed() {
  iface="$1"
  ip link set dev "$iface" up >/dev/null 2>&1 || true
  try_network_manager "$iface"
  try_systemd_networkd "$iface"
  try_dhcp "$iface"
}

disable_cloud_init_network_config() {
  write_if_changed "$CLOUD_CFG_FILE" "network: {config: disabled}"
}

detect_primary_interface() {
  iface="$(ip -o -4 route show to default 2>/dev/null | awk '{print $5}' | head -n1 || true)"

  if [ -z "$iface" ]; then
    for candidate in $(candidate_ifaces); do
      recover_interface_if_needed "$candidate"
      iface="$(ip -o -4 route show to default 2>/dev/null | awk '{print $5}' | head -n1 || true)"
      [ -n "$iface" ] && break
    done
  fi

  if [ -z "$iface" ]; then
    iface="$(candidate_ifaces | head -n1 || true)"
  fi

  if [ -z "$iface" ]; then
    error "Unable to detect primary network interface"
    return 1
  fi

  PRIMARY_IFACE="$iface"
  log "Detected primary interface: $PRIMARY_IFACE"
  return 0
}

configure_systemd_resolved() {
  if ! command -v systemctl >/dev/null 2>&1; then
    error "systemctl not found; cannot configure systemd-resolved"
    return 1
  fi

  set_or_append_kv "$RESOLVED_CONF" "DNS" "$DNS_PRIMARY_1 $DNS_PRIMARY_2"
  set_or_append_kv "$RESOLVED_CONF" "FallbackDNS" "$DNS_FALLBACK"

  if systemctl list-unit-files | grep -q '^systemd-resolved.service'; then
    systemctl enable systemd-resolved.service >/dev/null 2>&1 || true
    systemctl restart systemd-resolved.service
    return 0
  fi

  warn "systemd-resolved.service not found"
  return 1
}

ensure_resolv_conf() {
  stub=/run/systemd/resolve/stub-resolv.conf
  real=/run/systemd/resolve/resolv.conf

  if [ -e "$stub" ]; then
    if [ -e /etc/resolv.conf ] && [ ! -L /etc/resolv.conf ]; then
      cp -a /etc/resolv.conf "/etc/resolv.conf.vmigrate.bak.$(date +%s)" || true
    fi
    ln -sfn "$stub" /etc/resolv.conf
    log "Linked /etc/resolv.conf -> $stub"
    return 0
  fi

  if [ -e "$real" ]; then
    ln -sfn "$real" /etc/resolv.conf
    log "Linked /etc/resolv.conf -> $real"
    return 0
  fi

  write_if_changed /etc/resolv.conf "nameserver $DNS_PRIMARY_1
nameserver $DNS_PRIMARY_2
options edns0 trust-ad"
}

update_netplan_dns() {
  if [ -z "$PRIMARY_IFACE" ]; then
    error "PRIMARY_IFACE is empty"
    return 1
  fi

  mkdir -p "$NETPLAN_DIR"
  write_if_changed "$NETPLAN_FILE" "network:
  version: 2
  ethernets:
    $PRIMARY_IFACE:
      nameservers:
        addresses: [$DNS_PRIMARY_1, $DNS_PRIMARY_2]"
}

apply_netplan_safely() {
  if ! command -v netplan >/dev/null 2>&1; then
    warn "netplan not installed; skipping"
    return 0
  fi

  netplan generate
  netplan apply
}

main() {
  log "===== VMigrate network fix started ====="

  run_step "Disable cloud-init network config" disable_cloud_init_network_config || true
  run_step "Detect primary interface" detect_primary_interface || true
  run_step "Configure systemd-resolved" configure_systemd_resolved || true
  run_step "Ensure resolv.conf" ensure_resolv_conf || true
  run_step "Write netplan DNS config" update_netplan_dns || true
  run_step "Apply netplan" apply_netplan_safely || true

  if [ "$FAIL_COUNT" -gt 0 ]; then
    error "Completed with $FAIL_COUNT failed step(s)"
    log "===== VMigrate network fix finished (FAILED) ====="
    exit 1
  fi

  log "All steps completed successfully"
  log "===== VMigrate network fix finished (OK) ====="
  exit 0
}

main "$@"
"""


def render_network_heal_service() -> str:
    return """[Unit]
Description=VM Migrator guest network self-heal
After=local-fs.target NetworkManager.service systemd-networkd.service
Wants=network-pre.target
Before=multi-user.target

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/vm-migrator-network-heal
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
"""


def render_cloud_init_network_disable_config() -> str:
    return """network: {config: disabled}
"""


def apply_guest_network_remediation(
    image_paths: list[str],
    timeout_seconds: int = 300,
    *,
    disable_cloud_init_network_config: bool = False,
) -> dict[str, Any]:
    if shutil.which("virt-customize") is None:
        raise NetworkRemediationError("virt-customize is not installed on the migration host.")

    normalized_paths: list[Path] = []
    for raw in image_paths:
        path = Path(raw).expanduser()
        if not path.exists() or not path.is_file():
            raise NetworkRemediationError(f"Image not found for guest network remediation: {path}")
        normalized_paths.append(path)

    with tempfile.TemporaryDirectory(prefix="vm-migrator-netfix-") as td:
        workdir = Path(td)
        script_path = workdir / "vm-migrator-network-heal"
        unit_path = workdir / "vm-migrator-network-heal.service"
        script_path.write_text(render_network_heal_script(), encoding="utf-8")
        unit_path.write_text(render_network_heal_service(), encoding="utf-8")
        cloud_init_path = workdir / "99-vm-migrator-disable-network-config.cfg"
        if disable_cloud_init_network_config:
            cloud_init_path.write_text(render_cloud_init_network_disable_config(), encoding="utf-8")

        checks: list[dict[str, Any]] = []
        for image_path in normalized_paths:
            cmd = [
                "virt-customize",
                "-a",
                str(image_path),
                "--upload",
                f"{script_path}:{_SCRIPT_PATH}",
                "--upload",
                f"{unit_path}:{_UNIT_PATH}",
                "--run-command",
                f"chmod 0755 {_SCRIPT_PATH}",
                "--run-command",
                "mkdir -p /etc/systemd/system/multi-user.target.wants",
                "--run-command",
                f"ln -sf {_UNIT_PATH} {_WANTS_PATH}",
                "--run-command",
                "rm -f /etc/udev/rules.d/70-persistent-net.rules",
                "--run-command",
                (
                    "if [ -d /etc/sysconfig/network-scripts ]; then "
                    "find /etc/sysconfig/network-scripts -maxdepth 1 -type f -name 'ifcfg-*' "
                    "-exec sed -ri '/^(HWADDR|UUID)=/d' {} +; "
                    "fi"
                ),
            ]
            if disable_cloud_init_network_config:
                cmd.extend(
                    [
                        "--upload",
                        f"{cloud_init_path}:{_CLOUD_INIT_DISABLE_TMP_PATH}",
                        "--run-command",
                        (
                            "if [ -d /etc/cloud/cloud.cfg.d ]; then "
                            f"mv {_CLOUD_INIT_DISABLE_TMP_PATH} {_CLOUD_INIT_DISABLE_PATH}; "
                            f"else rm -f {_CLOUD_INIT_DISABLE_TMP_PATH}; fi"
                        ),
                    ]
                )
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                timeout=max(1, int(timeout_seconds)),
            )
            checks.append(
                {
                    "path": str(image_path),
                    "command": " ".join(cmd),
                    "returncode": result.returncode,
                    "stdout": (result.stdout or "").strip(),
                    "stderr": (result.stderr or "").strip(),
                    "ok": result.returncode == 0,
                }
            )

    failed = [item for item in checks if not item.get("ok")]
    if failed:
        raise NetworkRemediationError(
            "Guest network remediation failed for converted images: "
            + "; ".join(f"{item['path']} (rc={item['returncode']})" for item in failed)
        )

    return {
        "tool": "virt-customize",
        "script_path": _SCRIPT_PATH,
        "service_path": _UNIT_PATH,
        "cloud_init_network_config_disabled": disable_cloud_init_network_config,
        "checks": checks,
        "ok": True,
    }
