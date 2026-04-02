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
_CLOUD_INIT_DISABLE_PATH = "/etc/cloud/cloud.cfg.d/99-vm-migrator-disable-network-config.cfg"


def render_network_heal_script() -> str:
    return """#!/bin/sh
set -eu

PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

log() {
  if command -v logger >/dev/null 2>&1; then
    logger -t vm-migrator-network-heal "$*"
  fi
  printf '%s\\n' "$*" >&2
}

has_default_route() {
  [ -n "$(ip -4 route show default 2>/dev/null)" ] || [ -n "$(ip -6 route show default 2>/dev/null)" ]
}

has_global_address() {
  iface="$1"
  ip -o addr show dev "$iface" scope global 2>/dev/null | grep -q 'inet\\|inet6'
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

main() {
  if has_default_route; then
    log "default route already present; no remediation needed"
    exit 0
  fi

  for iface in $(candidate_ifaces); do
    ip link set dev "$iface" up >/dev/null 2>&1 || true
    try_network_manager "$iface"
    try_systemd_networkd "$iface"
    try_dhcp "$iface"
    sleep 2
    if has_default_route || has_global_address "$iface"; then
      log "network remediation succeeded on $iface"
      exit 0
    fi
  done

  log "network remediation could not establish connectivity"
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
