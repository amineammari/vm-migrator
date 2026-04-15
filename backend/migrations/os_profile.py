from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class OSProfile:
    family: str
    distro: str
    display_name: str
    package_manager: str
    connection_method: str
    detection_source: str
    confidence: str
    supported: bool
    notes: list[str] = field(default_factory=list)

    def to_image_properties(self) -> dict[str, str]:
        props = {
            "vmigrate_os_family": self.family,
            "vmigrate_os_distro": self.distro,
            "vmigrate_os_name": self.display_name,
            "vmigrate_os_detection_source": self.detection_source,
            "vmigrate_os_detection_confidence": self.confidence,
        }
        if self.family in {"linux", "windows"}:
            props["os_type"] = self.family
        if self.distro != "unknown":
            props["os_distro"] = self.distro
        return props


class BaseOSHandler:
    def __init__(self, profile: OSProfile) -> None:
        self.profile = profile

    def should_apply_guest_network_remediation(self) -> bool:
        return False

    def remediation_reason(self) -> str:
        return "not_applicable"


class LinuxOSHandler(BaseOSHandler):
    def should_apply_guest_network_remediation(self) -> bool:
        return True

    def remediation_reason(self) -> str:
        return "linux_guest"


class WindowsOSHandler(BaseOSHandler):
    def remediation_reason(self) -> str:
        return "windows_guest_no_linux_remediation"


class UnknownOSHandler(BaseOSHandler):
    def remediation_reason(self) -> str:
        return "unknown_guest_os"


def get_os_handler(profile: OSProfile) -> BaseOSHandler:
    if profile.family == "linux":
        return LinuxOSHandler(profile)
    if profile.family == "windows":
        return WindowsOSHandler(profile)
    return UnknownOSHandler(profile)


def detect_os_profile(discovered_vm, execution: dict[str, Any] | None = None) -> OSProfile:
    metadata = discovered_vm.metadata if isinstance(discovered_vm.metadata, dict) else {}
    summary = metadata.get("summary") if isinstance(metadata.get("summary"), dict) else {}

    candidate_tokens = [
        str(metadata.get("guest_id", "") or ""),
        str(metadata.get("guest_full_name", "") or ""),
        str(summary.get("guest_id", "") or ""),
        str(summary.get("guest_full_name", "") or ""),
    ]

    profile = _detect_from_tokens(candidate_tokens, source="vmware_metadata")
    if profile is not None:
        return profile

    runtime_tokens: list[str] = []
    if isinstance(execution, dict):
        disk_analysis = execution.get("disk_analysis")
        if isinstance(disk_analysis, list):
            for item in disk_analysis:
                if not isinstance(item, dict):
                    continue
                os_names = item.get("os_names")
                if isinstance(os_names, list):
                    runtime_tokens.extend(str(name) for name in os_names if isinstance(name, str))

    profile = _detect_from_tokens(runtime_tokens, source="runtime_inspection")
    if profile is not None:
        return profile

    return OSProfile(
        family="unknown",
        distro="unknown",
        display_name="Unknown OS",
        package_manager="unknown",
        connection_method="unknown",
        detection_source="fallback",
        confidence="low",
        supported=False,
        notes=["OS type could not be inferred from VMware metadata or runtime inspection"],
    )


def _detect_from_tokens(tokens: list[str], source: str) -> OSProfile | None:
    normalized = " ".join(token.strip().lower() for token in tokens if isinstance(token, str) and token.strip())
    if not normalized:
        return None

    if any(k in normalized for k in ["windows", "win", "win2k", "winxp", "win7", "win8", "win10", "win11", "server 20"]):
        return OSProfile(
            family="windows",
            distro="windows",
            display_name="Windows",
            package_manager="n/a",
            connection_method="winrm",
            detection_source=source,
            confidence="high",
            supported=True,
            notes=["Linux guest remediation is skipped for Windows guests"],
        )

    linux_match = _detect_linux_distribution(normalized)
    if linux_match is not None:
        distro, display_name, package_manager = linux_match
        return OSProfile(
            family="linux",
            distro=distro,
            display_name=display_name,
            package_manager=package_manager,
            connection_method="ssh",
            detection_source=source,
            confidence="high",
            supported=True,
            notes=[],
        )

    if any(k in normalized for k in ["linux", "gnu/", "kernel"]):
        return OSProfile(
            family="linux",
            distro="generic",
            display_name="Generic Linux",
            package_manager="unknown",
            connection_method="ssh",
            detection_source=source,
            confidence="medium",
            supported=True,
            notes=["Linux distribution could not be precisely identified"],
        )

    return None


def _detect_linux_distribution(text: str) -> tuple[str, str, str] | None:
    if "ubuntu" in text:
        return ("ubuntu", "Ubuntu", "apt")
    if "debian" in text:
        return ("debian", "Debian", "apt")
    if "centos" in text:
        return ("centos", "CentOS", "yum")
    if "rhel" in text or "red hat" in text or "redhat" in text:
        return ("rhel", "Red Hat Enterprise Linux", "dnf")
    if "rocky" in text:
        return ("rocky", "Rocky Linux", "dnf")
    if "almalinux" in text or "alma" in text:
        return ("almalinux", "AlmaLinux", "dnf")
    if "fedora" in text:
        return ("fedora", "Fedora", "dnf")
    if "suse" in text or "opensuse" in text or "sles" in text:
        return ("suse", "SUSE", "zypper")
    if "arch" in text:
        return ("arch", "Arch Linux", "pacman")
    return None