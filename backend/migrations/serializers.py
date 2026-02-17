from __future__ import annotations

from collections import Counter

from rest_framework import serializers

from .models import DiscoveredVM, MigrationJob, OpenstackEndpointSession, VmwareEndpointSession
from .openstack_client import OpenStackClient, OpenStackClientError


class NetworkOverrideSerializer(serializers.Serializer):
    network_id = serializers.CharField(max_length=255, required=False, allow_blank=True)
    network_name = serializers.CharField(max_length=255, required=False, allow_blank=True)
    fixed_ip = serializers.IPAddressField(required=False)


class VMOverridesSerializer(serializers.Serializer):
    flavor_id = serializers.CharField(max_length=255, required=False, allow_blank=True)
    cpu = serializers.IntegerField(required=False, min_value=1)
    ram = serializers.IntegerField(required=False, min_value=1)
    extra_disks_gb = serializers.ListField(
        required=False,
        child=serializers.IntegerField(min_value=1),
        allow_empty=True,
    )
    network = NetworkOverrideSerializer(required=False)
    # Merge/concat is explicitly forbidden: migration keeps 1-to-1 disk topology.
    disk_merge = serializers.BooleanField(required=False, default=False)
    disk_layout_mode = serializers.CharField(required=False, allow_blank=True)

    def validate(self, attrs):
        disk_layout_mode = str(attrs.get("disk_layout_mode", "") or "").strip().lower()
        disk_merge = bool(attrs.get("disk_merge", False))

        if disk_merge or disk_layout_mode in {"merge", "concat", "concatenate"}:
            raise serializers.ValidationError(
                "Disk concatenation/merge is not allowed. Disk architecture must remain unchanged "
                "(1-to-1: same number of disks, same order, no merge)."
            )
        return attrs


class SelectedVMSerializer(serializers.Serializer):
    name = serializers.CharField(max_length=255)
    source = serializers.ChoiceField(choices=DiscoveredVM.Source.choices)
    overrides = VMOverridesSerializer(required=False)


class CreateMigrationFromVMwareSerializer(serializers.Serializer):
    vmware_endpoint_session_id = serializers.IntegerField(min_value=1)
    openstack_endpoint_session_id = serializers.IntegerField(min_value=1)
    vms = SelectedVMSerializer(many=True, allow_empty=False)

    def validate_vms(self, value):
        vmware_endpoint_session_id = self.initial_data.get("vmware_endpoint_session_id")
        openstack_endpoint_session_id = self.initial_data.get("openstack_endpoint_session_id")

        vmware_session = VmwareEndpointSession.objects.filter(id=vmware_endpoint_session_id).first()
        if vmware_session is None:
            raise serializers.ValidationError("Invalid vmware_endpoint_session_id.")
        openstack_session = OpenstackEndpointSession.objects.filter(id=openstack_endpoint_session_id).first()
        if openstack_session is None:
            raise serializers.ValidationError("Invalid openstack_endpoint_session_id.")

        self.context["vmware_endpoint_session"] = vmware_session
        self.context["openstack_endpoint_session"] = openstack_session

        keys = [(item["name"], item["source"]) for item in value]
        duplicates = [k for k, count in Counter(keys).items() if count > 1]
        if duplicates:
            duplicate_repr = [{"name": n, "source": s} for n, s in duplicates]
            raise serializers.ValidationError(
                f"Duplicate VM selections are not allowed: {duplicate_repr}"
            )

        discovered_vm_map = {}
        missing = []
        for item in value:
            key = (item["name"], item["source"])
            vm = DiscoveredVM.objects.filter(
                name=item["name"],
                source=item["source"],
                vmware_endpoint_session_id=vmware_session.id,
            ).first()
            if vm is None:
                missing.append({"name": item["name"], "source": item["source"]})
            else:
                discovered_vm_map[key] = vm

        if missing:
            raise serializers.ValidationError(
                f"Selected VMs not found in discovery data: {missing}"
            )

        # Normalize empty override values to keep job metadata concise and predictable.
        normalized = []
        for item in value:
            override_payload = item.get("overrides")
            if not isinstance(override_payload, dict):
                normalized.append(item)
                continue

            cleaned = {}
            flavor_id = override_payload.get("flavor_id")
            cpu = override_payload.get("cpu")
            ram = override_payload.get("ram")
            extra_disks = override_payload.get("extra_disks_gb")
            network = override_payload.get("network")

            if isinstance(flavor_id, str) and flavor_id.strip():
                cleaned["flavor_id"] = flavor_id.strip()
            if isinstance(cpu, int):
                cleaned["cpu"] = cpu
            if isinstance(ram, int):
                cleaned["ram"] = ram
            if isinstance(extra_disks, list):
                cleaned["extra_disks_gb"] = [int(v) for v in extra_disks if isinstance(v, int) and v > 0]
            if isinstance(network, dict):
                network_id = network.get("network_id")
                network_name = network.get("network_name")
                fixed_ip = network.get("fixed_ip")
                cleaned_network = {}
                if isinstance(network_id, str) and network_id.strip():
                    cleaned_network["network_id"] = network_id.strip()
                if isinstance(network_name, str) and network_name.strip():
                    cleaned_network["network_name"] = network_name.strip()
                if isinstance(fixed_ip, str) and fixed_ip.strip():
                    cleaned_network["fixed_ip"] = fixed_ip.strip()
                if cleaned_network:
                    cleaned["network"] = cleaned_network

            next_item = {**item}
            if cleaned:
                next_item["overrides"] = cleaned
            else:
                next_item.pop("overrides", None)
            normalized.append(next_item)

        value = normalized

        flavor_ids = set()
        network_ids = set()
        for item in value:
            overrides = item.get("overrides") or {}
            if not isinstance(overrides, dict):
                continue
            flavor_id = overrides.get("flavor_id")
            if isinstance(flavor_id, str) and flavor_id.strip():
                flavor_ids.add(flavor_id.strip())
            network = overrides.get("network") or {}
            if isinstance(network, dict):
                network_id = network.get("network_id")
                if isinstance(network_id, str) and network_id.strip():
                    network_ids.add(network_id.strip())

        fixed_ip_checks = []
        has_fixed_ip = any(
            isinstance((item.get("overrides") or {}).get("network"), dict)
            and (item.get("overrides") or {}).get("network", {}).get("fixed_ip")
            for item in value
        )

        if flavor_ids or network_ids or has_fixed_ip:
            try:
                client = OpenStackClient(auth_config=openstack_session.to_connect_kwargs())
                available_flavors = {item.get("id") for item in client.list_flavors() if item.get("id")}
                networks_payload = client.list_networks()
                available_networks = {item.get("id") for item in networks_payload if item.get("id")}
                networks_by_name: dict[str, list[str]] = {}
                for item in networks_payload:
                    name = item.get("name")
                    net_id = item.get("id")
                    if not name or not net_id:
                        continue
                    networks_by_name.setdefault(name, []).append(net_id)
            except OpenStackClientError as exc:
                raise serializers.ValidationError(f"OpenStack validation failed: {exc}") from exc

            invalid_flavors = sorted([fid for fid in flavor_ids if fid not in available_flavors])
            invalid_networks = sorted([nid for nid in network_ids if nid not in available_networks])
            if invalid_flavors or invalid_networks:
                raise serializers.ValidationError(
                    {
                        "flavor_id": invalid_flavors,
                        "network_id": invalid_networks,
                    }
                )

            for item in value:
                overrides = item.get("overrides") or {}
                if not isinstance(overrides, dict):
                    continue
                network = overrides.get("network") or {}
                if not isinstance(network, dict):
                    continue
                fixed_ip = network.get("fixed_ip")
                if not fixed_ip:
                    continue

                network_id = network.get("network_id")
                network_name = network.get("network_name")
                resolved_network_id = None

                if isinstance(network_id, str) and network_id.strip():
                    resolved_network_id = network_id.strip()
                elif isinstance(network_name, str) and network_name.strip():
                    matches = networks_by_name.get(network_name.strip(), [])
                    if len(matches) == 1:
                        resolved_network_id = matches[0]
                    elif len(matches) > 1:
                        fixed_ip_checks.append(
                            f"VM '{item.get('name')}' has ambiguous network name '{network_name}'. Select a network explicitly."
                        )
                        continue
                    else:
                        fixed_ip_checks.append(
                            f"VM '{item.get('name')}' network '{network_name}' not found for fixed IP {fixed_ip}."
                        )
                        continue
                else:
                    fixed_ip_checks.append(
                        f"VM '{item.get('name')}' must select a network to use fixed IP {fixed_ip}."
                    )
                    continue

                try:
                    valid, reason = client.validate_fixed_ip(
                        network_id=resolved_network_id,
                        fixed_ip=str(fixed_ip),
                    )
                except OpenStackClientError as exc:
                    raise serializers.ValidationError(f"OpenStack validation failed: {exc}") from exc
                if not valid:
                    fixed_ip_checks.append(
                        f"VM '{item.get('name')}' fixed IP {fixed_ip} invalid: {reason}"
                    )

        if fixed_ip_checks:
            raise serializers.ValidationError({"fixed_ip": fixed_ip_checks})

        # Stash for the view so we do not query again.
        self.context["discovered_vm_map"] = discovered_vm_map
        return value


class VmwareEndpointConnectSerializer(serializers.Serializer):
    label = serializers.CharField(max_length=255, required=False, allow_blank=True)
    host = serializers.CharField(max_length=255)
    port = serializers.IntegerField(required=False, min_value=1, max_value=65535, default=443)
    username = serializers.CharField(max_length=255)
    password = serializers.CharField(max_length=1024, trim_whitespace=False)
    insecure = serializers.BooleanField(required=False, default=True)


class OpenstackEndpointConnectSerializer(serializers.Serializer):
    label = serializers.CharField(max_length=255, required=False, allow_blank=True)
    auth_url = serializers.CharField(max_length=512)
    username = serializers.CharField(max_length=255)
    password = serializers.CharField(max_length=1024, trim_whitespace=False)
    project_name = serializers.CharField(max_length=255)
    user_domain_name = serializers.CharField(max_length=255, required=False, default="Default")
    project_domain_name = serializers.CharField(max_length=255, required=False, default="Default")
    region_name = serializers.CharField(max_length=255, required=False, allow_blank=True, default="")
    interface = serializers.CharField(max_length=64, required=False, allow_blank=True, default="")
    identity_api_version = serializers.CharField(max_length=32, required=False, allow_blank=True, default="")
    verify = serializers.BooleanField(required=False, default=False)
    image_endpoint_override = serializers.CharField(max_length=512, required=False, allow_blank=True, default="")


class MigrationJobSummarySerializer(serializers.ModelSerializer):
    class Meta:
        model = MigrationJob
        fields = ("id", "vm_name", "status", "created_at", "updated_at")
