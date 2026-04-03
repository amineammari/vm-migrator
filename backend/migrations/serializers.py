from __future__ import annotations

from collections import Counter
from ipaddress import ip_address, ip_network
from typing import Optional

from django.contrib.auth import get_user_model
from rest_framework import serializers

from .models import DiscoveredVM, MigrationJob, OpenstackEndpointSession, VmwareEndpointSession
from .openstack_client import OpenStackClient, OpenStackClientError

User = get_user_model()


def _session_for_user(model_cls, session_id: int, user) -> Optional[object]:
    qs = model_cls.objects.filter(id=session_id)
    if not user or getattr(user, "role", None) != User.Role.SUPER_ADMIN:
        qs = qs.filter(user=user)
    return qs.first()


def _guess_system_disk_index(discovered_vm: DiscoveredVM) -> int:
    disks = discovered_vm.disks if isinstance(discovered_vm.disks, list) else []
    if not disks:
        return 0

    best_index = 0
    best_score = -1
    for idx, disk in enumerate(disks):
        score = 10 if idx == 0 else 0
        if isinstance(disk, dict):
            label = str(disk.get("label", "") or "").strip().lower()
            filename = str(disk.get("filename", "") or disk.get("path", "") or "").strip().lower()
            unit_number = disk.get("unit_number")

            if unit_number == 0:
                score += 100
            if label in {"hard disk 1", "disk 1", "boot disk"}:
                score += 80
            elif "hard disk 1" in label:
                score += 40
            if filename.endswith(".vmdk") or filename.endswith(".qcow2") or filename.endswith("-flat.vmdk"):
                score += 1

        if score > best_score:
            best_score = score
            best_index = idx
    return best_index


class NetworkOverrideSerializer(serializers.Serializer):
    network_id = serializers.CharField(max_length=255, required=False, allow_blank=True)
    network_name = serializers.CharField(max_length=255, required=False, allow_blank=True)
    fixed_ip = serializers.IPAddressField(required=False)


class FloatingIPOverrideSerializer(serializers.Serializer):
    mode = serializers.ChoiceField(choices=("disabled", "auto", "manual"), required=False, default="disabled")
    address = serializers.IPAddressField(required=False)
    external_network_id = serializers.CharField(max_length=255, required=False, allow_blank=True)
    external_network_name = serializers.CharField(max_length=255, required=False, allow_blank=True)
    reuse_existing = serializers.BooleanField(required=False, default=True)

    def validate(self, attrs):
        mode = str(attrs.get("mode", "disabled") or "disabled").strip().lower()
        attrs["mode"] = mode

        external_network_id = str(attrs.get("external_network_id", "") or "").strip()
        external_network_name = str(attrs.get("external_network_name", "") or "").strip()
        if external_network_id:
            attrs["external_network_id"] = external_network_id
        else:
            attrs.pop("external_network_id", None)
        if external_network_name:
            attrs["external_network_name"] = external_network_name
        else:
            attrs.pop("external_network_name", None)

        if mode == "disabled":
            return {"mode": "disabled"}
        return attrs


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
    floating_ip = FloatingIPOverrideSerializer(required=False)
    selected_disk_indexes = serializers.ListField(
        required=False,
        child=serializers.IntegerField(min_value=0),
        allow_empty=False,
    )
    # Backward-compatible flag. When true, it maps to disk_layout_mode=concat.
    disk_merge = serializers.BooleanField(required=False, default=False)
    disk_layout_mode = serializers.CharField(required=False, allow_blank=True)

    def validate(self, attrs):
        disk_layout_mode = str(attrs.get("disk_layout_mode", "") or "").strip().lower()
        disk_merge = bool(attrs.get("disk_merge", False))

        if disk_layout_mode and disk_layout_mode not in {"individual", "concat", "merge", "concatenate"}:
            raise serializers.ValidationError(
                "Invalid disk_layout_mode. Use 'individual' or 'concat'."
            )
        if disk_merge and disk_layout_mode in {"individual"}:
            raise serializers.ValidationError(
                "Conflicting disk settings: disk_merge=true cannot be combined with disk_layout_mode='individual'."
            )
        if disk_merge and not disk_layout_mode:
            attrs["disk_layout_mode"] = "concat"
        elif disk_layout_mode in {"merge", "concatenate"}:
            attrs["disk_layout_mode"] = "concat"
        elif not disk_layout_mode:
            attrs["disk_layout_mode"] = "individual"
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
        request = self.context.get("request")
        user = getattr(request, "user", None)
        vmware_endpoint_session_id = self.initial_data.get("vmware_endpoint_session_id")
        openstack_endpoint_session_id = self.initial_data.get("openstack_endpoint_session_id")

        vmware_session = _session_for_user(VmwareEndpointSession, vmware_endpoint_session_id, user)
        if vmware_session is None:
            raise serializers.ValidationError("Invalid or unauthorized vmware_endpoint_session_id.")
        openstack_session = _session_for_user(OpenstackEndpointSession, openstack_endpoint_session_id, user)
        if openstack_session is None:
            raise serializers.ValidationError("Invalid or unauthorized openstack_endpoint_session_id.")

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
            floating_ip = override_payload.get("floating_ip")
            disk_layout_mode = override_payload.get("disk_layout_mode")
            disk_merge = override_payload.get("disk_merge")
            selected_disk_indexes = override_payload.get("selected_disk_indexes")

            if isinstance(flavor_id, str) and flavor_id.strip():
                cleaned["flavor_id"] = flavor_id.strip()
            if isinstance(cpu, int):
                cleaned["cpu"] = cpu
            if isinstance(ram, int):
                cleaned["ram"] = ram
            if isinstance(extra_disks, list):
                cleaned["extra_disks_gb"] = [int(v) for v in extra_disks if isinstance(v, int) and v > 0]
            if isinstance(disk_layout_mode, str) and disk_layout_mode.strip():
                cleaned["disk_layout_mode"] = disk_layout_mode.strip()
            if isinstance(disk_merge, bool) and disk_merge:
                cleaned["disk_merge"] = True
            if isinstance(selected_disk_indexes, list):
                vm = discovered_vm_map.get((item["name"], item["source"]))
                valid_indexes = sorted(
                    {
                        int(index)
                        for index in selected_disk_indexes
                        if isinstance(index, int)
                        and vm is not None
                        and 0 <= int(index) < len(vm.disks if isinstance(vm.disks, list) else [])
                    }
                )
                if vm is not None:
                    system_disk_index = _guess_system_disk_index(vm)
                    if system_disk_index not in valid_indexes:
                        valid_indexes.insert(0, system_disk_index)
                if valid_indexes:
                    cleaned["selected_disk_indexes"] = valid_indexes
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
            if isinstance(floating_ip, dict):
                mode = floating_ip.get("mode")
                address = floating_ip.get("address")
                external_network_id = floating_ip.get("external_network_id")
                external_network_name = floating_ip.get("external_network_name")
                reuse_existing = floating_ip.get("reuse_existing")
                cleaned_floating_ip = {}
                if isinstance(mode, str) and mode.strip():
                    cleaned_floating_ip["mode"] = mode.strip().lower()
                if isinstance(address, str) and address.strip():
                    cleaned_floating_ip["address"] = address.strip()
                if isinstance(external_network_id, str) and external_network_id.strip():
                    cleaned_floating_ip["external_network_id"] = external_network_id.strip()
                if isinstance(external_network_name, str) and external_network_name.strip():
                    cleaned_floating_ip["external_network_name"] = external_network_name.strip()
                if isinstance(reuse_existing, bool):
                    cleaned_floating_ip["reuse_existing"] = reuse_existing
                if cleaned_floating_ip and cleaned_floating_ip.get("mode", "disabled") != "disabled":
                    cleaned["floating_ip"] = cleaned_floating_ip

            next_item = {**item}
            if cleaned:
                next_item["overrides"] = cleaned
            else:
                next_item.pop("overrides", None)
            normalized.append(next_item)

        value = normalized

        flavor_ids = set()
        network_ids = set()
        floating_external_network_ids = set()
        floating_external_network_names = set()
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
            floating_ip = overrides.get("floating_ip") or {}
            if isinstance(floating_ip, dict):
                external_network_id = floating_ip.get("external_network_id")
                external_network_name = floating_ip.get("external_network_name")
                if isinstance(external_network_id, str) and external_network_id.strip():
                    floating_external_network_ids.add(external_network_id.strip())
                if isinstance(external_network_name, str) and external_network_name.strip():
                    floating_external_network_names.add(external_network_name.strip())

        network_checks = []
        has_fixed_ip = any(
            isinstance((item.get("overrides") or {}).get("network"), dict)
            and (item.get("overrides") or {}).get("network", {}).get("fixed_ip")
            for item in value
        )
        has_floating_ip = any(
            isinstance((item.get("overrides") or {}).get("floating_ip"), dict)
            and str((item.get("overrides") or {}).get("floating_ip", {}).get("mode", "") or "").strip().lower()
            not in {"", "disabled"}
            for item in value
        )

        if flavor_ids or network_ids or has_fixed_ip or floating_external_network_ids or floating_external_network_names or has_floating_ip:
            try:
                client = OpenStackClient(auth_config=openstack_session.to_connect_kwargs())
                available_flavors = {item.get("id") for item in client.list_flavors() if item.get("id")}
                networks_payload = client.list_networks()
                available_networks = {item.get("id") for item in networks_payload if item.get("id")}
                available_external_networks = {
                    item.get("id")
                    for item in networks_payload
                    if item.get("id") and item.get("is_router_external") is True
                }
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

            invalid_external_networks = sorted(
                [nid for nid in floating_external_network_ids if nid not in available_external_networks]
            )
            if invalid_external_networks:
                raise serializers.ValidationError({"floating_ip.external_network_id": invalid_external_networks})

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
                        network_checks.append(
                            f"VM '{item.get('name')}' has ambiguous network name '{network_name}'. Select a network explicitly."
                        )
                        continue
                    else:
                        network_checks.append(
                            f"VM '{item.get('name')}' network '{network_name}' not found for fixed IP {fixed_ip}."
                        )
                        continue
                else:
                    network_checks.append(
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
                    network_checks.append(
                        f"VM '{item.get('name')}' fixed IP {fixed_ip} invalid: {reason}"
                    )

                floating_ip = overrides.get("floating_ip") or {}
                if not isinstance(floating_ip, dict):
                    continue
                mode = str(floating_ip.get("mode", "") or "").strip().lower()
                if mode in {"", "disabled"}:
                    continue

                external_network_name = floating_ip.get("external_network_name")
                if isinstance(external_network_name, str) and external_network_name.strip():
                    matches = networks_by_name.get(external_network_name.strip(), [])
                    external_matches = [match for match in matches if match in available_external_networks]
                    if not external_matches:
                        network_checks.append(
                            f"VM '{item.get('name')}' floating IP external network '{external_network_name}' was not found."
                        )
                    elif len(external_matches) > 1:
                        network_checks.append(
                            f"VM '{item.get('name')}' floating IP external network name '{external_network_name}' is ambiguous."
                        )

                floating_ip_address = floating_ip.get("address")
                external_network_id = floating_ip.get("external_network_id")
                if floating_ip_address:
                    try:
                        valid, reason = client.validate_floating_ip(
                            address=str(floating_ip_address),
                            external_network_id=str(external_network_id).strip() if external_network_id else None,
                        )
                    except OpenStackClientError as exc:
                        raise serializers.ValidationError(f"OpenStack validation failed: {exc}") from exc
                    if not valid:
                        network_checks.append(
                            f"VM '{item.get('name')}' floating IP {floating_ip_address} invalid: {reason}"
                        )

        if network_checks:
            raise serializers.ValidationError({"network": network_checks})

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


class OpenstackNetworkCreateSerializer(serializers.Serializer):
    openstack_endpoint_session_id = serializers.IntegerField(min_value=1, required=False)
    name = serializers.CharField(max_length=255)
    subnet_name = serializers.CharField(max_length=255, required=False, allow_blank=True, default="")
    cidr = serializers.CharField(max_length=64)
    gateway_ip = serializers.CharField(max_length=64, required=False, allow_blank=True, default="")
    enable_dhcp = serializers.BooleanField(required=False, default=True)
    allocation_pool_start = serializers.CharField(max_length=64, required=False, allow_blank=True, default="")
    allocation_pool_end = serializers.CharField(max_length=64, required=False, allow_blank=True, default="")
    dns_nameservers = serializers.ListField(
        required=False,
        allow_empty=True,
        child=serializers.CharField(max_length=64),
        default=list,
    )

    def validate(self, attrs):
        try:
            network = ip_network(str(attrs["cidr"]).strip(), strict=False)
        except ValueError as exc:
            raise serializers.ValidationError({"cidr": f"Invalid CIDR: {exc}"}) from exc

        gateway_ip = str(attrs.get("gateway_ip", "") or "").strip()
        if gateway_ip:
            try:
                gateway = ip_address(gateway_ip)
            except ValueError as exc:
                raise serializers.ValidationError({"gateway_ip": f"Invalid gateway IP: {exc}"}) from exc
            if gateway not in network:
                raise serializers.ValidationError({"gateway_ip": "Gateway IP must belong to the subnet CIDR."})
            attrs["gateway_ip"] = gateway_ip
        else:
            attrs["gateway_ip"] = ""

        pool_start = str(attrs.get("allocation_pool_start", "") or "").strip()
        pool_end = str(attrs.get("allocation_pool_end", "") or "").strip()
        if bool(pool_start) != bool(pool_end):
            raise serializers.ValidationError(
                {"allocation_pool_start": "Provide both allocation pool start and end, or leave both empty."}
            )
        if pool_start and pool_end:
            try:
                start_ip = ip_address(pool_start)
                end_ip = ip_address(pool_end)
            except ValueError as exc:
                raise serializers.ValidationError({"allocation_pool_start": f"Invalid allocation pool IP: {exc}"}) from exc
            if start_ip not in network or end_ip not in network:
                raise serializers.ValidationError({"allocation_pool_start": "Allocation pool must stay inside the subnet CIDR."})
            if start_ip > end_ip:
                raise serializers.ValidationError({"allocation_pool_start": "Allocation pool start must be <= end."})
            attrs["allocation_pool_start"] = pool_start
            attrs["allocation_pool_end"] = pool_end
        else:
            attrs["allocation_pool_start"] = ""
            attrs["allocation_pool_end"] = ""

        dns_nameservers = []
        for value in attrs.get("dns_nameservers", []):
            dns_value = str(value or "").strip()
            if not dns_value:
                continue
            try:
                ip_address(dns_value)
            except ValueError as exc:
                raise serializers.ValidationError({"dns_nameservers": f"Invalid DNS nameserver '{dns_value}': {exc}"}) from exc
            dns_nameservers.append(dns_value)
        attrs["dns_nameservers"] = dns_nameservers
        attrs["subnet_name"] = str(attrs.get("subnet_name", "") or "").strip()
        attrs["name"] = str(attrs["name"]).strip()
        attrs["cidr"] = str(attrs["cidr"]).strip()
        return attrs


class MigrationJobSummarySerializer(serializers.ModelSerializer):
    user = serializers.SerializerMethodField()

    class Meta:
        model = MigrationJob
        fields = (
            "id",
            "user",
            "vm_name",
            "source",
            "destination",
            "status",
            "created_at",
            "updated_at",
        )

    def get_user(self, obj):
        if not obj.user:
            return None
        return {
            "id": obj.user.id,
            "username": obj.user.username,
            "email": obj.user.email,
            "role": getattr(obj.user, "role", ""),
        }


class MigrationJobCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = MigrationJob
        fields = ("id", "vm_name", "source", "destination", "status", "created_at", "updated_at")
        read_only_fields = ("id", "status", "created_at", "updated_at")


class MigrationJobDetailSerializer(MigrationJobSummarySerializer):
    conversion_metadata = serializers.JSONField()

    class Meta(MigrationJobSummarySerializer.Meta):
        fields = MigrationJobSummarySerializer.Meta.fields + ("conversion_metadata",)
