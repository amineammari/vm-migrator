from __future__ import annotations

from collections import Counter

from rest_framework import serializers

from .models import DiscoveredVM, MigrationJob


class NetworkOverrideSerializer(serializers.Serializer):
    network_name = serializers.CharField(max_length=255, required=False, allow_blank=True)
    fixed_ip = serializers.IPAddressField(required=False)


class VMOverridesSerializer(serializers.Serializer):
    cpu = serializers.IntegerField(required=False, min_value=1)
    ram = serializers.IntegerField(required=False, min_value=1)
    extra_disks_gb = serializers.ListField(
        required=False,
        child=serializers.IntegerField(min_value=1),
        allow_empty=True,
    )
    network = NetworkOverrideSerializer(required=False)


class SelectedVMSerializer(serializers.Serializer):
    name = serializers.CharField(max_length=255)
    source = serializers.ChoiceField(choices=DiscoveredVM.Source.choices)
    overrides = VMOverridesSerializer(required=False)


class CreateMigrationFromVMwareSerializer(serializers.Serializer):
    vms = SelectedVMSerializer(many=True, allow_empty=False)

    def validate_vms(self, value):
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
            vm = DiscoveredVM.objects.filter(name=item["name"], source=item["source"]).first()
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
            cpu = override_payload.get("cpu")
            ram = override_payload.get("ram")
            extra_disks = override_payload.get("extra_disks_gb")
            network = override_payload.get("network")

            if isinstance(cpu, int):
                cleaned["cpu"] = cpu
            if isinstance(ram, int):
                cleaned["ram"] = ram
            if isinstance(extra_disks, list):
                cleaned["extra_disks_gb"] = [int(v) for v in extra_disks if isinstance(v, int) and v > 0]
            if isinstance(network, dict):
                network_name = network.get("network_name")
                fixed_ip = network.get("fixed_ip")
                cleaned_network = {}
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

        # Stash for the view so we do not query again.
        self.context["discovered_vm_map"] = discovered_vm_map
        return value


class MigrationJobSummarySerializer(serializers.ModelSerializer):
    class Meta:
        model = MigrationJob
        fields = ("id", "vm_name", "status", "created_at", "updated_at")
