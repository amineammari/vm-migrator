from __future__ import annotations

from collections import Counter

from rest_framework import serializers

from .models import DiscoveredVM, MigrationJob


class SelectedVMSerializer(serializers.Serializer):
    name = serializers.CharField(max_length=255)
    source = serializers.ChoiceField(choices=DiscoveredVM.Source.choices)


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

        # Stash for the view so we do not query again.
        self.context["discovered_vm_map"] = discovered_vm_map
        return value


class MigrationJobSummarySerializer(serializers.ModelSerializer):
    class Meta:
        model = MigrationJob
        fields = ("id", "vm_name", "status", "created_at", "updated_at")
