from django.core.exceptions import ValidationError
from django.db import models


class InvalidTransitionError(ValidationError):
    """Raised when a state transition is not allowed."""


class MigrationJob(models.Model):
    class Status(models.TextChoices):
        PENDING = "PENDING", "Pending"
        DISCOVERED = "DISCOVERED", "Discovered"
        CONVERTING = "CONVERTING", "Converting"
        UPLOADING = "UPLOADING", "Uploading"
        DEPLOYED = "DEPLOYED", "Deployed"
        VERIFIED = "VERIFIED", "Verified"
        FAILED = "FAILED", "Failed"
        ROLLED_BACK = "ROLLED_BACK", "Rolled Back"

    TRANSITIONS = {
        Status.PENDING: {Status.DISCOVERED, Status.FAILED},
        Status.DISCOVERED: {Status.CONVERTING, Status.FAILED},
        Status.CONVERTING: {Status.UPLOADING, Status.FAILED},
        Status.UPLOADING: {Status.DEPLOYED, Status.FAILED},
        Status.DEPLOYED: {Status.VERIFIED, Status.ROLLED_BACK, Status.FAILED},
        Status.VERIFIED: set(),
        Status.FAILED: {Status.ROLLED_BACK},
        Status.ROLLED_BACK: set(),
    }

    vm_name = models.CharField(max_length=255)
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True,
    )
    conversion_metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return f"{self.vm_name} [{self.status}]"

    def can_transition_to(self, new_status: str) -> bool:
        if new_status not in self.Status.values:
            return False
        return new_status in self.TRANSITIONS.get(self.status, set())

    def transition(self, new_status: str) -> None:
        if new_status not in self.Status.values:
            raise InvalidTransitionError(
                f"Unknown target status '{new_status}'. Allowed values: {', '.join(self.Status.values)}"
            )

        if not self.can_transition_to(new_status):
            allowed = sorted(self.TRANSITIONS.get(self.status, set()))
            raise InvalidTransitionError(
                f"Invalid transition from '{self.status}' to '{new_status}'. "
                f"Allowed targets: {allowed if allowed else 'none'}"
            )

        self.status = new_status
        self.save(update_fields=["status", "updated_at"])


class DiscoveredVM(models.Model):
    class Source(models.TextChoices):
        WORKSTATION = "workstation", "Workstation"
        ESXI = "esxi", "ESXi"

    name = models.CharField(max_length=255)
    source = models.CharField(max_length=20, choices=Source.choices, db_index=True)
    cpu = models.PositiveIntegerField(null=True, blank=True)
    ram = models.PositiveIntegerField(null=True, blank=True)
    disks = models.JSONField(default=list, blank=True)
    power_state = models.CharField(max_length=64, blank=True, default="")
    last_seen = models.DateTimeField(db_index=True)

    class Meta:
        ordering = ["-last_seen", "name"]
        constraints = [
            models.UniqueConstraint(fields=["name", "source"], name="uq_discovered_vm_name_source"),
        ]

    def __str__(self) -> str:
        return f"{self.name} ({self.source})"
