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
    vmware_endpoint_session = models.ForeignKey(
        "VmwareEndpointSession",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="discovered_vms",
    )
    source = models.CharField(max_length=20, choices=Source.choices, db_index=True)
    cpu = models.PositiveIntegerField(null=True, blank=True)
    ram = models.PositiveIntegerField(null=True, blank=True)
    disks = models.JSONField(default=list, blank=True)
    # Extra provider-specific details (eg. ESXi instance UUID, datastore path).
    metadata = models.JSONField(default=dict, blank=True)
    power_state = models.CharField(max_length=64, blank=True, default="")
    last_seen = models.DateTimeField(db_index=True)

    class Meta:
        ordering = ["-last_seen", "name"]
        constraints = [
            models.UniqueConstraint(
                fields=["name", "source", "vmware_endpoint_session"],
                name="uq_discovered_vm_name_source_endpoint",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.name} ({self.source})"


class OpenStackProvisioningRun(models.Model):
    task_id = models.CharField(max_length=255, unique=True, db_index=True)
    state = models.CharField(max_length=32, default="QUEUED")
    message = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return f"OpenStackProvisioningRun {self.task_id} [{self.state}]"


class VmwareEndpointSession(models.Model):
    class TestStatus(models.TextChoices):
        UNKNOWN = "UNKNOWN", "Unknown"
        PASSED = "PASSED", "Passed"
        FAILED = "FAILED", "Failed"

    label = models.CharField(max_length=255, blank=True, default="")
    host = models.CharField(max_length=255)
    port = models.PositiveIntegerField(default=443)
    username = models.CharField(max_length=255)
    password = models.CharField(max_length=1024)
    insecure = models.BooleanField(default=True)
    last_test_status = models.CharField(max_length=16, choices=TestStatus.choices, default=TestStatus.UNKNOWN)
    last_test_message = models.TextField(blank=True, default="")
    last_test_at = models.DateTimeField(null=True, blank=True)
    expires_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return self.label or f"{self.username}@{self.host}:{self.port}"


class OpenstackEndpointSession(models.Model):
    class TestStatus(models.TextChoices):
        UNKNOWN = "UNKNOWN", "Unknown"
        PASSED = "PASSED", "Passed"
        FAILED = "FAILED", "Failed"

    label = models.CharField(max_length=255, blank=True, default="")
    auth_url = models.CharField(max_length=512)
    username = models.CharField(max_length=255)
    password = models.CharField(max_length=1024)
    project_name = models.CharField(max_length=255)
    user_domain_name = models.CharField(max_length=255, default="Default")
    project_domain_name = models.CharField(max_length=255, default="Default")
    region_name = models.CharField(max_length=255, blank=True, default="")
    interface = models.CharField(max_length=64, blank=True, default="")
    identity_api_version = models.CharField(max_length=32, blank=True, default="")
    verify = models.BooleanField(default=False)
    image_endpoint_override = models.CharField(max_length=512, blank=True, default="")
    last_test_status = models.CharField(max_length=16, choices=TestStatus.choices, default=TestStatus.UNKNOWN)
    last_test_message = models.TextField(blank=True, default="")
    last_test_at = models.DateTimeField(null=True, blank=True)
    expires_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        region = self.region_name or "-"
        return self.label or f"{self.username}@{self.project_name} ({region})"

    def to_connect_kwargs(self) -> dict[str, object]:
        kwargs: dict[str, object] = {
            "auth_url": self.auth_url,
            "username": self.username,
            "password": self.password,
            "project_name": self.project_name,
            "user_domain_name": self.user_domain_name,
            "project_domain_name": self.project_domain_name,
            "verify": bool(self.verify),
        }
        if self.region_name:
            kwargs["region_name"] = self.region_name
        if self.interface:
            kwargs["interface"] = self.interface
        if self.identity_api_version:
            kwargs["identity_api_version"] = self.identity_api_version
        if self.image_endpoint_override:
            kwargs["image_endpoint_override"] = self.image_endpoint_override
        return kwargs
