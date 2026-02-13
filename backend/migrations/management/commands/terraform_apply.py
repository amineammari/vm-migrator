from __future__ import annotations

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from migrations.terraform_runner import TerraformRunner, TerraformRunnerError


class Command(BaseCommand):
    help = "Run terraform init/apply for OpenStack baseline infrastructure"

    def add_arguments(self, parser):
        parser.add_argument(
            "--working-dir",
            default=getattr(settings, "TERRAFORM_WORKING_DIR", "terraform"),
            help="Terraform working directory",
        )
        parser.add_argument(
            "--var",
            action="append",
            default=[],
            help="Override terraform variable (format: key=value). Can be repeated.",
        )

    def handle(self, *args, **options):
        if not getattr(settings, "ENABLE_TERRAFORM_INFRA", False):
            raise CommandError("ENABLE_TERRAFORM_INFRA is false. Refusing to run terraform apply.")

        var_overrides = {}
        for entry in options["var"]:
            if "=" not in entry:
                raise CommandError(f"Invalid --var '{entry}'. Expected key=value")
            key, value = entry.split("=", 1)
            var_overrides[key.strip()] = value.strip()

        default_vars = dict(getattr(settings, "TERRAFORM_DEFAULT_VARS", {}))
        default_vars.update(var_overrides)

        runner = TerraformRunner(binary=getattr(settings, "TERRAFORM_BIN", "terraform"))
        try:
            result = runner.apply(
                working_dir=options["working_dir"],
                var_overrides=default_vars,
                timeout_seconds=int(getattr(settings, "TERRAFORM_TIMEOUT_SECONDS", 1800)),
                auto_approve=True,
            )
        except TerraformRunnerError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(self.style.SUCCESS("Terraform apply completed successfully."))
        self.stdout.write(f"Outputs: {result.get('outputs', {})}")
