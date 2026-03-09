from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("migrations", "0006_openstackendpointsession_vmwareendpointsession_and_more"),
    ]

    operations = [
        migrations.AlterField(
            model_name="migrationjob",
            name="status",
            field=models.CharField(
                choices=[
                    ("PENDING", "Pending"),
                    ("DISCOVERED", "Discovered"),
                    ("PRECHECK", "Precheck"),
                    ("SNAPSHOT_CREATED", "Snapshot Created"),
                    ("DISK_ANALYZING", "Disk Analyzing"),
                    ("CONVERTING", "Converting"),
                    ("BLOCK_VALIDATING", "Block Validating"),
                    ("UPLOADING", "Uploading"),
                    ("DEPLOYED", "Deployed"),
                    ("VERIFIED", "Verified"),
                    ("FAILED", "Failed"),
                    ("ROLLED_BACK", "Rolled Back"),
                ],
                db_index=True,
                default="PENDING",
                max_length=20,
            ),
        ),
    ]

