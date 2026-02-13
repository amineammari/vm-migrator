from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("migrations", "0004_discoveredvm_metadata"),
    ]

    operations = [
        migrations.CreateModel(
            name="OpenStackProvisioningRun",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("task_id", models.CharField(db_index=True, max_length=255, unique=True)),
                ("state", models.CharField(default="QUEUED", max_length=32)),
                ("message", models.TextField(blank=True, default="")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ["-created_at"],
            },
        ),
    ]
