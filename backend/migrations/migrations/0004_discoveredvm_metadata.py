from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("migrations", "0003_migrationjob_conversion_metadata"),
    ]

    operations = [
        migrations.AddField(
            model_name="discoveredvm",
            name="metadata",
            field=models.JSONField(blank=True, default=dict),
        ),
    ]

