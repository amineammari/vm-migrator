from django.conf import settings
from django.db import migrations, models
import django_cryptography.fields


class Migration(migrations.Migration):
    dependencies = [
        ("migrations", "0009_migrationjob_user"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.AddField(
            model_name="openstackendpointsession",
            name="user",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=models.SET_NULL,
                related_name="openstack_endpoint_sessions",
                to=settings.AUTH_USER_MODEL,
            ),
        ),
        migrations.AddField(
            model_name="vmwareendpointsession",
            name="user",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=models.SET_NULL,
                related_name="vmware_endpoint_sessions",
                to=settings.AUTH_USER_MODEL,
            ),
        ),
        migrations.AddField(
            model_name="openstackprovisioningrun",
            name="user",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=models.SET_NULL,
                related_name="openstack_provisioning_runs",
                to=settings.AUTH_USER_MODEL,
            ),
        ),
        migrations.AlterField(
            model_name="openstackendpointsession",
            name="password",
            field=django_cryptography.fields.encrypt(models.CharField(max_length=1024)),
        ),
        migrations.AlterField(
            model_name="vmwareendpointsession",
            name="password",
            field=django_cryptography.fields.encrypt(models.CharField(max_length=1024)),
        ),
    ]
