import os
from django.conf import settings

def check_nfs_mounted():
    if not os.path.exists(settings.NFS_BASE_PATH):
        raise Exception(f"NFS not mounted at {settings.NFS_BASE_PATH}")

def prepare_vm_dirs(vm_id):
    vmdk_path = os.path.join(settings.NFS_VMDK_PATH, str(vm_id))
    qcow2_path = os.path.join(settings.NFS_QCOW2_PATH, str(vm_id))
    os.makedirs(vmdk_path, exist_ok=True)
    os.makedirs(qcow2_path, exist_ok=True)
    return vmdk_path, qcow2_path
