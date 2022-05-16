from __main__ import kubeNCBackup
from common.backupconfig import BackupConfig
from common.backupexceptions import BackupException
from apihandlers.kubernetesapi import K8sApiInstanceHandler
from apihandlers.longhornapi import LonghornApiInstanceHandler
from apihandlers.mariadbapi import MariaDBApiInstanceHandler
from apphandlers.mariadbapp import MariaDBAppHandler
from apphandlers.nextcloudapp import NextcloudAppHandler

class LogException(BackupException):
    def __init__(self,message):
        super().__init__(message)

__SRC_TYPE = {
    K8sApiInstanceHandler : "[KUBERNETES-API]",
    MariaDBApiInstanceHandler : "[MARIADB-API ]",
    LonghornApiInstanceHandler : "[LONGHORN-API ]",
    NextcloudAppHandler : "[NEXTCLOUD-APP ]",
    MariaDBAppHandler : "[MARIADB-APP ]",
    BackupConfig : "[BACKUP-CONFIG ]",
    kubeNCBackup : "[KUBE-NC-BACKUP]"
}

def print_info(obj, msg):
    if type(obj) not in __SRC_TYPE.keys:
        raise LogException(msg="Type not supported.")

    print(__SRC_TYPE[type(obj)] + "[INFO]: " + msg)

def print_err(obj, msg):
    if type(obj) not in __SRC_TYPE.keys:
        raise LogException(msg="Type not supported.")

    print(__SRC_TYPE[type(obj)] + "[INFO]: " + msg)
