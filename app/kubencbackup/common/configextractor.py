from common.backupexceptions import BackupException
from common.backupconfig import BackupConfig
from apihandlers.kubernetesapi import K8sApiInstanceConfig
from apihandlers.mariadbapi import MariaDBApiInstanceConfig
from apihandlers.longhornapi import LonghornApiInstanceConfig
from apphandlers.nextcloudapp import NextcloudAppConfig
from apphandlers.mariadbapp import MariaDBAppConfig

class ConfigExtractorException(BackupException):
    def __init__(self,message):
        super().__init__(message)

def backupconfig_to_k8s_api_instance_config(backupconfig):
    if type(backupconfig) != BackupConfig:
        raise ConfigExtractorException(message="Wrong backupconfig object type. Must be BackupConfig.")

    return K8sApiInstanceConfig(
        namespace=backupconfig.namespace
    )

def backupconfig_to_mariadb_api_instance_config(backupconfig):
    if type(backupconfig) != BackupConfig:
        raise ConfigExtractorException(message="Wrong backupconfig object type. Must be BackupConfig.")

    return MariaDBApiInstanceConfig(
        db_url=backupconfig.db_url,
        db_port=backupconfig.db_port,
        db_root_password=backupconfig.db_root_password
    )

def backupconfig_to_longhorn_api_instance_config(backupconfig):
    if type(backupconfig) != BackupConfig:
        raise ConfigExtractorException(message="Wrong backupconfig object type. Must be BackupConfig.")

    return LonghornApiInstanceConfig(
        longhorn_url=backupconfig.longhorn_url,
        nr_snapshots_to_retain=backupconfig.nr_snapshots_to_retain,
        nr_backups_to_retain=backupconfig.nr_backups_to_retain
    )

def backupconfig_to_nextcloud_app_config(backupconfig):
    if type(backupconfig) != BackupConfig:
        raise ConfigExtractorException(message="Wrong backupconfig object type. Must be BackupConfig.")

    return NextcloudAppConfig(
        app_name=backupconfig.app_name,
        app_volume_name=backupconfig.app_volume_name
    )

def backupconfig_to_mariadb_app_config(backupconfig):
    if type(backupconfig) != BackupConfig:
        raise ConfigExtractorException(message="Wrong backupconfig object type. Must be BackupConfig.")
    
    return MariaDBAppConfig(
        db_app_name=backupconfig.db_app_name,
        db_root_password=backupconfig.db_root_password,
        db_actual_volume_name=backupconfig.db_actual_volume_name,
        db_backup_volume_name=backupconfig.db_backup_volume_name,
        db_backup_file_path=backupconfig.db_backup_file_path
    )
