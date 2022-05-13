from datetime import datetime
from app.kubencbackup.apphandlers.nextcloudapp import NextcloudAppHandler

import kubencbackup.common.configextractor as conf_ext

from kubencbackup.common.backupconfig import BackupConfigException
from kubencbackup.common.backupconfig import BackupConfig
from kubencbackup.apihandlers.kubernetesapi import K8sApiInstanceConfigException, K8sApiInstanceHandlerException, K8sApiInstanceHandler
from kubencbackup.apihandlers.mariadbapi import MariaDBApiInstanceConfigException, MariaDBApiInstanceHandler, MariaDBApiInstanceHandlerException
from kubencbackup.apihandlers.longhornapi import LonghornApiInstanceConfigException, LonghornApiInstanceHandler, LonghornApiInstanceHandlerException
from kubencbackup.common.configextractor import ConfigExtractorException

def main():
    # Init Environment and kubernetes, longhorn and mariadb api handlers
    try:
        backup_config = BackupConfig()
        k8s_api = K8sApiInstanceHandler(conf_ext.backupconfig_to_k8s_api_instance_config(backup_config))
        mariadb_api = MariaDBApiInstanceHandler(conf_ext.backupconfig_to_mariadb_api_instance_config(backup_config))
        longhorn_api = LonghornApiInstanceHandler(conf_ext.backupconfig_to_longhorn_api_instance_config(backup_config))
    except BackupConfigException as bce:
        print(bce)
        exit(1)
    except K8sApiInstanceHandlerException as kaihe:
        print(kaihe)
        del(k8s_api)
        exit(1)
    except MariaDBApiInstanceHandlerException as maihe:
        print(maihe)
        del(k8s_api)
        del(mariadb_api)
        exit(1)
    except LonghornApiInstanceHandlerException as laihe:
        print(laihe)
        del(k8s_api)
        del(mariadb_api)
        del(longhorn_api)
        exit(1)
    except (ConfigExtractorException, 
            K8sApiInstanceConfigException, 
            MariaDBApiInstanceConfigException,
            LonghornApiInstanceConfigException) as misc_e:
        if ('k8s_api' in locals()) and (k8s_api != None):
            del(k8s_api)
        if ('mariadb_api' in locals()) and (mariadb_api != None):
            del(mariadb_api)
        if ('longhorn_api' in locals()) and (longhorn_api != None):
            del(longhorn_api)
        print(misc_e)
        exit(1)

    # Init Apps handlers
    try:
        nextcloud_app = NextcloudAppHandler(
            config=conf_ext.backupconfig_to_nextcloud_app_config(backup_config)
        )
    except:
        pass



##########################################################################################################################################

    ### Init backup process ###
    try:
        # Enter NextCloud Maintenance Mode
        enter_nextcloud_maintenance_mode(namespace=lhbe.namespace,app_name=lhbe.app_name)

        # Create MariaDB backup
        create_mysqldump_backup(namespace=lhbe.namespace,db_name=db_name,password=lhbe.db_root_password)

        # Create volumes backup suffix 
        backup_name_suffix=datetime.now().strftime("%d-%m-%Y__%H-%M-%S")

    ### Backup volumes (Nextcloud volume, actual MariaDB volume, backup MariaDB volume) ###

        # Create longhorn nextcloud snapshots and backup
        create_volume_backup_or_snapshot(backup_type=lhbe.backup_type, backup_name_suffix=backup_name_suffix, volume_name=lhbe.app_volume_name, namespace=lhbe.namespace)

        # Create longhorn MariaDB actual volume snapshots and backup
        create_mariadb_volume_backup_or_snapshot()

        # Create longhorn MariaDB backup volume snapshots and backup
        create_volume_backup_or_snapshot(backup_type=lhbe.backup_type, backup_name_suffix=backup_name_suffix, volume_name=lhbe.db_backup_volume_name, namespace=lhbe.namespace)

    except BaseException:
        # Exit NextCloud Maintenance Mode
        exit_nextcloud_maintenance_mode(namespace=lhbe.namespace,app_name=lhbe.app_name)
        exit(1)

    # Exit NextCloud Maintenance Mode
    exit_nextcloud_maintenance_mode(namespace=lhbe.namespace,app_name=lhbe.app_name)

    # Clean old backup/snapshot
    clean_old_backup_snapshot()

    return 0


if __name__ == '__main__':
    main()
    exit(0)
