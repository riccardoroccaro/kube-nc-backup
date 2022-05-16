from datetime import datetime

from common.backupconfig import BackupConfigException
from common.backupconfig import BackupConfig
from apihandlers.kubernetesapi import K8sApiInstanceHandler
from apihandlers.mariadbapi import MariaDBApiInstanceHandler
from apihandlers.longhornapi import LonghornApiInstanceHandler

import common.configextractor as conf_ext

from common.backupexceptions import AppHandlerException, BackupException
from apphandlers.nextcloudapp import NextcloudAppHandler
from apphandlers.mariadbapp import MariaDBAppHandler

def main():
    # Init BackupConfig
    try:
        backup_config = BackupConfig()
    except BackupConfigException as bce:
        print(bce)
        exit(1)

    # Init kubernetes, longhorn and mariadb api handlers with their correspondent connections
    try:
        with \
            K8sApiInstanceHandler(conf_ext.backupconfig_to_k8s_api_instance_config(backup_config)) as k8s_api, \
            MariaDBApiInstanceHandler(conf_ext.backupconfig_to_mariadb_api_instance_config(backup_config)) as mariadb_api, \
            LonghornApiInstanceHandler(conf_ext.backupconfig_to_longhorn_api_instance_config(backup_config)) as longhorn_api:

            # Create nextcloud app handler and automatically enter maintenance mode
            with NextcloudAppHandler(config=conf_ext.backupconfig_to_nextcloud_app_config(backup_config),k8s_api=k8s_api,longhorn_api=longhorn_api) as ncah:
                # Create mysqldump file
                try:
                    MariaDBAppHandler(
                        config=conf_ext.backupconfig_to_mariadb_app_config(backup_config),
                        k8s_api=k8s_api,
                        mariadb_api=mariadb_api,
                        longhorn_api=longhorn_api).create_mariadb_mysqldump()
                except BackupException as e:
                    print(e)
                    return 1

                # Create mariadb app handler and automatically enter backup mode
                with MariaDBAppHandler(config=conf_ext.backupconfig_to_mariadb_app_config(backup_config),k8s_api=k8s_api,mariadb_api=mariadb_api,longhorn_api=longhorn_api) as mdbah:
                    # Set snapshots and backups name
                    snapshot_backup_name=datetime.now().strftime("%d-%m-%Y__%H-%M-%S")
                    
                    try:
                        # Create nextcloud snapshot
                        ncah.create_volume_snapshot(snapshot_name=snapshot_backup_name)

                        # Create mariadb actual volume snapshot
                        mdbah.create_actual_volume_snapshot(snapshot_name=snapshot_backup_name)

                        # Create mariadb backup volume snapshot
                        mdbah.create_backup_volume_snapshot(snapshot_name=snapshot_backup_name)

                        # Check whether a simple snapshot or a backup too have to be done
                        if backup_config.backup_type == "FULL-BACKUP":
                            # Create nextcloud backup
                            ncah.create_volume_backup(snapshot_name=snapshot_backup_name)

                            # Create mariadb actual volume backup
                            mdbah.create_actual_volume_backup(snapshot_name=snapshot_backup_name)

                            # Create mariadb backup volume backup
                            mdbah.create_backup_volume_backup(snapshot_name=snapshot_backup_name)

                        # Cleanup nextcloud old snapshots and backups
                        ncah.delete_backups_and_snapshots_over_retain_count()

                        # Cleanup mariadb old snapshots and backups
                        mdbah.delete_backups_and_snapshots_over_retain_count()

                    except AppHandlerException as ahe:
                        print(ahe)
                        return 1
            # The resources will be freed and the correspondent connections closed through the 'with' statement
        # The resources will be freed and the correspondent connections closed through the 'with' statement
    except BackupException as e:
        print(e)
        return 1

    return 0


if __name__ == '__main__':
    exit(main())
