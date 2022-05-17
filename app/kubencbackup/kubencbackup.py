from datetime import datetime

from kubencbackup.apihandlers.kubernetesapi import K8sApiInstanceHandler
from kubencbackup.apihandlers.longhornapi import LonghornApiInstanceHandler
from kubencbackup.apihandlers.mariadbapi import MariaDBApiInstanceHandler
from kubencbackup.apphandlers.nextcloudapp import NextcloudAppHandler
from kubencbackup.apphandlers.mariadbapp import MariaDBAppHandler

import kubencbackup.common.configextractor as conf_ext

from kubencbackup.common.backupexceptions import AppHandlerException, BackupException
from kubencbackup.common.backupconfig import BackupConfig, BackupConfigException
from kubencbackup.common.loggable import Loggable

class KubeNCBackup(Loggable):
    def __init__(self):
        # Set the name attribute in superclass for log purpose
        super().__init__(name="KUBE-NC-BACKUP")

    def main(self):
        # Init BackupConfig
        try:
            self.log_info("Retrieving the configuration from the environment variables...")

            backup_config = BackupConfig()

            self.log_info("DONE. Configurations succesfully retrieved.")
        except BackupConfigException as bce:
            self.log_err("Cannot retrieve the config environment variables.")
            exit(1)

        # Init kubernetes, longhorn and mariadb api handlers with their correspondent connections
        try:
            self.log_info("Preparing the system for the backups...")
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
        except conf_ext.ConfigExtractorException:
            self.log_err(err="Error extracting config values from the main BackupConfig object.")
        except BackupException as e:
            self.log_err("Cannot complete the operations. Note: The operations already done will not be undone. Take care of it on your own.")
        finally:
            return 1

        return 0
