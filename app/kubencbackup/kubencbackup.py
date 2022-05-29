import os
import sys
import traceback

from datetime import datetime

from kubencbackup.apihandlers.kubernetesapi import K8sApiInstanceHandler, K8sApiInstanceHandlerException
from kubencbackup.apihandlers.longhornapi import LonghornApiInstanceConfigException, LonghornApiInstanceHandler
from kubencbackup.apihandlers.mariadbapi import MariaDBApiInstanceHandler, MariaDBApiInstanceHandlerException
from kubencbackup.apphandlers.nextcloudapp import NextcloudAppHandler, NextcloudAppHandlerException
from kubencbackup.apphandlers.mariadbapp import MariaDBAppHandler, MariaDBAppHandlerException

import kubencbackup.common.configextractor as conf_ext

from kubencbackup.common.backupconfig import BackupConfig
from kubencbackup.common.loggable import Loggable
from kubencbackup.common.textcolor import TextColor

class KubeNCBackup(Loggable):
    def __init__(self):
        # Set the name attribute in superclass for log purpose
        super().__init__(name="KUBE-NC-BACKUP")

        # Check debug mode
        try:
            if os.getenv("DEBUG").lower() != "true":
                self.__debug_mode_enabled = False
            else:
                self.__debug_mode_enabled = True
        except:
            self.__debug_mode_enabled = False
        
        # Init the variables that keep track of the process status to give an appropriate return value and message
        self.__process_status = [0,0,0,0,0]
        self.__process_step = 0

        # Init the traceback list
        self.__tracebacks_list = []

    ### debug_mode_enabled getter###
    @property
    def debug_mode_enabled(self):
        return self.__debug_mode_enabled
    ### END ###

    ### process_step getter ###
    @property
    def process_step(self):
        return self.__process_step
    ### END ###

    def __do_tasks(self):
        # Debug mode on initial log
        if self.debug_mode_enabled:
            self.log_debug("<===== BEGIN exceptions traceback block #" + str(len(self.__tracebacks_list)) + " =====>")

        # Init BackupConfig
        try:
            self.log_info("Retrieving the configuration from the environment variables...")

            raise Exception("Test")

            backup_config = BackupConfig()

            self.log_info("DONE. Configurations successfully retrieved.")
        except:
            self.log_err("Cannot retrieve the config environment variables.")
            self.__new_stacktrace_block(sys.exc_info())
            return

        # Init kubernetes, longhorn and mariadb api handlers with their correspondent connections
        try:
            raise Exception("Test")

            self.log_info("Preparing the system for the snapshots/backups...")
            self.log_info("  Initializing Kubernetes, MariaDB and Longhorn APIs...")
            # raise Exception("Test")
            with \
                K8sApiInstanceHandler(conf_ext.backupconfig_to_k8s_api_instance_config(backup_config)) as k8s_api, \
                MariaDBApiInstanceHandler(conf_ext.backupconfig_to_mariadb_api_instance_config(backup_config)) as mariadb_api, \
                LonghornApiInstanceHandler(conf_ext.backupconfig_to_longhorn_api_instance_config(backup_config)) as longhorn_api:

                raise Exception("Test")
                try:
                    # Create nextcloud app handler and automatically enter maintenance mode
                    self.log_info("DONE. Prepare Nextcloud to be backupped...")
                    raise Exception("Test")
                    with NextcloudAppHandler(config=conf_ext.backupconfig_to_nextcloud_app_config(backup_config),k8s_api=k8s_api,longhorn_api=longhorn_api) as ncah:
                        # Succesfully initialized the APIs and Nextcloud APP => next step
                        self.__process_status[self.process_step] = 1
                        self.__process_step += 1
                        raise Exception("Test")
                        # Create nextcloud snapshot and backup (if required)
                        try:
                            raise Exception("Test")
                            # Set snapshots and backups name
                            snapshot_backup_name=datetime.now().strftime("%d-%m-%Y__%H-%M-%S")

                            # Create nextcloud snapshot
                            self.log_info("DONE. Creating the Nextcloud snapshot...")
                            ncah.create_volume_snapshot(snapshot_name=snapshot_backup_name)

                            # Check whether a simple snapshot or a backup too have to be done
                            if backup_config.backup_type == "FULL-BACKUP":
                                # Create nextcloud backup
                                self.log_info("DONE. Creating the Nextcloud backup...")
                                ncah.create_volume_backup(snapshot_name=snapshot_backup_name)

                            # Succesfully created the Nextcloud snapshot/backup => reducing self.__return_code
                            self.__process_status[self.process_step] = 1
                            self.__process_step += 1
                        except NextcloudAppHandlerException:
                            self.log_err(err="Unable to create the nextcloud volume snapshot/backup")
                            self.__new_stacktrace_block(sys.exc_info())
                            return
                        except:
                            self.log_err(err="Unknown error while creating nextcloud volume snapshot/backup: unable to create the nextcloud volume snapshot/backup")
                            self.__new_stacktrace_block(sys.exc_info())
                            return

                        raise Exception("Test")

                        self.log_info("DONE. Prepare MariaDB to be backupped...")
                        # Create mariadb app handler and automatically enter backup mode
                        try:
                            raise Exception("Test")
                            with MariaDBAppHandler(config=conf_ext.backupconfig_to_mariadb_app_config(backup_config),k8s_api=k8s_api,mariadb_api=mariadb_api,longhorn_api=longhorn_api) as mdbah:
                                raise Exception("Test")
                                try:
                                    raise Exception("Test")
                                    # Create mariadb actual volume snapshot
                                    self.log_info("DONE. Creating the MariaDB actual volume snapshot...")
                                    # raise Exception("Test")
                                    mdbah.create_actual_volume_snapshot(snapshot_name=snapshot_backup_name)

                                    # Check whether a simple snapshot or a backup too have to be done
                                    if backup_config.backup_type == "FULL-BACKUP":
                                        # Create mariadb actual volume backup
                                        self.log_info("DONE. Creating the MariaDB actual volume backup...")
                                        mdbah.create_actual_volume_backup(snapshot_name=snapshot_backup_name)

                                    # Succesfully created the MariaDB actual volume snapshot/backup => reducing self.__return_code
                                    self.__process_status[self.process_step] = 1
                                except MariaDBAppHandlerException:
                                    self.log_err(err="Unable to create the MariaDB actual volume snapshot/backup")
                                    self.__new_stacktrace_block(sys.exc_info())
                                except:
                                    self.log_err(err="Unknown error while creating MariaDB actual volume snapshot/backup: unable to create the MariaDB actual volume snapshot/backup")
                                    self.__new_stacktrace_block(sys.exc_info())
                        except MariaDBAppHandlerException:
                            self.log_err(err="Unable to initialize the MariaDB app")
                            self.__new_stacktrace_block(sys.exc_info())
                        except conf_ext.ConfigExtractorException:
                            self.log_err(err="Error extracting config values from the main BackupConfig object.")
                            self.__new_stacktrace_block(sys.exc_info())
                        except:
                            self.log_err(err="Unknown error while initializing MariaDB app handler: unable to create the MariaDB actual volume snapshot/backup")
                            self.__new_stacktrace_block(sys.exc_info())
                        finally:
                            self.__process_step += 1
                        # The resources above will be released and the correspondent connections closed through the 'with' statement

                        raise Exception("Test")

                        # Create mysqldump file
                        self.log_info("DONE. Create the db backup in SQL format...")
                        try:
                            raise Exception("Test")
                            # Create the mysqldump
                            db_hand = MariaDBAppHandler(
                                        config=conf_ext.backupconfig_to_mariadb_app_config(backup_config),
                                        k8s_api=k8s_api,
                                        mariadb_api=mariadb_api,
                                        longhorn_api=longhorn_api)
                            db_hand.create_mariadb_mysqldump()

                            # Create mariadb backup volume snapshot
                            self.log_info("DONE. Creating the MariaDB backup snapshot...")
                            db_hand.create_backup_volume_snapshot(snapshot_name=snapshot_backup_name)

                            # Check whether a simple snapshot or a backup too have to be done
                            if backup_config.backup_type == "FULL-BACKUP":
                                # Create mariadb backup volume backup
                                self.log_info("DONE. Creating the MariaDB backup volume backup...")
                                db_hand.create_backup_volume_backup(snapshot_name=snapshot_backup_name)

                            # Succesfully created the MariaDB backup volume snapshot/backup => reducing self.__return_code
                            self.__process_status[self.process_step] = 1
                        except MariaDBAppHandlerException:
                            self.log_err(err="Unable to create the MariaDB mysqldump file or 'backup' volume snapshot/backup")
                            self.__new_stacktrace_block(sys.exc_info())
                        except conf_ext.ConfigExtractorException:
                            self.log_err(err="Error extracting config values from the main BackupConfig object.")
                            self.__new_stacktrace_block(sys.exc_info())
                        except:
                            self.log_err(err="Unknown error while creating MariaDB mysqldump file or 'backup' volume snapshot/backup: unable to create the MariaDB backup volume snapshot/backup")
                            self.__new_stacktrace_block(sys.exc_info())
                        finally:
                            self.__process_step += 1

                        raise Exception("Test")

                        # Remove old snapshots and backups only if everithing before has gone right
                        if self.__process_status == [1,1,1,1,0]:
                            try:
                                raise Exception("Test")
                                # Cleanup nextcloud old snapshots and backups
                                self.log_info("DONE. Cleaning up Nextcloud old snapshots and backups...")
                                ncah.delete_backups_and_snapshots_over_retain_count()

                                # Cleanup mariadb old snapshots and backups
                                self.log_info("DONE. Cleaning up MariaDB old snapshots and backups...")
                                db_hand.delete_backups_and_snapshots_over_retain_count()

                                # Succesfully cleaned up the old snapshot/backup => reducing self.__return_code
                                self.__process_status[self.process_step] = 1
                            except NextcloudAppHandlerException:
                                self.log_err(err="Error when deleting nextcloud backups and snapshots: unable to delete old snapshots and backups")
                                self.__new_stacktrace_block(sys.exc_info())
                                return
                            except MariaDBAppHandlerException:
                                self.log_err(err="Error when deleting mariasb backups and snapshots: unable to delete old snapshots and backups")
                                self.__new_stacktrace_block(sys.exc_info())
                                return
                            except:
                                self.log_err(err="Unknown error: unable to delete old snapshots and backups")
                                self.__new_stacktrace_block(sys.exc_info())
                                return
                    # The resources above will be released and the correspondent connections closed through the 'with' statement
                # The resources above will be released and the correspondent connections closed through the 'with' statement
                except NextcloudAppHandlerException:
                    self.log_err("Cannot initialize Nextcloud app handler.")
                    self.__new_stacktrace_block(sys.exc_info())
                    return
                except conf_ext.ConfigExtractorException:
                    self.log_err(err="Error extracting config values from the main BackupConfig object.")
                    self.__new_stacktrace_block(sys.exc_info())
                    return
                except:
                    self.log_err("Unknown error: cannot initialize Nextcloud app handler")
                    self.__new_stacktrace_block(sys.exc_info())
                    return
            self.log_info("DONE. Cleaning up the remaining allocated resources, if any...")
        except K8sApiInstanceHandlerException:
            self.log_err("Cannot initialize Kubernetes API handler.")
            self.__new_stacktrace_block(sys.exc_info())
            return
        except LonghornApiInstanceConfigException:
            self.log_err("Cannot initialize Longhorn API handler.")
            self.__new_stacktrace_block(sys.exc_info())
            return
        except MariaDBApiInstanceHandlerException:
            self.log_err("Cannot initialize MariaDB API handler.")
            self.__new_stacktrace_block(sys.exc_info())
            return
        except conf_ext.ConfigExtractorException:
            self.log_err(err="Error extracting config values from the main BackupConfig object.")
            self.__new_stacktrace_block(sys.exc_info())
            return
        except:
            self.log_err("Unknown error: cannot complete the operations. Note: The operations already done will not be undone. Take care of it on your own.")
            self.__new_stacktrace_block(sys.exc_info())
            return

        return

    def __get_return_value_message(self):
        __2_PREFIX = "DONE. Process completed WITH ERRORS :-| => "
        if self.__process_status == [1,1,1,1,1]:
            return (0, "DONE. Process completed without errors. SEE YOU NEXT TIME ;-)! BYE!")
        elif self.__process_status == [1,1,1,1,0]:
            return (1, __2_PREFIX + "Unable to delete old snapshots and backups")
        elif self.__process_status == [1,1,1,0,0]:
            return (2, __2_PREFIX + "Unable to create MariaDB 'backup' volume snapshot/backup. Nextcloud and MariaDB 'actual' volumes snapshots/backups succesfully created. Nothing has been deleted")
        elif self.__process_status == [1,1,0,1,0]:
            return (3, __2_PREFIX + "Unable to create MariaDB 'actual' volume snapshot/backup. Nextcloud and MariaDB 'backup' volumes snapshots/backups succesfully created. Nothing has been deleted")
        elif self.__process_status == [1,1,0,0,0]:
            return (4, __2_PREFIX + "Unable to create MariaDB 'actual' and 'backup' volumes snapshots/bakups. Nextcloud volume snapshot/backup succesfully created. Nothing has been deleted")
        elif self.__process_status == [1,0,0,0,0] or self.__process_status == [0,0,0,0,0]:
            return (5, "Critical error: unable to preceed. :-( => ABORT! Please check by hands the resources have been succesfully released and cleaned up...")
        else:
            return (6, "Unknown exit status")

    def __print_err_return_status(self):
        if self.debug_mode_enabled:
            i = 0
            for t in self.__tracebacks_list:
                print("\n")
                self.log_debug("##### Exceptions traceback for block #" + str(i) + " #####", False)
                traceback.print_exception(*t)
                self.log_debug("##### END Exception traceback for block #" + str(i) + " #####\n", False)
                i += 1

    def __new_stacktrace_block(self, ex_info):
        if self.debug_mode_enabled:
            self.log_debug("<====== END exceptions traceback block #" + str(len(self.__tracebacks_list)) + " ======>")
            self.__tracebacks_list.append(ex_info)
            self.log_debug("<===== BEGIN exceptions traceback block #" + str(len(self.__tracebacks_list)) + " =====>")
    
    def main(self):
        self.__do_tasks()

        # Process completed
        if self.__process_status == [1,1,1,1,1]:
            # Succesfully completed
            self.log_ok(self.__get_return_value_message()[1])
            return self.__get_return_value_message()[0]
        elif self.__process_status == [1,1,1,0,0] or self.__process_status == [1,1,0,1,0] or self.__process_status == [1,1,0,0,0]:
            # Completed with errors
            self.log_warn(self.__get_return_value_message()[1])
        else:
            # Critical error
            self.log_err(self.__get_return_value_message()[1])
        
        self.__print_err_return_status()
        return self.__get_return_value_message()[0]
