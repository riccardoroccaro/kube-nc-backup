from kubencbackup.apihandlers.kubernetesapi import K8sApiInstanceHandler, K8sApiInstanceHandlerException
from kubencbackup.apihandlers.longhornapi import LonghornApiInstanceHandler, LonghornApiInstanceHandlerException
from kubencbackup.apihandlers.mariadbapi import MariaDBApiInstanceHandler, MariaDBApiInstanceHandlerException
from kubencbackup.common.backupexceptions import AppConfigException, AppHandlerException
from kubencbackup.common.loggable import Loggable

### Config ###
class MariaDBAppConfigException(AppConfigException):
    def __init__(self,message):
        super().__init__(message)


class MariaDBAppConfig:
    def __init__(self, db_app_name, db_root_password, db_actual_volume_name, db_backup_volume_name, db_backup_file_path):
        self.db_app_name=db_app_name
        self.db_root_password=db_root_password
        self.db_actual_volume_name=db_actual_volume_name
        self.db_backup_volume_name=db_backup_volume_name
        self.db_backup_file_path=db_backup_file_path

    ### db_app_name getter and setter ###
    @property
    def db_app_name(self):
        return self.__db_app_name
    
    @db_app_name.setter
    def db_app_name(self,db_app_name):
        if db_app_name == None:
            raise MariaDBAppConfigException(message='"db_app_name" variable is mandatory')
        self.__db_app_name=db_app_name
    ### END ###

    ### db_root_password getter and setter ###
    @property
    def db_root_password(self):
        return self.__db_root_password
    
    @db_root_password.setter
    def db_root_password(self,db_root_password):
        if db_root_password == None:
            raise MariaDBAppHandlerException(message='db_root_password is mandatory')
        self.__db_root_password=db_root_password
    ### END ###

    ### db_actual_volume_name getter and setter ###
    @property
    def db_actual_volume_name(self):
        return self.__db_actual_volume_name
    
    @db_actual_volume_name.setter
    def db_actual_volume_name(self,db_actual_volume_name):
        if db_actual_volume_name == None:
            raise MariaDBAppConfigException(message='"db_actual_volume_name" variable is mandatory')
        self.__db_actual_volume_name=db_actual_volume_name
    ### END ###

    ### db_backup_volume_name getter and setter ###
    @property
    def db_backup_volume_name(self):
        return self.__db_backup_volume_name
    
    @db_backup_volume_name.setter
    def db_backup_volume_name(self,db_backup_volume_name):
        if db_backup_volume_name == None:
            raise MariaDBAppConfigException(message='"db_backup_volume_name" variable is mandatory')
        self.__db_backup_volume_name=db_backup_volume_name
    ### END ###

    ### db_backup_file_path getter and setter ###
    @property
    def db_backup_file_path(self):
        return self.__db_backup_file_path
    
    @db_backup_file_path.setter
    def db_backup_file_path(self,db_backup_file_path):
        if db_backup_file_path == None:
            raise MariaDBAppConfigException(message='"db_backup_file_path" variable is mandatory')
        self.__db_backup_file_path=db_backup_file_path
    ### END ###
### END - Config ###

### Handler ###
class MariaDBAppHandlerException(AppHandlerException):
    def __init__(self,message):
        super().__init__(message)


class MariaDBAppHandler(Loggable):
    __BACKUP_STAGE_START_CMD="BACKUP STAGE START"
    __BACKUP_STAGE_BLOCK_COMMIT_CMD="BACKUP STAGE BLOCK_COMMIT"
    __BACKUP_STAGE_END_CMD="BACKUP STAGE END"

    __PASSWORD_ESCAPE="____PASSWORD____"
    __FILE_PATH_ESCAPE="____FILE_PATH____"
    __MYSQLDUMP_CMD="mysqldump --add-drop-database --add-drop-table --lock-all-tables --result-file=" + __FILE_PATH_ESCAPE + " --password=" + __PASSWORD_ESCAPE + " --all-databases"

    def __init__(self,config, k8s_api, mariadb_api, longhorn_api):
        super().__init__(name="MARIADB-APP###", log_level=1)

        self.log_info(msg="Initializing MariaDB App Handler...")
        try:
            self.config=config
            self.k8s_api=k8s_api
            self.mariadb_api=mariadb_api
            self.longhorn_api=longhorn_api

            self.__backup_mode=False
        except MariaDBAppHandlerException:
            self.log_err(err="Unable to initialize MariaDB App Handler")
            raise MariaDBAppHandlerException(message="Unable to initialize MariaDB App Handler")
        except:
            self.log_err(err="Unknown error: unable to initialize MariaDB App Handler")
            raise MariaDBAppHandlerException(message="Unknown error: unable to initialize MariaDB App Handler")
        self.log_info("DONE. MariaDB App Handler successfully initialized")

    def __enter__(self):
        try:
            self.log_info(msg="Entering MariaDB backup mode...")
            self.enter_backup_mode()
            self.log_info(msg="DONE. Successfully entered MariaDB bakcup mode")
        except MariaDBAppHandlerException:
            self.log_err(err="Unable to enter MariaDB backup mode")
            self.clean_resources()
            raise MariaDBAppHandlerException(message="Unable to enter MariaDB backup mode")
        except:
            self.log_err(err="Unknown error: unable to enter MariaDB backup mode")
            self.clean_resources()
            raise MariaDBAppHandlerException(message="Unknown error: unable to enter MariaDB backup mode")

        return self

    def __exit__(self, *a):
        self.clean_resources()

    def __del__(self):
        self.clean_resources()

    def clean_resources(self):
        if self.backup_mode:
            try:
                self.log_info(msg="Exiting MariaDB backup mode...")
                self.exit_backup_mode()
                self.log_info(msg="Successfully exited MariaDB backup mode")
            except:
                self.log_err(err="Unable to exit MariaDB backup mode. Check that the DB works well after the end of this process")

    ### config getter and setter ###
    @property
    def config(self):
        return self.__config
    
    @config.setter
    def config(self,config):
        if config == None or type(config) != MariaDBAppConfig:
            self.log_err(err="the configuration object mustn't be None and must be a MariaDBAppConfig instance")
            raise MariaDBAppHandlerException(message='"config" variable is mandatory and must be of type "MariaDBAppConfig"')
        self.__config=config
    ### END ###

    ### k8s_api getter and setter ###
    @property
    def k8s_api(self):
        return self.__k8s_api
    
    @k8s_api.setter
    def k8s_api(self,k8s_api):
        if k8s_api == None or type(k8s_api) != K8sApiInstanceHandler:
            self.log_err(err='"k8s_api" variable is mandatory and must be of type "K8sApiInstanceHandler"')
            raise MariaDBAppHandlerException(message='"k8s_api" variable is mandatory and must be of type "K8sApiInstanceHandler"')
        self.__k8s_api=k8s_api
    ### END ###

    ### mariadb_api getter and setter ###
    @property
    def mariadb_api(self):
        return self.__mariadb_api
    
    @mariadb_api.setter
    def mariadb_api(self,mariadb_api):
        if mariadb_api == None or type(mariadb_api) != MariaDBApiInstanceHandler:
            self.log_err(err='"mariadb_api" variable is mandatory and must be of type "MariaDBApiInstanceHandler"')
            raise MariaDBAppHandlerException(message='"mariadb_api" variable is mandatory and must be of type "MariaDBApiInstanceHandler"')
        self.__mariadb_api=mariadb_api
    ### END ###

    ### longhorn_api getter and setter ###
    @property
    def longhorn_api(self):
        return self.__longhorn_api
    
    @longhorn_api.setter
    def longhorn_api(self,longhorn_api):
        if longhorn_api == None or type(longhorn_api) != LonghornApiInstanceHandler:
            self.log_err(err='"longhorn_api" variable is mandatory and must be of type "LonghornApiInstanceHandler"')
            raise MariaDBAppHandlerException(message='"longhorn_api" variable is mandatory and must be of type "LonghornApiInstanceHandler"')
        self.__longhorn_api=longhorn_api
    ### END ###

    ### backup_mode getter ###
    @property
    def backup_mode(self):
        return self.__backup_mode
    ### END ###

    ### Methods implementation ###
    def create_mariadb_mysqldump(self):
        self.log_info("Creating mysqldump backup file...")
        try:
            resp = self.k8s_api.exec_container_command(
                pod_label="app="+self.config.db_app_name, 
                command=(MariaDBAppHandler.__MYSQLDUMP_CMD)\
                    .replace(MariaDBAppHandler.__PASSWORD_ESCAPE,self.config.db_root_password)\
                    .replace(MariaDBAppHandler.__FILE_PATH_ESCAPE,self.config.db_backup_file_path))
        except K8sApiInstanceHandlerException:
            self.log_err(err="Unable to create mysqldump backup file")
            raise MariaDBAppHandlerException(message="Unable to create mysqldump file")
        except:
            self.log_err(err="Unknown error: unable to create mysqldump backup file")
            raise MariaDBAppHandlerException(message="Unknown error: unable to create mysqldump backup file")

        if resp != "":
            self.log_err(err="Unable to create mysqldump backup file")
            raise MariaDBAppHandlerException(message="Unable to create mysqldump file")
        self.log_info("DONE. mysqldump backup file successfully created")

    def enter_backup_mode(self):
        self.log_info(msg="  Enabling MariaDB backup mode...")
        try:
            if self.backup_mode == False:
                self.mariadb_api.exec_sql_command(MariaDBAppHandler.__BACKUP_STAGE_START_CMD)
                self.mariadb_api.exec_sql_command(MariaDBAppHandler.__BACKUP_STAGE_BLOCK_COMMIT_CMD)
                self.__backup_mode=True
            else:
                self.log_info("  MariaDB backup mode already enabled")
        except MariaDBApiInstanceHandlerException:
            self.log_err(err="Unable to enable MariaDB backup mode")
            raise MariaDBAppHandlerException(message="Unable to enter MariaDB backup mode")
        except:
            self.log_err(err="Unable to create mysqldump backup file")
            raise MariaDBAppHandlerException(message="Unable to create mysqldump file")
        self.log_info(msg="  DONE. Successfully enabled MariaDB backup mode")

    def exit_backup_mode(self):
        self.log_info(msg="  Disabling MariaDB backup mode...")
        try:
            if self.backup_mode == True:
                self.mariadb_api.exec_sql_command(MariaDBAppHandler.__BACKUP_STAGE_END_CMD)
                self.__backup_mode=False
            else:
                self.log_info("  MariaDB backup mode already disabled")
        except MariaDBApiInstanceHandlerException:
            self.log_err(err="Unable to disable backup mode. Check that the DB works well after the end of this process")
            raise MariaDBAppHandlerException(message="Unable to disable backup mode. Check that the DB works well after the end of this process")
        except:
            self.log_err(err="Unknown error: unable to create mysqldump backup file")
            raise MariaDBAppHandlerException(message="Unknown error: unable to create mysqldump backup file")
        self.log_info(msg="  DONE. Successfully disabled MariaDB backup mode")

    def create_actual_volume_snapshot(self, snapshot_name):
        self.log_info(msg="Creating the MariaDB volume snapshot with name " + snapshot_name + "...")
        if self.backup_mode:
            try:
                self.longhorn_api.create_volume_snapshot(
                    snapshot_name=snapshot_name,
                    pv_name=self.k8s_api.get_pv_name_from_pvc_name(pvc_name=self.config.db_actual_volume_name)
                )
            except (K8sApiInstanceHandlerException,LonghornApiInstanceHandlerException):
                self.log_err(err="Unable to create the snapshot")
                raise MariaDBAppHandlerException(message="Unable to create the snapshot.")
            except:
                self.log_err(err="Unknown error: unable to create the snapshot")
                raise MariaDBAppHandlerException(message="Unknown error: unable to create the snapshot")
        else:
            self.log_err(err="MariaDB backup mode not enabled. Cannot continue with the snapshot creation")
            raise MariaDBAppHandlerException(message="MariaDB backup mode not enabled. Cannot continue with the snapshot creation")
        self.log_info(msg="DONE. MariaDB volume snapshot " + snapshot_name + " successfully created")

    def create_actual_volume_backup(self, snapshot_name):
        self.log_info(msg="Creating the MariaDB volume backup from the snapshot" + snapshot_name + "...")
        try:
            self.longhorn_api.create_volume_backup(
                snapshot_name=snapshot_name,
                pv_name=self.k8s_api.get_pv_name_from_pvc_name(pvc_name=self.config.db_actual_volume_name)
            )
        except (K8sApiInstanceHandlerException,LonghornApiInstanceHandlerException):
            self.log_err(err="Unable to create the backup")
            raise MariaDBAppHandlerException(message="Unable to create the backup")
        except:
            self.log_err(err="Unknown error: unable to create the backup")
            raise MariaDBAppHandlerException(message="Unknown error: unable to create the backup")
        self.log_info(msg="DONE. MariaDB volume backup from snapshot " + snapshot_name + " successfully created")

    def create_backup_volume_snapshot(self, snapshot_name):
        self.log_info(msg="Creating the MariaDB backup volume snapshot with name " + snapshot_name + "...")
        try:
            self.longhorn_api.create_volume_snapshot(
                snapshot_name=snapshot_name,
                pv_name=self.k8s_api.get_pv_name_from_pvc_name(pvc_name=self.config.db_backup_volume_name)
            )
        except (K8sApiInstanceHandlerException,LonghornApiInstanceHandlerException):
            self.log_err(err="Unable to create the snapshot")
            raise MariaDBAppHandlerException(message="Unable to create the snapshot")
        except:
            self.log_err(err="Unknown error: unable to create the snapshot")
            raise MariaDBAppHandlerException(message="Unknown error: unable to create the snapshot")
        self.log_info(msg="DONE. MariaDB backup volume snapshot " + snapshot_name + " successfully created")

    def create_backup_volume_backup(self, snapshot_name):
        self.log_info(msg="Creating the MariaDB backup volume backup from the snapshot" + snapshot_name + "...")
        try:
            self.longhorn_api.create_volume_backup(
                snapshot_name=snapshot_name,
                pv_name=self.k8s_api.get_pv_name_from_pvc_name(pvc_name=self.config.db_backup_volume_name)
            )
        except (K8sApiInstanceHandlerException,LonghornApiInstanceHandlerException):
            self.log_err(err="Unable to create the backup")
            raise MariaDBAppHandlerException(message="Unable to create the backup")
        except:
            self.log_err(err="Unknown error: unable to create the backup")
            raise MariaDBAppHandlerException(message="Unknown error: unable to create the backup")
        self.log_info(msg="DONE. MariaDB backup volume backup from snapshot " + snapshot_name + " successfully created")


    def delete_backups_and_snapshots_over_retain_count(self):
        self.log_info(msg="Deleting the MariaDB old backups and snapshots...")
        try:
            self.longhorn_api.delete_backups_and_snapshots_over_retain_count(
                pv_name=self.k8s_api.get_pv_name_from_pvc_name(pvc_name=self.config.db_actual_volume_name)
            )

            self.longhorn_api.delete_backups_and_snapshots_over_retain_count(
                pv_name=self.k8s_api.get_pv_name_from_pvc_name(pvc_name=self.config.db_backup_volume_name)
            )
        except (K8sApiInstanceHandlerException,LonghornApiInstanceHandlerException):
            self.log_err(err="Unable to delete the old snapshots and backups")
            raise MariaDBAppHandlerException(message="Unable to delete the old snapshots and backups")
        except:
            self.log_err(err="Unknown error: unable to delete the old snapshots and backups")
            raise MariaDBAppHandlerException(message="Unknown error: unable to delete the old snapshots and backups")
        self.log_info(msg="DONE. MariaDB oldest volume backups and snapshots successfully deleted")
    ### END - Methods implementation###
### END - Handler ###
