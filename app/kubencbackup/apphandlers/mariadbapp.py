from kubencbackup.apihandlers.kubernetesapi import K8sApiInstanceHandlerException
from kubencbackup.apihandlers.kubernetesapi import K8sApiInstanceHandler
from kubencbackup.apihandlers.mariadbapi import MariaDBApiInstanceHandlerException
from kubencbackup.apihandlers.mariadbapi import MariaDBApiInstanceHandler
from kubencbackup.apihandlers.longhornapi import LonghornApiInstanceHandlerException
from kubencbackup.apihandlers.longhornapi import LonghornApiInstanceHandler
from kubencbackup.common.backupexceptions import AppConfigException
from kubencbackup.common.backupexceptions import AppHandlerException

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


class MariaDBAppHandler:
    __BACKUP_STAGE_START_CMD="BACKUP STAGE START"
    __BACKUP_STAGE_BLOCK_COMMIT_CMD="BACKUP STAGE BLOCK_COMMIT"
    __BACKUP_STAGE_END_CMD="BACKUP STAGE BLOCK_COMMIT"

    __PASSWORD_ESCAPE="____PASSWORD____"
    __FILE_PATH_ESCAPE="____FILE_PATH____"
    __MYSQLDUMP_CMD="mysqldump --add-drop-database --add-drop-table --lock-all-tables --result-file=" + __FILE_PATH_ESCAPE + " --password=" + __PASSWORD_ESCAPE + " --all-databases"

    def __init__(self,config, k8s_api, mariadb_api, longhorn_api):
        self.config=config
        self.k8s_api=k8s_api
        self.mariadb_api=mariadb_api
        self.longhorn_api=longhorn_api

    def __enter__(self):
        try:
            self.enter_backup_mode()
        except MariaDBAppHandlerException as e:
            self.exit_backup_mode()
            raise MariaDBAppHandlerException(message=e.message)

        return self

    def __exit__(self, *a):
        self.exit_backup_mode()

    def __del__(self):
        self.exit_backup_mode()

    ### config getter and setter ###
    @property
    def config(self):
        return self.__config
    
    @config.setter
    def config(self,config):
        if config == None or type(config) != MariaDBAppConfig:
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
            raise MariaDBAppHandlerException(message='"longhorn_api" variable is mandatory and must be of type "LonghornApiInstanceHandler"')
        self.__longhorn_api=longhorn_api
    ### END ###

    ### Methods implementation ###
    def create_mariadb_mysqldump(self):
        try:
            resp = self.k8s_api.exec_container_command(
                pod_label="app="+self.config.db_app_name, 
                command=(MariaDBAppHandler.__MYSQLDUMP_CMD)\
                    .replace(MariaDBAppHandler.__PASSWORD_ESCAPE,self.config.db_root_password)\
                    .replace(MariaDBAppHandler.__FILE_PATH_ESCAPE,self.config.db_backup_file_path))
        except K8sApiInstanceHandlerException as e:
            raise MariaDBAppHandlerException(message="Unable to create mysql dumpfile. The issue is the following:\n" + e)

        if resp != "":
            raise MariaDBAppHandlerException(message="Unable to create mysql dumpfile. The issue is the following:\n" + resp)

    def enter_backup_mode(self):
        try:
## TODO Controllare cosa ritornano queste query per fare un controllo ed eventualmente lanciare una eccezione
            res = self.mariadb_api.exec_sql_command(MariaDBAppHandler.__BACKUP_STAGE_START_CMD)
            res = self.mariadb_api.exec_sql_command(MariaDBAppHandler.__BACKUP_STAGE_BLOCK_COMMIT_CMD)
        except MariaDBApiInstanceHandlerException as e:
            raise MariaDBAppHandlerException(message="Unable to enter backup mode. The issue is the following:\n" + e)

    def exit_backup_mode(self):
        try:
## TODO Controllare cosa ritornano queste query per fare un controllo ed eventualmente lanciare una eccezione
            res = self.mariadb_api.exec_sql_command(MariaDBAppHandler.__BACKUP_STAGE_END_CMD)
        except MariaDBApiInstanceHandlerException as e:
            raise MariaDBAppHandlerException(message="Unable to enter backup mode. The issue is the following:\n" + e)

    def create_actual_volume_snapshot(self, snapshot_name):
        try:
            self.longhorn_api.create_volume_snapshot(
                snapshot_name=snapshot_name,
                pv_name=self.k8s_api.get_pv_name_from_pvc_name(pvc_name=self.config.db_actual_volume_name)
            )
        except (K8sApiInstanceHandlerException,LonghornApiInstanceHandlerException) as e:
            raise MariaDBAppHandlerException(message="Unable to create the snapshot. The issue is the following:\n" + e)

    def create_actual_volume_backup(self, snapshot_name):
        try:
            self.longhorn_api.create_volume_backup(
                snapshot_name=snapshot_name,
                pv_name=self.k8s_api.get_pv_name_from_pvc_name(pvc_name=self.config.db_actual_volume_name)
            )
        except (K8sApiInstanceHandlerException,LonghornApiInstanceHandlerException) as e:
            raise MariaDBAppHandlerException(message="Unable to create the backup. The issue is the following:\n" + e)

    def create_backup_volume_snapshot(self, snapshot_name):
        try:
            self.longhorn_api.create_volume_snapshot(
                snapshot_name=snapshot_name,
                pv_name=self.k8s_api.get_pv_name_from_pvc_name(pvc_name=self.config.db_backup_volume_name)
            )
        except (K8sApiInstanceHandlerException,LonghornApiInstanceHandlerException) as e:
            raise MariaDBAppHandlerException(message="Unable to create the snapshot. The issue is the following:\n" + e)

    def create_backup_volume_backup(self, snapshot_name):
        try:
            self.longhorn_api.create_volume_backup(
                snapshot_name=snapshot_name,
                pv_name=self.k8s_api.get_pv_name_from_pvc_name(pvc_name=self.config.db_backup_volume_name)
            )
        except (K8sApiInstanceHandlerException,LonghornApiInstanceHandlerException) as e:
            raise MariaDBAppHandlerException(message="Unable to create the backup. The issue is the following:\n" + e)


    def delete_backups_and_snapshots_over_retain_count(self):
        try:
            self.longhorn_api.delete_backups_and_snapshots_over_retain_count(
                pv_name=self.k8s_api.get_pv_name_from_pvc_name(pvc_name=self.config.db_actual_volume_name)
            )

            self.longhorn_api.delete_backups_and_snapshots_over_retain_count(
                pv_name=self.k8s_api.get_pv_name_from_pvc_name(pvc_name=self.config.db_backup_volume_name)
            )

        except (K8sApiInstanceHandlerException,LonghornApiInstanceHandlerException) as e:
            raise MariaDBAppHandlerException(message="Unable to delete the old snapshots and backups. The issue is the following:\n" + e)
    ### END - Methods implementation###
### END - Handler ###
