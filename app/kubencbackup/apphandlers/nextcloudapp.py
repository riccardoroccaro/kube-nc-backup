from time import sleep

from kubencbackup.apihandlers.kubernetesapi import K8sApiInstanceHandler
from kubencbackup.apihandlers.longhornapi import LonghornApiInstanceHandler
from kubencbackup.common.backupexceptions import AppConfigException, AppHandlerException
from kubencbackup.common.loggable import Loggable

### Config ###
class NextcloudAppConfigException(AppConfigException):
    def __init__(self,message):
        super().__init__(message)


class NextcloudAppConfig:
    def __init__(self, app_name, app_volume_name):
        self.app_name=app_name
        self.app_volume_name=app_volume_name

    ### app_name getter and setter ###
    @property
    def app_name(self):
        return self.__app_name
    
    @app_name.setter
    def app_name(self,app_name):
        if app_name == None:
            raise NextcloudAppConfigException(message='"app_name" variable is mandatory')
        self.__app_name=app_name
    ### END ###

    ### app_volume_name getter and setter ###
    @property
    def app_volume_name(self):
        return self.__app_volume_name
    
    @app_volume_name.setter
    def app_volume_name(self,app_volume_name):
        if app_volume_name == None:
            raise NextcloudAppConfigException(message='"app_volume_name" variable is mandatory')
        self.__app_volume_name=app_volume_name
    ### END ###

### END - Config ###

### Handler ###
class NextcloudAppHandlerException(AppHandlerException):
    def __init__(self,message):
        super().__init__(message)


class NextcloudAppHandler(Loggable):
    # Declaring class private const
    __ENTER_MAINTENANCE_CMD='runuser -u www-data -- php occ maintenance:mode --on'
    __EXIT_MAINTENANCE_CMD='runuser -u www-data -- php occ maintenance:mode --off'

    def __init__(self, config, k8s_api, longhorn_api):
        super().__init__(name="NEXTCLOUD-APP#", log_level=1)

        self.log_info(msg="Initializing Nextcloud App Handler...")
        try:
            self.config=config
            self.k8s_api=k8s_api
            self.longhorn_api=longhorn_api

            self.__is_maintenance_mode_enabled = False
        except:
            self.log_err(err="Unable to initialize Nextcloud App Handler")
            raise NextcloudAppHandlerException(message="Unable to initialize Nextcloud App Handler")

    def __enter__(self):
        try:
            self.log_info(msg="Entering Nextcloud maintenance mode...")
            self.enter_maintenance_mode()
            self.log_info(msg="DONE. successfully entered Nextcloud maintenance mode")
        except:
            self.log_err(err="Unable to enter Nextcloud maintenance mode")
            self.clean_resources()
            raise NextcloudAppHandlerException(message="Unable to enter Nextcloud maintenance mode")

        return self

    def __exit__(self, *a):
        self.clean_resources()

    def __del__(self):
        self.clean_resources()

    def clean_resources(self):
        if self.is_maintenance_mode_enabled:
            try:
                self.log_info(msg="Exiting Nextcloud maintenance mode...")
                self.exit_maintenance_mode()
                self.log_info(msg="Successfully exited Nextcloud maintenance mode")
            except:
                self.log_err(err="Unable to exit Nextcloud maintenance mode. You have to do it on your own")
    
    ### config getter and setter ###
    @property
    def config(self):
        return self.__config
    
    @config.setter
    def config(self,config):
        if config == None or type(config) != NextcloudAppConfig:
            self.log_err(err="the configuration object mustn't be None and must be a NextcloudAppConfig instance")
            raise NextcloudAppHandlerException(message='"config" variable is mandatory and must be of type "NextcloudAppConfig"')
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
            raise NextcloudAppHandlerException(message='"k8s_api" variable is mandatory and must be of type "K8sApiInstanceHandler"')
        self.__k8s_api=k8s_api
    ### END ###

    ### longhorn_api getter and setter ###
    @property
    def longhorn_api(self):
        return self.__longhorn_api
    
    @longhorn_api.setter
    def longhorn_api(self,longhorn_api):
        if longhorn_api == None or type(longhorn_api) != LonghornApiInstanceHandler:
            self.log_err(err='"longhorn_api" variable is mandatory and must be of type "LonghornApiInstanceHandler"')
            raise NextcloudAppHandlerException(message='"longhorn_api" variable is mandatory and must be of type "LonghornApiInstanceHandler"')
        self.__longhorn_api=longhorn_api
    ### END ###

    ### is_maintenance_mode_enabled getter ###
    @property
    def is_maintenance_mode_enabled(self):
        return self.__is_maintenance_mode_enabled
    ### END ###

    ### Methods implementation ###
    def enter_maintenance_mode(self):
        self.log_info(msg="Enabling Netcloud maintenance mode...")
        try:
            resp = self.k8s_api.exec_container_command(pod_label="app="+self.config.app_name, command=NextcloudAppHandler.__ENTER_MAINTENANCE_CMD)
        except:
            self.log_err(err="Unable to enable Nextcloud maintenance mode")
            raise NextcloudAppHandlerException(message="Unable to enter maintenance mode")
        
        if resp == "Maintenance mode enabled\n":
            sleep(30)
        else:
            self.log_err(err="Unable to enable Nextcloud maintenance mode")
            raise NextcloudAppHandlerException(message="Unable to enter maintenance mode")

        self.__is_maintenance_mode_enabled = True
        self.log_info(msg="DONE. Nextcloud maintenance mode successfully enabled")

    def exit_maintenance_mode(self):
        self.log_info(msg="Disabling Netcloud maintenance mode...")
        try:
            resp = self.k8s_api.exec_container_command(pod_label="app="+self.config.app_name, command=NextcloudAppHandler.__EXIT_MAINTENANCE_CMD)
        except:
            self.log_err(err="Unable to disable Nextcloud maintenance mode. You have to do it by hands")
            raise NextcloudAppHandlerException(message="Unable to exit maintenance mode. You have to do it by hands")
        
        if resp != "Maintenance mode disabled\n":
            self.log_err(err="Unable to disable Nextcloud maintenance mode. You have to do it by hands")
            raise NextcloudAppHandlerException(message="Unable to exit maintenance mode. You have to do it by hands")

        self.__is_maintenance_mode_enabled = False
        self.log_info(msg="DONE. Nextcloud maintenance mode successfully disabled")

    def create_volume_snapshot(self, snapshot_name):
        self.log_info(msg="Creating the nextcloud volume snapshot with name " + snapshot_name + "...")
        if self.is_maintenance_mode_enabled:
            try:
                self.longhorn_api.create_volume_snapshot(
                    snapshot_name=snapshot_name,
                    pv_name=self.k8s_api.get_pv_name_from_pvc_name(pvc_name=self.config.app_volume_name)
                )
            except:
                self.log_err(err="Unable to create the snapshot")
                raise NextcloudAppHandlerException(message="Unable to create the snapshot")
        else:
            self.log_err(err="Nextcloud maintenance mode not enabled. Cannot continue with the snapshot creation")
            raise NextcloudAppHandlerException(message="Nextcloud maintenance mode not enabled. Cannot continue with the snapshot creation")
        self.log_info(msg="DONE. Nextcloud volume snapshot " + snapshot_name + " successfully created")

    def create_volume_backup(self, snapshot_name):
        self.log_info(msg="Creating the nextcloud volume backup from the snapshot" + snapshot_name + "...")
        if self.is_maintenance_mode_enabled:
            try:
                self.longhorn_api.create_volume_backup(
                    snapshot_name=snapshot_name,
                    pv_name=self.k8s_api.get_pv_name_from_pvc_name(pvc_name=self.config.app_volume_name)
                )
            except:
                self.log_err(err="Unable to create the backup")
                raise NextcloudAppHandlerException(message="Unable to create the backup")
        else:
            self.log_err(err="Nextcloud maintenance mode not enabled. Cannot continue with the backup creation")
            raise NextcloudAppHandlerException(message="Nextcloud maintenance mode not enabled. Cannot continue with the backup creation")
        self.log_info(msg="DONE. Nextcloud volume backup from snapshot " + snapshot_name + " successfully created")

    def delete_backups_and_snapshots_over_retain_count(self):
        self.log_info(msg="Deleting the nextcloud old backups and snapshots...")
        if self.is_maintenance_mode_enabled:
            try:
                self.longhorn_api.delete_backups_and_snapshots_over_retain_count(
                    pv_name=self.k8s_api.get_pv_name_from_pvc_name(pvc_name=self.config.app_volume_name)
                )
            except:
                self.log_err(err="Unable to delete the old snapshots and backups")
                raise NextcloudAppHandlerException(message="Unable to delete the old snapshots and backups")
        else:
            self.log_err(err="Nextcloud maintenance mode not enabled. Cannot continue with the old snapshots and backups deletion")
            raise NextcloudAppHandlerException(message="Nextcloud maintenance mode not enabled. Cannot continue with the old backups and snapshots deletion")
        self.log_info(msg="DONE. Nextcloud oldest volume backups and snapshots successfully deleted")
    ### END - Methods implementation###
### END - Handler ###
