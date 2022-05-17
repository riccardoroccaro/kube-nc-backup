from time import sleep

from kubencbackup.apihandlers.kubernetesapi import K8sApiInstanceHandler, K8sApiInstanceHandlerException
from kubencbackup.apihandlers.longhornapi import LonghornApiInstanceHandler, LonghornApiInstanceHandlerException
from kubencbackup.common.backupexceptions import AppConfigException, AppHandlerException

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


class NextcloudAppHandler:
    # Declaring class private const
    __ENTER_MAINTENANCE_CMD='runuser -u www-data -- php occ maintenance:mode --on'
    __EXIT_MAINTENANCE_CMD='runuser -u www-data -- php occ maintenance:mode --off'

    def __init__(self, config, k8s_api, longhorn_api):
        self.config=config
        self.k8s_api=k8s_api
        self.longhorn_api=longhorn_api

    def __enter__(self):
        try:
            self.enter_maintenance_mode()
        except NextcloudAppHandlerException as e:
            self.exit_maintenance_mode()
            raise NextcloudAppHandlerException(message=e.message)

        return self

    def __exit__(self, *a):
        try:
            self.exit_maintenance_mode()
        except NextcloudAppHandlerException as e:
            print (e)

    def __del__(self):
        try:
            self.exit_maintenance_mode()
        except NextcloudAppHandlerException as e:
            print (e)
    
    ### config getter and setter ###
    @property
    def config(self):
        return self.__config
    
    @config.setter
    def config(self,config):
        if config == None or type(config) != NextcloudAppConfig:
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
            raise NextcloudAppHandlerException(message='"longhorn_api" variable is mandatory and must be of type "LonghornApiInstanceHandler"')
        self.__longhorn_api=longhorn_api
    ### END ###

    ### Methods implementation ###
    def enter_maintenance_mode(self):
        try:
            resp = self.k8s_api.exec_container_command(pod_label="app="+self.config.app_name, command=NextcloudAppHandler.__ENTER_MAINTENANCE_CMD)
        except K8sApiInstanceHandlerException as e:
            raise NextcloudAppHandlerException(message="Unable to enter maintenance mode. The issue is the following:\n" + e)
        
        if resp == "Maintenance mode enabled\n":
            sleep(30)
        else:
            raise NextcloudAppHandlerException(message="Unable to enter maintenance mode. The issue is the following:\n" + resp)
            
    def exit_maintenance_mode(self):
        try:
            resp = self.k8s_api.exec_container_command(pod_label="app="+self.config.app_name, command=NextcloudAppHandler.__EXIT_MAINTENANCE_CMD)
        except K8sApiInstanceHandlerException as e:
            raise NextcloudAppHandlerException(message="Unable to exit maintenance mode. You have to do it by hands. The issue is the following:\n" + e)
        
        if resp != "Maintenance mode disabled\n":
            raise NextcloudAppHandlerException(message="Unable to exit maintenance mode. You have to do it by hands. The issue is the following:\n" + resp)

    def create_volume_snapshot(self, snapshot_name):
        try:
            self.longhorn_api.create_volume_snapshot(
                snapshot_name=snapshot_name,
                pv_name=self.k8s_api.get_pv_name_from_pvc_name(pvc_name=self.config.app_volume_name)
            )
        except (K8sApiInstanceHandlerException,LonghornApiInstanceHandlerException) as e:
            raise NextcloudAppHandlerException(message="Unable to create the snapshot. The issue is the following:\n" + e)

    def create_volume_backup(self, snapshot_name):
        try:
            self.longhorn_api.create_volume_backup(
                snapshot_name=snapshot_name,
                pv_name=self.k8s_api.get_pv_name_from_pvc_name(pvc_name=self.config.app_volume_name)
            )
        except (K8sApiInstanceHandlerException,LonghornApiInstanceHandlerException) as e:
            raise NextcloudAppHandlerException(message="Unable to create the backup. The issue is the following:\n" + e)

    def delete_backups_and_snapshots_over_retain_count(self):
        try:
            self.longhorn_api.delete_backups_and_snapshots_over_retain_count(
                pv_name=self.k8s_api.get_pv_name_from_pvc_name(pvc_name=self.config.app_volume_name)
            )
        except (K8sApiInstanceHandlerException,LonghornApiInstanceHandlerException) as e:
            raise NextcloudAppHandlerException(message="Unable to delete the old snapshots and backups. The issue is the following:\n" + e)
    ### END - Methods implementation###
### END - Handler ###
