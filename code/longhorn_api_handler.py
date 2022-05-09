from lh_backup_exceptions import ApiInstancesHandlerException

import longhorn

class LonghornApiInstanceHandlerException(ApiInstancesHandlerException):
    def __init__(self,message):
        super().__init__(message)

class LonghornApiInstanceHandler:
    def __init__(self, lh_bak_env):
        try:
            self.lh_bak_env=lh_bak_env
            self.lh_client = longhorn.Client(url=self.lh_bak_env.longhorn_url)
        except BaseException as e:
            raise LonghornApiInstanceHandlerException(message="Error creating Longhorn API client instance. The error message is:\n" + e)

    def __del__(self):
        del(self.lh_client)

    ### lh_client getter, setter and deleter ###
    @property
    def lh_client(self):
        return self.__lh_client
    
    @lh_client.setter
    def lh_client(self, lh_client):
        if lh_client == None:
            raise LonghornApiInstanceHandlerException(message="lh_client cannot be None.")
        self.__lh_client=lh_client

    @lh_client.deleter
    def lh_client(self):
        self.lh_client = None
    ### END ###

    ### lh_bak_env getter, setter and deleter ###
    @property
    def lh_bak_env(self):
        return self.__lh_bak_env
    
    @lh_bak_env.setter
    def lh_bak_env(self, lh_bak_env):
        if lh_bak_env == None:
            raise LonghornApiInstanceHandlerException(message="LHBackupEnvironment mustn't be None")
        self.__lh_bak_env=lh_bak_env
    ### END ###

    ### Methods implementation ###
    def create_volume_snapshot(self, snapshot_name, pv_name):
        try:
            # Retrieve the volume to snapshot
            volume = self.lh_client.by_id_volume(id=pv_name)

            # Create the snapshot
            volume.snapshotCreate(name=snapshot_name)

            return volume
        except BaseException as e:
            raise LonghornApiInstanceHandlerException(message="Unable to create the snapshot for the volume " + pv_name +". The error message is:\n" + e)

    def create_volume_snapshot_and_backup(self, backup_name, pv_name):
        # Create the snapshot
        volume = self.create_volume_snapshot(snapshot_name=backup_name, pv_name=pv_name)

        # Create the backup
        try:
            volume.snapshotBackup(name=backup_name)
        except BaseException as e:
            raise LonghornApiInstanceHandlerException(message="Unable to create the backup for the volume " + pvc_name +". The error message is:\n" + e)

# TODO Implement the following methods and update LHBackupEnvironment by adding the retain policies fields
    def delete_snapshots_over_retain_count():
        pass

    def delete_backups_over_retain_count():
        pass
