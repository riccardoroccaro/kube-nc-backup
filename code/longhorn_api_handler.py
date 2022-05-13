from dateutil.parser import parse as dateutil_parse

import longhorn

from lh_backup_exceptions import ApiInstancesHandlerException

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
            if volume == None:
                raise LonghornApiInstanceHandlerException(message="Cannot find volume "+ pv_name)

            # Create the snapshot
            volume.snapshotCreate(name=snapshot_name)

        except BaseException as e:
            raise LonghornApiInstanceHandlerException(message="Unable to create the snapshot for the volume " + pv_name +". The error message is:\n" + e)


    def create_volume_backup(self, snapshot_name, pv_name):
        try:
            # Retrieve the volume to backup
            volume = self.lh_client.by_id_volume(id=pv_name)
            if volume == None:
                raise LonghornApiInstanceHandlerException(message="Cannot find volume "+ pv_name)

            # Check the snapshot existence
            for snap in volume.vol.snapshotList().data:
                if snap.name == snapshot_name:
                    break
            else:
                raise LonghornApiInstanceHandlerException(message="Unable to find the snapshot named " + snapshot_name +". The backup cannot be done.")

            # Create the backup
            volume.snapshotBackup(name=snapshot_name)
        except BaseException as e:
            raise LonghornApiInstanceHandlerException(message="Unable to create the backup for the volume " + pv_name +". The error message is:\n" + e)
    

    def create_volume_snapshot_and_backup(self, backup_name, pv_name):
        # Create the snapshot
        self.create_volume_snapshot(snapshot_name=backup_name, pv_name=pv_name)
        # Create the backup
        self.create_volume_backup(snapshot_name=backup_name, pv_name=pv_name)


    def delete_snapshots_over_retain_count(self, pv_name):
        try:
            #Retrieve volume by name
            vol = self.lh_client.by_id_volume(id=pv_name)

            #Retrieve snapshots list
            snaps=vol.snapshotList().data

            if len(snaps) > self.lh_bak_env.nr_snapshots_to_retain:
                # Polulate a dictionary with the snapshot creation time as a key
                snaps_dict={}
                for snap in snaps:
                    snaps_dict[dateutil_parse(snap.created)]=snap.name
                
                # Retrieve the dictionary keys and sort them chronologically
                keys=[*snaps_dict]
                keys.sort()

                # Remove the older snapshots
                while len(keys) > self.lh_bak_env.nr_snapshots_to_retain:
                    vol.snapshotDelete(name=snaps_dict[keys.pop(0)])

        except BaseException as e:
            raise LonghornApiInstanceHandlerException(message="Cannot delete snapshots over retain count due to the following issue:\n" +e)


    def delete_backups_over_retain_count(self, pv_name):
        try:
            #Retrieve volume by name
            vol = self.lh_client.by_id_backupVolume(id=pv_name)
            
            #Retrieve backups list
            backs=vol.backupList().data

            if len(backs) > self.lh_bak_env.nr_backups_to_retain:
                # Polulate a dictionary with the snapshot (linked to the backup) creation time as a key
                backs_dict={}
                for back in backs:
                    backs_dict[dateutil_parse(back.snapshotCreated)]=back.name
                
                # Retrieve the dictionary keys and sort them chronologically
                keys=[*backs_dict]
                keys.sort()

                # Remove the older backups
                while len(keys) > self.lh_bak_env.nr_backups_to_retain:
                    vol.backupDelete(name=backs_dict[keys.pop(0)])

        except BaseException as e:
            raise LonghornApiInstanceHandlerException(message="Cannot delete backups over retain count due to the following issue:\n" +e)

    
    def delete_backups_and_snapshots_over_retain_count(self, pv_name):
        # Delete old backups
        self.delete_backups_over_retain_count(pv_name=pv_name)
        # Delete old snapshots
        self.delete_snapshots_over_retain_count(pv_name=pv_name)
    ### END ###
