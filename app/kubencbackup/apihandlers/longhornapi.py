from dateutil.parser import parse as dateutil_parse

import extlib.longhornlib as longhornlib

from common.backupexceptions import ApiInstancesHandlerException
from common.backupexceptions import ApiInstancesConfigException
from common.backupconfig import BackupConfig

### Config ###
class LonghornApiInstanceConfigException(ApiInstancesConfigException):
    def __init__(self,message):
        super().__init__(message)


class LonghornApiInstanceConfig:
    def __init__(self, longhorn_url, nr_snapshots_to_retain, nr_backups_to_retain):
        self.longhorn_url=longhorn_url
        self.nr_snapshots_to_retain=nr_snapshots_to_retain
        self.nr_backups_to_retain=nr_backups_to_retain

    ### longhorn_url getter and setter ###
    @property
    def longhorn_url(self):
        return self.__longhorn_url
    
    @longhorn_url.setter
    def longhorn_url(self,longhorn_url):
        if longhorn_url == None:
            raise LonghornApiInstanceConfigException(message='longhorn_url is mandatory')
        self.__longhorn_url=longhorn_url
    ### END ###

    ### nr_snapshots_to_retain getter and setter ###
    @property
    def nr_snapshots_to_retain(self):
        return self.__nr_snapshots_to_retain
    
    @nr_snapshots_to_retain.setter
    def nr_snapshots_to_retain(self,nr_snapshots_to_retain):
        if nr_snapshots_to_retain == None:
            self.__nr_snapshots_to_retain = BackupConfig.DEFAULT_SNAPSHOTS_TO_RETAIN
        else:
            try:
                self.__nr_snapshots_to_retain = int(nr_snapshots_to_retain)
            except (ValueError,TypeError) as e:
                raise LonghornApiInstanceConfigException(message="nr_snapshot_to_retain must be a integer number")
    ### END ###

    ### nr_backups_to_retain getter and setter ###
    @property
    def nr_backups_to_retain(self):
        return self.__nr_backups_to_retain
    
    @nr_backups_to_retain.setter
    def nr_backups_to_retain(self,nr_backups_to_retain):
        if nr_backups_to_retain == None:
            self.__nr_backups_to_retain = BackupConfig.DEFAULT_BACKUPS_TO_RETAIN
        else:
            try:
                self.__nr_backups_to_retain = int(nr_backups_to_retain)
            except (ValueError,TypeError) as e:
                raise LonghornApiInstanceConfigException(message="nr_backups_to_retain must be a integer number")
    ### END ###

### END - Config ###

### Handler ###
class LonghornApiInstanceHandlerException(ApiInstancesHandlerException):
    def __init__(self,message):
        super().__init__(message)

class LonghornApiInstanceHandler:
    def __init__(self, config):
        if type(config) != LonghornApiInstanceConfig:
            raise LonghornApiInstanceHandlerException(message="config must be of type LonghornApiInstanceConfig")
        self.config=config

    def __enter__(self):
        try:
            self.client = longhornlib.Client(url=self.config.longhorn_url)
        except BaseException as e:
            self.free_resources()
            raise LonghornApiInstanceHandlerException(message="Error creating Longhorn API client instance. The error message is:\n" + e)

        return self

    def __exit__(self, *a):
        self.free_resources()

    def __del__(self):
        self.free_resources()

    def free_resources(self):
        del(self.client)

    ### client getter, setter and deleter ###
    @property
    def client(self):
        return self.__client
    
    @client.setter
    def client(self, client):
        if client == None:
            raise LonghornApiInstanceHandlerException(message="client cannot be None.")
        self.__client=client

    @client.deleter
    def client(self):
        del(self.__client)
    ### END ###

    ### config getter, setter and deleter ###
    @property
    def config(self):
        return self.__config
    
    @config.setter
    def config(self, config):
        if config == None or type(config) != LonghornApiInstanceConfig:
            raise LonghornApiInstanceHandlerException(message="config mustn't be None and must be of type LonghornApiInstanceConfig")
        self.__config=config
    ### END ###

    ### Methods implementation ###
    def create_volume_snapshot(self, snapshot_name, pv_name):
        try:
            # Retrieve the volume to snapshot
            volume = self.client.by_id_volume(id=pv_name)
            if volume == None:
                raise LonghornApiInstanceHandlerException(message="Cannot find volume "+ pv_name)

            # Create the snapshot
            volume.snapshotCreate(name=snapshot_name)

        except BaseException as e:
            raise LonghornApiInstanceHandlerException(message="Unable to create the snapshot for the volume " + pv_name +". The error message is:\n" + e)


    def create_volume_backup(self, snapshot_name, pv_name):
        try:
            # Retrieve the volume to backup
            volume = self.client.by_id_volume(id=pv_name)
            if volume == None:
                raise LonghornApiInstanceHandlerException(message="Cannot find volume "+ pv_name)

            # Check the snapshot existence
            for snap in volume.snapshotList().data:
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
            vol = self.client.by_id_volume(id=pv_name)

            #Retrieve snapshots list
            snaps=vol.snapshotList().data

            if len(snaps) > self.config.nr_snapshots_to_retain:
                # Polulate a dictionary with the snapshot creation time as a key
                snaps_dict={}
                for snap in snaps:
                    snaps_dict[dateutil_parse(snap.created)]=snap.name
                
                # Retrieve the dictionary keys and sort them chronologically
                keys=[*snaps_dict]
                keys.sort()

                # Remove the older snapshots
                while len(keys) > self.config.nr_snapshots_to_retain:
                    vol.snapshotDelete(name=snaps_dict[keys.pop(0)])

        except BaseException as e:
            raise LonghornApiInstanceHandlerException(message="Cannot delete snapshots over retain count due to the following issue:\n" +e)


    def delete_backups_over_retain_count(self, pv_name):
        try:
            #Retrieve volume by name
            vol = self.client.by_id_backupVolume(id=pv_name)
            
            #Retrieve backups list
            backs=vol.backupList().data

            if len(backs) > self.config.nr_backups_to_retain:
                # Polulate a dictionary with the snapshot (linked to the backup) creation time as a key
                backs_dict={}
                for back in backs:
                    backs_dict[dateutil_parse(back.snapshotCreated)]=back.name
                
                # Retrieve the dictionary keys and sort them chronologically
                keys=[*backs_dict]
                keys.sort()

                # Remove the older backups
                while len(keys) > self.config.nr_backups_to_retain:
                    vol.backupDelete(name=backs_dict[keys.pop(0)])

        except BaseException as e:
            raise LonghornApiInstanceHandlerException(message="Cannot delete backups over retain count due to the following issue:\n" +e)

    
    def delete_backups_and_snapshots_over_retain_count(self, pv_name):
        # Delete old backups
        self.delete_backups_over_retain_count(pv_name=pv_name)
        # Delete old snapshots
        self.delete_snapshots_over_retain_count(pv_name=pv_name)
    ### END ###
### END - Handler ###
