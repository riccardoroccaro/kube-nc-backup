import functools

from dateutil.parser import parse as dateutil_parse
from dateutil.parser._parser import ParserError

import kubencbackup.extlib.longhornlib as longhornlib
from kubencbackup.extlib.longhornlib import ApiError, ClientApiError

from kubencbackup.common.backupconfig import BackupConfig
from kubencbackup.common.backupexceptions import ApiInstancesConfigException, ApiInstancesHandlerException
from kubencbackup.common.loggable import Loggable


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
            except (ValueError,TypeError):
                raise LonghornApiInstanceConfigException(message="nr_snapshot_to_retain must be a integer number")
            except:
                raise LonghornApiInstanceConfigException(message="Unknown error while setting nr_snapshot_to_retain")
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
            except (ValueError,TypeError):
                raise LonghornApiInstanceConfigException(message="nr_backups_to_retain must be a integer number")
            except:
                raise LonghornApiInstanceConfigException(message="Unknown error while setting nr_backups_to_retain")
    ### END ###

### END - Config ###

### Methods decorator ###
def exec_only_if_conn_init(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if self.is_connection_initialized:
            return method(self, *args, **kwargs)
        else:
            self.log_err("Connection to Longhorn API not initialized")
            raise LonghornApiInstanceHandlerException(message="Connection to Longhorn API not initialized")
    return wrapper
### END ###

### Handler ###
class LonghornApiInstanceHandlerException(ApiInstancesHandlerException):
    def __init__(self,message):
        super().__init__(message)

class LonghornApiInstanceHandler(Loggable):
    def __init__(self, config):
        super().__init__(name="LONGHORN-API##", log_level=2)

        self.__is_connection_initialized = False

        if config == None or type(config) != LonghornApiInstanceConfig:
            self.log_err(err="the configuration object mustn't be None and must be a LonghornApiInstanceConfig instance")
            raise LonghornApiInstanceHandlerException(message="config must be of type LonghornApiInstanceConfig")
        self.config=config

    def __enter__(self):
        try:
            self.log_info(msg="Initializing Longhorn API...")

            self.client = longhornlib.Client(url=self.config.longhorn_url)

            self.log_info(msg="DONE. Longhorn API successfully initialized.")
        except (ApiError,ClientApiError):
            self.log_err(err="Longhorn API error. Unable to create the Longhorn API client instance.")
            self.free_resources()
            raise LonghornApiInstanceHandlerException(message="Longhorn API error. Error creating Longhorn API client instance")
        except:
            self.log_err(err="Unknown error. Unable to create the Longhorn API client instance")
            self.free_resources()
            raise LonghornApiInstanceHandlerException(message="Unknown error. Unable to create the Longhorn API client instance")

        self.__is_connection_initialized = True

        return self

    def __exit__(self, *a):
        self.log_info(msg="Cleaning up Longhorn API resources...")
        self.free_resources()

    def __del__(self):
        self.free_resources()

    def free_resources(self):
        try:
            del(self.client)
            self.log_info(msg="Longhorn API resources successfully cleaned up")
            self.__is_connection_initialized = False
        except (AttributeError,NameError):
            pass
        except:
            self.log_err(err="Unable to clean up the Longhorn API resources")

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
            self.log_err(err="the configuration object mustn't be None and must be a LonghornApiInstanceConfig instance")
            raise LonghornApiInstanceHandlerException(message="config mustn't be None and must be of type LonghornApiInstanceConfig")
        self.__config=config
    ### END ###

    ### is_connection_initialized getter###
    @property
    def is_connection_initialized(self):
        return self.__is_connection_initialized
    ### END ###

    ### Methods implementation ###
    @exec_only_if_conn_init
    def create_volume_snapshot(self, snapshot_name, pv_name):
        self.log_info(msg="Creating the "+ pv_name + " volume snapshot...")
        try:
            # Retrieve the volume to snapshot
            self.log_info(msg="  Retrieving the volume to snapshot...")
            volume = self.client.by_id_volume(id=pv_name)
            if volume == None:
                self.log_err(err="Cannot find volume named " + pv_name)
                raise LonghornApiInstanceHandlerException(message="Cannot find volume "+ pv_name)

            # Create the snapshot
            self.log_info(msg="  DONE. Creating the snapshot...")
            volume.snapshotCreate(name=snapshot_name)
            self.log_info(msg="DONE. Snapshot named " + snapshot_name + " for " + pv_name + " volume successfully created")

        except LonghornApiInstanceHandlerException as e:
            raise e
        except (ApiError,ClientApiError):
            self.log_err(err="Longhorn API error. Unable to create the snapshot for the volume " + pv_name)
            raise LonghornApiInstanceHandlerException(message="Longhorn API error. Unable to create the snapshot for the volume " + pv_name)
        except:
            self.log_err(err="Unknown error. Unable to create the snapshot for the volume " + pv_name)
            raise LonghornApiInstanceHandlerException(message="Unknown error. Unable to create the snapshot for the volume " + pv_name)

    @exec_only_if_conn_init
    def create_volume_backup(self, snapshot_name, pv_name):
        self.log_info(msg="Creating the "+ pv_name + " volume backup from snapshot " + snapshot_name + "...")
        try:
            # Retrieve the volume to backup
            self.log_info(msg="  Retrieving the volume to backup...")
            volume = self.client.by_id_volume(id=pv_name)
            if volume == None:
                self.log_err(err="Cannot find volume named " + pv_name)
                raise LonghornApiInstanceHandlerException(message="Cannot find volume "+ pv_name)

            # Check the snapshot existence
            self.log_info(msg="  DONE. Checking the snapshot " + snapshot_name + " existence...")
            for snap in volume.snapshotList().data:
                if snap.name == snapshot_name:
                    self.log_info(msg="  DONE. Snapshot " + snapshot_name + " successfully found")
                    break
            else:
                self.log_err(err="Unable to find the snapshot named " + snapshot_name +". The backup cannot be done")
                raise LonghornApiInstanceHandlerException(message="Unable to find the snapshot named " + snapshot_name +". The backup cannot be done")

            # Create the backup
            self.log_info(msg="  Creating the backup...")
            volume.snapshotBackup(name=snapshot_name)
            self.log_info(msg="DONE. Backup from snapshot " + snapshot_name + " for " + pv_name + " volume successfully created")
        except LonghornApiInstanceHandlerException as e:
            raise e
        except (ApiError,ClientApiError):
            self.log_err(err="Longhorn API error. Unable to create the backup for the volume " + pv_name)
            raise LonghornApiInstanceHandlerException(message="Longhorn API error. Unable to create the backup for the volume " + pv_name)
        except:
            self.log_err(err="Unknown error. Unable to create the backup for the volume " + pv_name)
            raise LonghornApiInstanceHandlerException(message="Unknown error. Unable to create the backup for the volume " + pv_name)


    def create_volume_snapshot_and_backup(self, backup_name, pv_name):
        # Create the snapshot
        self.create_volume_snapshot(snapshot_name=backup_name, pv_name=pv_name)
        # Create the backup
        self.create_volume_backup(snapshot_name=backup_name, pv_name=pv_name)

    @exec_only_if_conn_init
    def delete_snapshots_over_retain_count(self, pv_name):
        self.log_info("Deleting the snapshots older than the last " + str(self.config.nr_snapshots_to_retain) + "' for the volume " + pv_name + "...")
        try:
            #Retrieve volume by name
            self.log_info(msg="  Retrieving the volume " + pv_name + "...")
            vol = self.client.by_id_volume(id=pv_name)
            if vol == None:
                self.log_err(err="Cannot find volume named " + pv_name)
                raise LonghornApiInstanceHandlerException(message="Cannot find volume "+ pv_name)

            #Retrieve snapshots list
            self.log_info(msg="  DONE. Retrieving the snapshots list for the volume " + pv_name)
            try:
                snaps=vol.snapshotList().data

                # Removing the already deleted snapshots
                clean_snaps = []
                for s in snaps:
                    if not s.removed:
                        clean_snaps.append(s)
                snaps = clean_snaps
            except:
                self.log_err(err="Cannot find the snapshot list for the volume " + pv_name + ". Unable to continue")
                raise LonghornApiInstanceHandlerException(message="Cannot find the snapshot list for the volume " + pv_name + ". Unable to continue")

            self.log_info(msg="  DONE. Retrieving the snapshots to remove...")
            if len(snaps) > self.config.nr_snapshots_to_retain:
                # Polulate a dictionary with the snapshot creation time as a key
                try:
                    snaps_dict={}
                    for snap in snaps:
                        snaps_dict[dateutil_parse(snap.created)]=snap.name
                except ParserError:
                    self.log_err("Snapshot creation date and time parsing error")
                    raise LonghornApiInstanceHandlerException(message="Snapshot creation date and time parsing error")
                except:
                    self.log_err("Unknown error while parsing snapshot creation date and time")
                    raise LonghornApiInstanceHandlerException(message="Unknown error while parsing snapshot creation date and time")
                
                # Retrieve the dictionary keys and sort them chronologically
                keys=[*snaps_dict]
                keys.sort()

                self.log_info(msg="  DONE. Removing old snapshots for the volume " + pv_name + "...")
                # Remove the older snapshots
                while len(keys) > self.config.nr_snapshots_to_retain:
                    to_del = snaps_dict[keys.pop(0)]
                    self.log_info("    Deleting the snapshot named: " + to_del)
                    vol.snapshotDelete(name=to_del)
                self.log_info(msg="DONE. Successfully removed the older snapshots for the volume " + pv_name)
        except LonghornApiInstanceHandlerException as e:
            raise e
        except (ApiError,ClientApiError):
            self.log_err(err="Longhorn API error. Cannot delete snapshots over retain count")
            raise LonghornApiInstanceHandlerException(message="Longhorn API error. Cannot delete snapshots over retain count")
        except:
            self.log_err(err="Unknown error. Cannot delete snapshots over retain count")
            raise LonghornApiInstanceHandlerException(message="Unknown error. Cannot delete snapshots over retain count")

    @exec_only_if_conn_init
    def delete_backups_over_retain_count(self, pv_name):
        self.log_info("Deleting the backups older than the last '" + str(self.config.nr_backups_to_retain) + "' for the volume " + pv_name + "...")
        try:
            #Retrieve volume by name
            self.log_info(msg="  Retrieving the volume " + pv_name + "...")
            vol = self.client.by_id_backupVolume(id=pv_name)
            if vol == None:
                self.log_err(err="Cannot find volume named " + pv_name)
                raise LonghornApiInstanceHandlerException(message="Cannot find volume "+ pv_name)
            
            #Retrieve backups list
            self.log_info(msg="  DONE. Retrieving the backups list for the volume " + pv_name)
            try:
                backs=vol.backupList().data
            except:
                self.log_err(err="Cannot find the backups list for the volume " + pv_name + ". Unable to continue")
                raise LonghornApiInstanceHandlerException(message="Cannot find the backups list")

            self.log_info(msg="  DONE. Retrieving the backups to remove...")
            if len(backs) > self.config.nr_backups_to_retain:
                # Polulate a dictionary with the snapshot (linked to the backup) creation time as a key
                try:
                    backs_dict={}
                    for back in backs:
                        # Check if the backup is already completed, otherwise it will be skipped
                        if back.progress == 100:
                            backs_dict[dateutil_parse(back.snapshotCreated)]=back.name
                except ParserError:
                    self.log_err("Snapshot creation date and time parsing error")
                    raise LonghornApiInstanceHandlerException(message="Snapshot creation date and time parsing error")
                except:
                    self.log_err("Unknown error while parsing snapshot creation date and time")
                    raise LonghornApiInstanceHandlerException(message="Unknown error while parsing snapshot creation date and time")
                
                # Retrieve the dictionary keys and sort them chronologically
                keys=[*backs_dict]
                keys.sort()

                self.log_info(msg="  DONE. Removing old backups for the volume " + pv_name + "...")
                # Remove the older backups
                while len(keys) > self.config.nr_backups_to_retain:
                    to_del = backs_dict[keys.pop(0)]
                    self.log_info("    Deleting the backup named: " + to_del)
                    vol.backupDelete(name=to_del)
                self.log_info(msg="DONE. Successfully removed the older backups for the volume " + pv_name)
        except LonghornApiInstanceHandlerException as e:
            raise e
        except (ApiError,ClientApiError):
            self.log_err(err="Longhorn API error. Cannot delete backups over retain count")
            raise LonghornApiInstanceHandlerException(message="Longhorn API error. Cannot delete backups over retain count")
        except:
            self.log_err(err="Unknown error. Cannot delete backups over retain count")
            raise LonghornApiInstanceHandlerException(message="Unknown error. Cannot delete backups over retain count")

    
    def delete_backups_and_snapshots_over_retain_count(self, pv_name):
        # Delete old backups
        self.delete_backups_over_retain_count(pv_name=pv_name)
        # Delete old snapshots
        self.delete_snapshots_over_retain_count(pv_name=pv_name)
    ### END ###
### END - Handler ###
