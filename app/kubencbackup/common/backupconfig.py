import os

from kubencbackup.common.backupexceptions import BackupException
from kubencbackup.common.loggable import Loggable

class BackupConfigException(BackupException):
    def __init__(self,message):
        super().__init__(message)


class BackupConfig(Loggable):
    DEFAULT_NAMESPACE = 'default'
    DEFAULT_SNAPSHOTS_TO_RETAIN = 30
    DEFAULT_BACKUPS_TO_RETAIN = 4

    def __init__(self):
        super().__init__("BACKUP-CONFIG ")
        
        self.backup_type=os.getenv('BACKUP_TYPE')
        self.namespace=os.getenv('NAMESPACE')
        self.app_name=os.getenv('NEXTCLOUD_APP_NAME')
        self.app_volume_name=os.getenv('NEXTCLOUD_VOLUME_NAME')
        self.db_app_name=os.getenv('MARIADB_APP_NAME')
        self.db_root_password=os.getenv('MARIADB_DB_ROOT_PASSWORD')
        self.db_url=os.getenv('MARIADB_URL')
        self.db_port=os.getenv('MARIADB_PORT')
        self.db_actual_volume_name=os.getenv('MARIADB_ACTUAL_VOLUME_NAME')
        self.db_backup_volume_name=os.getenv('MARIADB_BACKUP_VOLUME_NAME')
        self.db_backup_file_path=os.getenv('MARIADB_BACKUP_FILE_PATH')
        self.longhorn_url=os.getenv('LONGHORN_URL')
        self.nr_snapshots_to_retain=os.getenv('NR_SNAPSHOTS_TO_RETAIN')
        self.nr_backups_to_retain=os.getenv('NR_BACKUPS_TO_RETAIN')
        
    ### backup_type getter and setter ###
    @property
    def backup_type(self):
        return self.__backup_type
    
    @backup_type.setter
    def backup_type(self,backup_type):
        if backup_type not in ['FULL-BACKUP', 'SNAPSHOT']:
            self.log_err('Wrong backup type. "BACKUP_TYPE" environment variable is mandatory and must be either "FULL-BACKUP" or "SNAPSHOT"')
            raise BackupConfigException(message='Wrong backup type. "BACKUP_TYPE" environment variable is mandatory and must be either "FULL-BACKUP" or "SNAPSHOT"')
        self.__backup_type=backup_type
        self.log_info("Succesfully retrieved BACKUP_TYPE as '" + backup_type + "'.")
    ### END ###

    ### namespace getter and setter ###
    @property
    def namespace(self):
        return self.__namespace
    
    @namespace.setter
    def namespace(self,namespace):
        if namespace == None:
            self.__namespace = BackupConfig.DEFAULT_NAMESPACE
            self.log_info("NAMESPACE env var not set. Using the default one: " + BackupConfig.DEFAULT_NAMESPACE)
        else:
            self.__namespace=namespace
            self.log_info("Succesfully retrieved NAMESPACE as '" + namespace + "'.")
    ### END ###

    ### app_name getter and setter ###
    @property
    def app_name(self):
        return self.__app_name
    
    @app_name.setter
    def app_name(self,app_name):
        if app_name == None:
            self.log_err('"NEXTCLOUD_APP_NAME" environment variable is mandatory')
            raise BackupConfigException(message='"NEXTCLOUD_APP_NAME" environment variable is mandatory')
        self.__app_name=app_name
        self.log_info("Succesfully retrieved NEXTCLOUD_APP_NAME as '" + app_name + "'.")
    ### END ###

    ### app_volume_name getter and setter ###
    @property
    def app_volume_name(self):
        return self.__app_volume_name
    
    @app_volume_name.setter
    def app_volume_name(self,app_volume_name):
        if app_volume_name == None:
            self.log_err('"NEXTCLOUD_VOLUME_NAME" environment variable is mandatory')
            raise BackupConfigException(message='"NEXTCLOUD_VOLUME_NAME" environment variable is mandatory')
        self.__app_volume_name=app_volume_name
        self.log_info("Succesfully retrieved NEXTCLOUD_VOLUME_NAME as '" + app_volume_name + "'.")
    ### END ###

    ### db_app_name getter and setter ###
    @property
    def db_app_name(self):
        return self.__db_app_name
    
    @db_app_name.setter
    def db_app_name(self,db_app_name):
        if db_app_name == None:
            self.log_err('"MARIADB_APP_NAME" environment variable is mandatory')
            raise BackupConfigException(message='"MARIADB_APP_NAME" environment variable is mandatory')
        self.__db_app_name=db_app_name
        self.log_info("Succesfully retrieved MARIADB_APP_NAME as '" + db_app_name + "'.")
    ### END ###

    ### db_root_password getter and setter ###
    @property
    def db_root_password(self):
        return self.__db_root_password
    
    @db_root_password.setter
    def db_root_password(self,db_root_password):
        if db_root_password == None:
            self.log_err('"MARIADB_DB_ROOT_PASSWORD" environment variable is mandatory')
            raise BackupConfigException(message='"MARIADB_DB_ROOT_PASSWORD" environment variable is mandatory')
        self.__db_root_password=db_root_password
        self.log_info("Succesfully retrieved MARIADB_DB_ROOT_PASSWORD.")
    ### END ###

    ### db_url getter and setter ###
    @property
    def db_url(self):
        return self.__db_url
    
    @db_url.setter
    def db_url(self,db_url):
        if db_url == None:
            self.log_err('"MARIADB_URL" environment variable is mandatory')
            raise BackupConfigException(message='"MARIADB_URL" environment variable is mandatory')
        self.__db_url=db_url
        self.log_info("Succesfully retrieved MARIADB_URL as '" + db_url + "'.")
    ### END ###

    ### db_port getter and setter ###
    @property
    def db_port(self):
        return self.__db_port
    
    @db_port.setter
    def db_port(self,db_port):
        if db_port == None:
            self.log_err('"MARIADB_PORT" environment variable is mandatory')
            raise BackupConfigException(message='"MARIADB_PORT" environment variable is mandatory')
        self.__db_port=db_port
        self.log_info("Succesfully retrieved MARIADB_PORT as '" + db_port + "'.")
    ### END ###

    ### db_actual_volume_name getter and setter ###
    @property
    def db_actual_volume_name(self):
        return self.__db_actual_volume_name
    
    @db_actual_volume_name.setter
    def db_actual_volume_name(self,db_actual_volume_name):
        if db_actual_volume_name == None:
            self.log_err('"MARIADB_ACTUAL_VOLUME_NAME" environment variable is mandatory')
            raise BackupConfigException(message='"MARIADB_ACTUAL_VOLUME_NAME" environment variable is mandatory')
        self.__db_actual_volume_name=db_actual_volume_name
        self.log_info("Succesfully retrieved MARIADB_ACTUAL_VOLUME_NAME as '" + db_actual_volume_name + "'.")
    ### END ###
    
    ### db_backup_volume_name getter and setter ###
    @property
    def db_backup_volume_name(self):
        return self.__db_backup_volume_name
    
    @db_backup_volume_name.setter
    def db_backup_volume_name(self,db_backup_volume_name):
        if db_backup_volume_name == None:
            self.log_err('"MARIADB_BACKUP_VOLUME_NAME" environment variable is mandatory')
            raise BackupConfigException(message='"MARIADB_BACKUP_VOLUME_NAME" environment variable is mandatory')
        self.__db_backup_volume_name=db_backup_volume_name
        self.log_info("Succesfully retrieved MARIADB_BACKUP_VOLUME_NAME as '" + db_backup_volume_name + "'.")
    ### END ###
    
    ### db_backup_file_path getter and setter ###
    @property
    def db_backup_file_path(self):
        return self.__db_backup_file_path
    
    @db_backup_file_path.setter
    def db_backup_file_path(self,db_backup_file_path):
        if db_backup_file_path == None:
            self.log_err('"MARIADB_BACKUP_FILE_PATH" environment variable is mandatory')
            raise BackupConfigException(message='"MARIADB_BACKUP_FILE_PATH" environment variable is mandatory')
        self.__db_backup_file_path=db_backup_file_path
        self.log_info("Succesfully retrieved MARIADB_BACKUP_FILE_PATH as '" + db_backup_file_path + "'.")
    ### END ###
    
    ### longhorn_url getter and setter ###
    @property
    def longhorn_url(self):
        return self.__longhorn_url
    
    @longhorn_url.setter
    def longhorn_url(self,longhorn_url):
        if longhorn_url == None:
            self.log_err('"LONGHORN_URL" environment variable is mandatory')
            raise BackupConfigException(message='"LONGHORN_URL" environment variable is mandatory')
        self.__longhorn_url=longhorn_url
        self.log_info("Succesfully retrieved LONGHORN_URL as '" + longhorn_url + "'.")
    ### END ###

    ### nr_snapshots_to_retain getter and setter ###
    @property
    def nr_snapshots_to_retain(self):
        return self.__nr_snapshots_to_retain
    
    @nr_snapshots_to_retain.setter
    def nr_snapshots_to_retain(self,nr_snapshots_to_retain):
        if nr_snapshots_to_retain == None:
            self.log_info("'NR_SNAPSHOTS_TO_RETAIN' environment variable not set. Setting the default value: " + BackupConfig.DEFAULT_SNAPSHOTS_TO_RETAIN)
            self.__nr_snapshots_to_retain = BackupConfig.DEFAULT_SNAPSHOTS_TO_RETAIN
        else:
            try:
                self.__nr_snapshots_to_retain = int(nr_snapshots_to_retain)
                self.log_info("Succesfully retrieved NR_SNAPSHOTS_TO_RETAIN as '" + nr_snapshots_to_retain + "'.")
            except (ValueError,TypeError) as e:
                self.log_err("'NR_SNAPSHOTS_TO_RETAIN' environment variable must be a integer number")
                raise BackupConfigException(message="NR_SNAPSHOTS_TO_RETAIN environment variable must be a integer number")
    ### END ###

    ### nr_backups_to_retain getter and setter ###
    @property
    def nr_backups_to_retain(self):
        return self.__nr_backups_to_retain
    
    @nr_backups_to_retain.setter
    def nr_backups_to_retain(self,nr_backups_to_retain):
        if nr_backups_to_retain == None:
            self.log_info("'NR_BACKUPS_TO_RETAIN' environment variable not set. Setting the default value: " + BackupConfig.DEFAULT_SNAPSHOTS_TO_RETAIN)
            self.__nr_backups_to_retain = BackupConfig.DEFAULT_BACKUPS_TO_RETAIN
        else:
            try:
                self.__nr_backups_to_retain = int(nr_backups_to_retain)
                self.log_info("Succesfully retrieved NR_BACKUPS_TO_RETAIN as '" + nr_backups_to_retain + "'.")
            except (ValueError,TypeError) as e:
                self.log_err("'NR_BACKUPS_TO_RETAIN' environment variable must be a integer number")
                raise BackupConfigException(message="NR_BACKUPS_TO_RETAIN environment variable must be a integer number")
    ### END ###
