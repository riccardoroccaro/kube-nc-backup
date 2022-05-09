import os

from lh_backup_exceptions import LHBackupException

class LHBackupEnvironmentException(LHBackupException):
    def __init__(self,message):
        super().__init__(message)


class LHBackupEnvironment:
    def __init__(self):
        self.backup_type=os.getenv('BACKUP_TYPE')
        self.namespace=os.getenv('NAMESPACE')
        self.app_name=os.getenv('NEXTCLOUD_APP_NAME')
        self.app_volume_name=os.getenv('NEXTCLOUD_VOLUME_NAME')
        self.db_app_name=os.getenv('MARIADB_APP_NAME')
        self.db_root_password=os.getenv('MARIADB_DB_ROOT_PASSWORD')
        self.db_actual_volume_name=os.getenv('MARIADB_ACTUAL_VOLUME_NAME')
        self.db_backup_volume_name=os.getenv('MARIADB_BACKUP_VOLUME_NAME')
        self.db_backup_file_path=os.getenv('MARIADB_BACKUP_FILE_PATH')
        self.longhorn_url=os.getenv('LONGHORN_URL')
        
    ### backup_type getter and setter ###
    @property
    def backup_type(self):
        return self.__backup_type
    
    @backup_type.setter
    def backup_type(self,backup_type):
        if backup_type not in ['FULL-BACKUP', 'SNAPSHOT']:
            raise LHBackupEnvironmentException(message='Wrong backup type. "BACKUP_TYPE" environment variable is mandatory and must be either "FULL_BACKUP" or "SNAPSHOT"')
        self.__backup_type=backup_type
    ### END ###

    ### namespace getter and setter ###
    @property
    def namespace(self):
        return self.__namespace
    
    @namespace.setter
    def namespace(self,namespace):
        if namespace == None:
            self.__namespace = 'default'
        else:
            self.__namespace=namespace
    ### END ###

    ### app_name getter and setter ###
    @property
    def app_name(self):
        return self.__app_name
    
    @app_name.setter
    def app_name(self,app_name):
        if app_name == None:
            raise LHBackupEnvironmentException(message='"NEXTCLOUD_APP_NAME" environment variable is mandatory')
        self.__app_name=app_name
    ### END ###

    ### app_volume_name getter and setter ###
    @property
    def app_volume_name(self):
        return self.__app_volume_name
    
    @app_volume_name.setter
    def app_volume_name(self,app_volume_name):
        if app_volume_name == None:
            raise LHBackupEnvironmentException(message='"NEXTCLOUD_VOLUME_NAME" environment variable is mandatory')
        self.__app_volume_name=app_volume_name
    ### END ###

    ### db_app_name getter and setter ###
    @property
    def db_app_name(self):
        return self.__db_app_name
    
    @db_app_name.setter
    def db_app_name(self,db_app_name):
        if db_app_name == None:
            raise LHBackupEnvironmentException(message='"MARIADB_APP_NAME" environment variable is mandatory')
        self.__db_app_name=db_app_name
    ### END ###

    ### db_root_password getter and setter ###
    @property
    def db_root_password(self):
        return self.__db_root_password
    
    @db_root_password.setter
    def db_root_password(self,db_root_password):
        if db_root_password == None:
            raise LHBackupEnvironmentException(message='"MARIADB_DB_ROOT_PASSWORD" environment variable is mandatory')
        self.__db_root_password=db_root_password
    ### END ###

    ### db_actual_volume_name getter and setter ###
    @property
    def db_actual_volume_name(self):
        return self.__db_actual_volume_name
    
    @db_actual_volume_name.setter
    def db_actual_volume_name(self,db_actual_volume_name):
        if db_actual_volume_name == None:
            raise LHBackupEnvironmentException(message='"MARIADB_ACTUAL_VOLUME_NAME" environment variable is mandatory')
        self.__db_actual_volume_name=db_actual_volume_name
    ### END ###
    
    ### db_backup_volume_name getter and setter ###
    @property
    def db_backup_volume_name(self):
        return self.__db_backup_volume_name
    
    @db_backup_volume_name.setter
    def db_backup_volume_name(self,db_backup_volume_name):
        if db_backup_volume_name == None:
            raise LHBackupEnvironmentException(message='"MARIADB_BACKUP_VOLUME_NAME" environment variable is mandatory')
        self.__db_backup_volume_name=db_backup_volume_name
    ### END ###
    
    ### db_backup_file_path getter and setter ###
    @property
    def db_backup_file_path(self):
        return self.__db_backup_file_path
    
    @db_backup_file_path.setter
    def db_backup_file_path(self,db_backup_file_path):
        if db_backup_file_path == None:
            raise LHBackupEnvironmentException(message='"MARIADB_BACKUP_FILE_PATH" environment variable is mandatory')
        self.__db_backup_file_path=db_backup_file_path
    ### END ###
    
    ### longhorn_url getter and setter ###
    @property
    def longhorn_url(self):
        return self.__longhorn_url
    
    @longhorn_url.setter
    def longhorn_url(self,longhorn_url):
        if longhorn_url == None:
            raise LHBackupEnvironmentException(message='"LONGHORN_URL" environment variable is mandatory')
        self.__longhorn_url=longhorn_url
    ### END ###
