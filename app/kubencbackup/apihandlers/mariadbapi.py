import functools

import mariadb

from kubencbackup.common.backupexceptions import ApiInstancesConfigException, ApiInstancesHandlerException
from kubencbackup.common.loggable import Loggable

### Config ###
class MariaDBApiInstanceConfigException(ApiInstancesConfigException):
    def __init__(self,message):
        super().__init__(message)


class MariaDBApiInstanceConfig:
    def __init__(self, db_url, db_port, db_root_password):
        self.db_root_password=db_root_password
        self.db_url=db_url
        self.db_port=db_port

    ### db_root_password getter and setter ###
    @property
    def db_root_password(self):
        return self.__db_root_password
    
    @db_root_password.setter
    def db_root_password(self,db_root_password):
        if db_root_password == None:
            raise MariaDBApiInstanceConfigException(message='db_root_password is mandatory')
        self.__db_root_password=db_root_password
    ### END ###

    ### db_url getter and setter ###
    @property
    def db_url(self):
        return self.__db_url
    
    @db_url.setter
    def db_url(self,db_url):
        if db_url == None:
            raise MariaDBApiInstanceConfigException(message='db_url is mandatory')
        self.__db_url=db_url
    ### END ###

    ### db_port getter and setter ###
    @property
    def db_port(self):
        return self.__db_port
    
    @db_port.setter
    def db_port(self,db_port):
        try:
            self.__db_port=int(db_port)
        except (ValueError,TypeError):
            raise MariaDBApiInstanceConfigException(message="db_port is mandatory and must be a integer number")
        except:
            raise MariaDBApiInstanceConfigException(message="Unknown error while setting db_port")
    ### END ###

### END - Config ###

### Methods decorator ###
def exec_only_if_conn_init(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if self.is_connection_initialized:
            return method(self, *args, **kwargs)
        else:
            self.log_err("Connection to MariaDB API not initialized")
            raise MariaDBApiInstanceConfigException(message="Connection to MariaDB API not initialized")
    return wrapper
### END ###

### Handler ###
class MariaDBApiInstanceHandlerException(ApiInstancesHandlerException):
    def __init__(self,message):
        super().__init__(message)
        
class MariaDBApiInstanceHandler (Loggable):
    def __init__(self, config):
        super().__init__(name="MARIADB-API###", log_level=2)

        self.__is_connection_initialized = False

        if config == None or type(config) != MariaDBApiInstanceConfig:
            self.log_err(err="the configuration object mustn't be None and must be a MariaDBApiInstanceConfig instance")
            raise MariaDBApiInstanceHandlerException(message="Config type not valid. Must be MariaDBApiInstanceConfig and not None")
        self.__config=config

    def __enter__(self):
        self.log_info(msg="Initializing MariaDB API...")
        try:
            self.log_info(msg="  Initializing the connection...")
            self.conn = mariadb.connect(
                host=self.__config.db_url,
                port=self.__config.db_port,
                user="root",
                password=self.__config.db_root_password,
                autocommit=True)
            self.log_info(msg="  DONE. Connection to MariaDB successfully initialized.")
        except mariadb.Error:
            self.log_err(err="MariaDB API error connecting to the database. Unable to connect to MariaDB")
            raise MariaDBApiInstanceHandlerException(message="MariaDB API error connecting to the database. Unable to connect to MariaDB")
        except:
            self.log_err(err="Unknown error while connecting to the database. Unable to connect to MariaDB")
            raise MariaDBApiInstanceHandlerException(message="Unknown error while connecting to the database. Unable to connect to MariaDB")
        
        try:
            self.log_info(msg="  Retrieving the cursor from connection...")
            self.cur = self.conn.cursor()
            self.log_info(msg="  DONE. Cursor successfully retrieved.")
        except mariadb.Error:
            self.log_err(err="MariaDB API error: unable to retrieve the cursor")
            self.clean_conn()
            raise MariaDBApiInstanceHandlerException(message="MariaDB API error: unable to retrieve the cursor")
        except:
            self.log_err(err="Unknownw error: unable to retrieve the cursor")
            self.clean_conn()
            raise MariaDBApiInstanceHandlerException(message="Unknownw error: unable to retrieve the cursor")

        self.log_info(msg="MariaDB API successfully initialized")

        self.__is_connection_initialized = True

        return self

    def __exit__(self, *a):
        self.log_info(msg="Cleaning up MariaDB API resources...")
        self.clean_resources()

    def __del__(self):       
        self.clean_resources()

    def clean_resources(self):
        try:
            self.clean_cur()
            self.log_info(msg="  MariaDB cursor successfully closed")
            self.__is_connection_initialized = False
        except (AttributeError,NameError):
            pass
        except:
            self.log_err(err="Unable to close the MariaDB API cursor")

        try:
            self.clean_conn()
            self.log_info(msg="  MariaDB connection successfully closed")
            self.log_info(msg="MariaDB API resources successfully cleaned up")
        except (AttributeError,NameError):
            pass
        except:
            self.log_err(err="Unable to close the MariaDB API connection")

    def clean_cur(self):
        self.cur.close()
        del(self.cur)

    def clean_conn(self):
        self.conn.close()
        del(self.conn)

   ### conn getter and setter ###
    @property
    def conn(self):
        return self.__conn
    
    @conn.setter
    def conn(self,conn):
        if conn == None:
            self.log_err(err="Cannot establish connection. It mustn't be None")
            raise MariaDBApiInstanceHandlerException(message="Cannot establish connection. It mustn't be None")
        self.__conn=conn            
    
    @conn.deleter
    def conn(self):
        del(self.__conn)
    ### END ###

    ### cur getter and setter ###
    @property
    def cur(self):
        return self.__cur
    
    @cur.setter
    def cur(self,cur):
        if cur == None:
            self.log_err(err="Cannot retrieve cursor. It mustn't be None")
            raise MariaDBApiInstanceHandlerException(message="Cannot retrieve cursor. It mustn't be None")
        self.__cur=cur
    
    @cur.deleter
    def cur(self):
        del(self.__cur)
    ### END ###

    ### is_connection_initialized getter###
    @property
    def is_connection_initialized(self):
        return self.__is_connection_initialized
    ### END ###

    ### Methods implementation ###
    @exec_only_if_conn_init
    def exec_sql_command(self, command):
        self.log_info(msg="Executing the SQL command '" + command + "' on MariaDB...")
        try:
            res = self.cur.execute(command)
            self.log_info(msg="DONE. SQL command successfully executed")
            return res
        except mariadb.Error:
            self.log_err(err="MariaDB API error: unable to execute the SQL command '" + command + "'")
            raise MariaDBApiInstanceHandlerException(message="MariaDB API error: unable to execute the SQL command '" + command + "'")
        except:
            self.log_err(err="Unknown error: unable to execute the SQL command '" + command + "'")
            raise MariaDBApiInstanceHandlerException(message="Unknown error: unable to execute the SQL command '" + command + "'")
    ### END ###
### END - Handler ###
