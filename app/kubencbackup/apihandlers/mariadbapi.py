import mariadb

from kubencbackup.common.backupexceptions import ApiInstancesHandlerException
from kubencbackup.common.backupexceptions import ApiInstancesConfigException

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
        if db_port == None:
            raise MariaDBApiInstanceConfigException(message='db_port is mandatory')
        self.__db_port=db_port
    ### END ###

### END - Config ###

### Handler ###
class MariaDBApiInstanceHandlerException(ApiInstancesHandlerException):
    def __init__(self,message):
        super().__init__(message)
        
class MariaDBApiInstanceHandler:
    def __init__(self, config):
        if config == None or type(config) != MariaDBApiInstanceConfig:
            raise MariaDBApiInstanceHandlerException(message="Config type not valid. Must be MariaDBApiInstanceConfig and not None")

        self.__config=config

    def __enter__(self):
        try:
            self.conn = mariadb.connect(
                host=self.__config.db_url,
                port=self.__config.db_port,
                user="root",
                password=self.__config.db_root_password,
                autocommit=True)
        except mariadb.Error as e:
            raise MariaDBApiInstanceHandlerException(message="Error connecting to the database. The error message is:\n" + e)
        
        try:
            self.cur = self.conn.cursor()
        except BaseException as e:
            self.clean_conn()
            raise MariaDBApiInstanceHandlerException(message="Unable to retrieve the cursor. The error message is:\n" + e)

        return self

    def __exit__(self, *a):
        self.clean_resources()

    def __del__(self):       
        self.clean_resources()

    def clean_resources(self):
        self.clean_cur()
        self.clean_conn()

    def clean_cur(self):
        try:
            self.cur.close()
        except BaseException as e:
            print(e)

    def clean_conn(self):
        try:
            self.conn.close()
        except BaseException as e:
            print(e)

   ### conn getter and setter ###
    @property
    def conn(self):
        return self.__conn
    
    @conn.setter
    def conn(self,conn):
        try:
            self.__conn=conn
        except BaseException as e:
            raise MariaDBApiInstanceHandlerException(message="Cannot establish connection. The error message is:\n" + e)
    
    @conn.deleter
    def conn(self):
        self.clean_conn()
        del(self.__conn)
        self.conn = None
    ### END ###

    ### cur getter and setter ###
    @property
    def cur(self):
        return self.__cur
    
    @cur.setter
    def cur(self,cur):
        try:
            self.__cur=cur
        except BaseException as e:
            raise MariaDBApiInstanceHandlerException(message="Cannot retrieve cursor. The error message is:\n" + e)
    
    @cur.deleter
    def cur(self):
        self.clean_cur()
        del(self.__cur)
        self.cur = None
    ### END ###

    ### Methods implementation ###
    def exec_sql_command(self, command):
        try:
            return self.cur.execute(command)
        except BaseException as e:
            raise MariaDBApiInstanceHandlerException(message="Unable to execute command '" + command + "' . The error message is:\n" + e)
    ### END ###
### END - Handler ###
