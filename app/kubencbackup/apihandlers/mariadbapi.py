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
        if type(config) != MariaDBApiInstanceConfig:
            raise MariaDBApiInstanceHandlerException(message="Config type not valid. Must be MariaDBApiInstanceConfig")

        try:
            self.conn = mariadb.connect(
                host=config.db_url,
                port=config.db_port,
                user="root",
                password=config.db_root_password,
                autocommit=True)
        except mariadb.Error as e:
            raise MariaDBApiInstanceHandlerException(message="Error connecting to the database:\n" + e)
        
        try:
            self.cur = self.conn.cursor()
        except BaseException as e:
            del(self.conn)
            raise MariaDBApiInstanceHandlerException(message="Unable to retrieve the cursor. The error message is:\n" + e)

    def __del__(self):       
        del(self.cur)
        del(self.conn)

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
        self.conn.close()
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
        self.cur.close()
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