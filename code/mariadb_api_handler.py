import mariadb

from lh_backup_exceptions import ApiInstancesHandlerException

class MariaDBApiInstanceHandlerException(ApiInstancesHandlerException):
    def __init__(self,message):
        super().__init__(message)

def init_db_connection(lh_bak_env):
    # Instantiate Connection
    try:
        conn = mariadb.connect(
            host=lh_bak_env.db_url,
            port=lh_bak_env.db_port,
            user="root",
            password=lh_bak_env.db_root_password,
            autocommit=True)
    except mariadb.Error as e:
        raise MariaDBApiInstanceHandlerException(message="Error connecting to the database:\n" + e)
    return conn
        
class MariaDBApiInstanceHandler:
    def __init__(self, lh_bak_env):
        self.conn = init_db_connection(lh_bak_env=lh_bak_env)
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
