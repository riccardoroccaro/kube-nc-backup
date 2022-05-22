class BackupException(Exception):
    def __init__(self,message):
        super().__init__(message)

class ApiInstancesHandlerException(BackupException):
    def __init__(self,message):
        super().__init__(message)

class ApiInstancesConfigException(BackupException):
    def __init__(self,message):
        super().__init__(message)

class AppHandlerException(BackupException):
    def __init__(self,message):
        super().__init__(message)

class AppConfigException(BackupException):
    def __init__(self,message):
        super().__init__(message)
