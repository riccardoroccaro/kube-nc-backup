class LHBackupException(Exception):
    def __init__(self,message):
        self.message=message

    def __str__(self) -> str:
        return self.message

    @property
    def message(self):
        return self.__message

    @message.setter
    def message(self,message):
        self.__message=message

class ApiInstancesHandlerException(LHBackupException):
    def __init__(self,message):
        super().__init__(message)
