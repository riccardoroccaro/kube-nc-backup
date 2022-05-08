class LHBackupException(Exception):
    def __init__(self,message):
        self.message=message

    def __str__(self) -> str:
        return self._message

    @property
    def message(self):
        return self._message

    @message.setter
    def message(self,message):
        self._message=message
