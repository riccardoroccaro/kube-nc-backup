import os
import re
import textwrap

from datetime import datetime

class Loggable:
    def __init__(self, name=None, log_level=0):
        self.name = name
        self.log_level = log_level

        self.__indent_str = ""
        for i in range(0,log_level):
            self.__indent_str += "  "

        self.log_depth = os.getenv('LOG_DEPTH')

    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, name):
        if name == None:
            try:
                self.__name = re.search("\.(.+?)'>", str(type(self))).group(1)
            except:
                self.__name = str(type(self))
        else:
            self.__name = name

    @property
    def log_level(self):
        return self.__log_level

    @log_level.setter
    def log_level(self, log_level):
        try:
            self.__log_level = int (log_level)
        except:
            self.__log_level=0

    @property
    def log_depth(self):
        return self.__log_depth

    @log_depth.setter
    def log_depth(self, log_depth):
        try:
            self.__log_depth = int (log_depth)
        except:
            self.__log_depth=100

    def __prefix(prefix):
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return "# [" + now_str + "]" + prefix

    def log_info(self, msg):
        if self.log_level <= self.log_depth:
            prefix = Loggable.__prefix(self.__indent_str + "["+self.name+"][INFO]: ")
            wrapper = textwrap.TextWrapper(
                initial_indent=prefix,
                width=180,
                subsequent_indent=' '*len(prefix))
            print(wrapper.fill(msg))

    def log_err(self, err):
        prefix = Loggable.__prefix(self.__indent_str + "["+self.name+"][ERR ]: ")
        wrapper = textwrap.TextWrapper(
            initial_indent=prefix,
            width=180,
            subsequent_indent=' '*len(prefix))
        print(wrapper.fill(err))
