import re
import textwrap

from datetime import datetime

class Loggable:
    def __init__(self, name=None):
        self.name = name

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

    def __prefix(prefix):
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return "# [" + now_str + "]" + prefix

    def log_info(self, msg):
        prefix = Loggable.__prefix("["+self.name+"][INFO]: ")
        wrapper = textwrap.TextWrapper(
            initial_indent=prefix,
            width=180,
            subsequent_indent=' '*len(prefix))
        print(wrapper.fill(msg))

    def log_err(self, err):
        prefix = Loggable.__prefix("["+self.name+"][ERR ]: ")
        wrapper = textwrap.TextWrapper(
            initial_indent=prefix,
            width=180,
            subsequent_indent=' '*len(prefix))
        print(wrapper.fill(err))
