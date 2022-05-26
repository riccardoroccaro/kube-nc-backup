import os
import re
import textwrap

from datetime import datetime

from kubencbackup.common.textcolor import TextColor

class Loggable:
    def __init__(self, name=None, log_level=0):
        self.name = name
        self.log_level = log_level

        self.__indent_str = ""
        for i in range(0,log_level):
            self.__indent_str += "  "

        self.log_depth = os.getenv('LOG_DEPTH')

        self.log_msg_col_len = os.getenv('LOG_MSG_COL_LEN')

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

    @property
    def log_msg_col_len(self):
        return self.__log_msg_col_len

    @log_msg_col_len.setter
    def log_msg_col_len(self, log_msg_col_len):
        try:
            self.__log_msg_col_len = int (log_msg_col_len)
        except:
            self.__log_msg_col_len=180

    def __prefix(prefix):
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return "# [" + now_str + "]" + prefix

    def log_info(self, msg):
        if self.log_level <= self.log_depth:
            prefix = Loggable.__prefix(self.__indent_str + "["+self.name+"]["+TextColor.GREEN()+"INFO"+TextColor.NO_COL()+"]: ")
            wrapper = textwrap.TextWrapper(
                initial_indent=prefix,
                width=self.log_msg_col_len,
                subsequent_indent=' '*len(prefix))
            print(wrapper.fill(msg))

    def log_err(self, err):
        prefix = Loggable.__prefix(TextColor.wrap_text(col=TextColor.RED(), text=self.__indent_str + "["+self.name+"][ERR#]: "))
        wrapper = textwrap.TextWrapper(
            initial_indent=prefix,
            width=self.log_msg_col_len,
            subsequent_indent=' '*(len(prefix) - TextColor.wrapped_text_overhead(col=TextColor.RED())))
        print(wrapper.fill(TextColor.wrap_text(col=TextColor.RED(), text=err)))

    def log_warn(self, msg):
        prefix = Loggable.__prefix(TextColor.wrap_text(col=TextColor.YELLOW(), text=self.__indent_str + "["+self.name+"][WARN]: "))
        wrapper = textwrap.TextWrapper(
            initial_indent=prefix,
            width=self.log_msg_col_len,
            subsequent_indent=' '*(len(prefix) - TextColor.wrapped_text_overhead(col=TextColor.YELLOW())))
        print(wrapper.fill(TextColor.wrap_text(col=TextColor.YELLOW(), text=msg)))

    def log_ok(self, msg):
        prefix = Loggable.__prefix(TextColor.wrap_text(col=TextColor.GREEN(), text=self.__indent_str + "["+self.name+"][OK##]: "))
        wrapper = textwrap.TextWrapper(
            initial_indent=prefix,
            width=self.log_msg_col_len,
            subsequent_indent=' '*(len(prefix) - TextColor.wrapped_text_overhead(col=TextColor.GREEN())))
        print(wrapper.fill(TextColor.wrap_text(col=TextColor.GREEN(), text=msg)))

    def log_debug(self, msg, date_time_enabled=True):
        if date_time_enabled:
            prefix = Loggable.__prefix(TextColor.wrap_text(col=TextColor.YELLOW(), text=self.__indent_str + "["+self.name+"][DEB#]: "))
        else:
            prefix = TextColor.wrap_text(col=TextColor.YELLOW(), text=self.__indent_str + "["+self.name+"][DEB#]: ")
        wrapper = textwrap.TextWrapper(
            initial_indent=prefix,
            width=self.log_msg_col_len,
            subsequent_indent=' '*len(prefix))
        print(wrapper.fill(TextColor.wrap_text(col=TextColor.YELLOW(),text=msg)))
