# Functionality : logger management
# Version       : V1.0 (09/17/2017)
# Author        : wSun


import logging


class MyLogger:
    def __init__(self, logFile):
        self.logFile = logFile
        self.make_config()

    def make_config(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename=self.logFile,
                            filemode='a')

    def log_info(self, info):
        logging.info(info)

    def log_debug(self, info):
        logging.debug(info)

    def log_warning(self, info):
        logging.warning(info)

    def log_error(self, info):
        logging.error(info)

    def log_critical(self, info):
        logging.critical(info)


def Main():
    mylog = MyLogger('myapp.log')
    mylog.log_info('Start:---------------------------')
    mylog.log_debug('This is debug message')
    mylog.log_info('This is info message')
    mylog.log_warning('This is warning message')
    mylog.log_error('This is error message')
    mylog.log_critical('This is critical message')
    mylog.log_info('completion\n')

if __name__ == '__main__':
    Main()