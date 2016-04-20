#!/usr/bin/env python

import sys, os.path
import logging
import logging.config
#sys.path.append('..')
from base.gflags import *

DEFINE_integer('global_log_level',
                logging.INFO,
                ('FATAL:50 '
                'ERROR:40 '
                'WARNING:30'
                'INFO:20 '
                'DEBUG:10 '
                'NOTSET:0 '))
DEFINE_integer('global_log_file_maxbytes', 0, 'max byte for logratating file handler')
DEFINE_integer('global_log_file_backupcount', 0, 'backup count for logratating file handler')

formatters = {
        'verbose':logging.Formatter('[%(asctime)s][%(levelname)s][%(filename)s] %(funcName)s: %(lineno)d: %(message)s'),
        'normal' :logging.Formatter('[%(asctime)s][%(levelname)s]: %(message)s'),
        'none': logging.Formatter('%(message)s'),
        'raw' : logging.Formatter('[%(levelname)s] %(message)s'),
        'base': logging.Formatter(logging.BASIC_FORMAT),
        }

class CustomLog:
    def __init__(self, logger_name, level = logging.INFO, formatter = formatters['verbose']):
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(level)
        self.formatter = formatter

    def SetLevel(self, level):
        self.logger.setLevel(level)

    def SetFormat(self, formatter):
        self.logger.setFormatter(formatter)

    def AddHandler(self, handlerType, level = None, formatter = None, *args):
        handler = handlerType(*args)
        if level:
            handler.setLevel(level)
        if not formatter:
            handler.setLevel(self.formatter)
        self.logger.addHandler(handler)

    def AddHandlerObj(self, handler):
        self.logger.addHandler(handler)

    def GetLogger(self):
        return self.logger

def TimedLogRotatingFileHandler(
        filename, maxBytes=0, level = logging.INFO,
        formatter = formatters['verbose'], mode='a',
        backupCount=6, encoding=None, delay=0):
    handler = logging.handlers.TimedRotatingFileHandler(filename, 'midnight', 1, backupCount)
    handler.setFormatter(formatter)
    handler.setLevel(level)
    return handler

def LogRotatingFileHandler(
        filename, maxBytes=0, level = logging.INFO, 
        formatter = formatters['verbose'], mode='a',
        backupCount=0, encoding=None, delay=0):
    handler = logging.handlers.RotatingFileHandler(filename, mode, maxBytes, backupCount)
    handler.setFormatter(formatter)
    handler.setLevel(level)
    return handler

def LogConsoleHandler(level = logging.INFO, formatter = formatters['verbose']):
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    handler.setLevel(level)
    return handler

class GlobalLog:
    logger = None
    @staticmethod
    def Init(logger_name, filename = None, stdout = False, formatter = formatters['verbose'], ):
        level=FLAGS.global_log_level
        GlobalLog.logger = CustomLog(logger_name, level=level)
        if filename:
            max_bytes = FLAGS.global_log_file_maxbytes
            backup_count = FLAGS.global_log_file_backupcount
            GlobalLog.logger.AddHandlerObj(LogRotatingFileHandler(filename=filename, formatter=formatter, level=level, maxBytes=max_bytes, backupCount=backup_count))
        if stdout:
            GlobalLog.logger.AddHandlerObj(LogConsoleHandler(formatter=formatter, level=level))
        
    @staticmethod
    def GetLog(**kwargs):
        filename = kwargs.pop('filename', None)
        stdout = kwargs.pop('stdout', True)
        formatter = kwargs.pop('formatter', formatters['verbose'])
        if not GlobalLog.logger:
            GlobalLog.Init('globle',
                    filename=filename,
                    stdout=stdout,
                    formatter=formatter)
        return GlobalLog.logger.GetLogger()

InitGlobalLog = GlobalLog.Init
GloLog = GlobalLog.GetLog

if __name__ == "__main__":
    logger = CustomLog('shell')
    logger.AddHandlerObj(LogConsoleHandler(formatter=formatters['verbose']))
    logger.AddHandlerObj(LogConsoleHandler(formatter=formatters['normal']))
    logger.AddHandlerObj(LogConsoleHandler(formatter=formatters['base']))
    logger.AddHandlerObj(LogConsoleHandler(formatter=formatters['raw']))
    logger.AddHandlerObj(LogConsoleHandler(formatter=formatters['none']))
    logger = logger.GetLogger()
    logger.info("testingggggggggggg")

    logger = GloLog(formatter=formatters['none'])
    logger.info("testingggggggggggggggggg")
