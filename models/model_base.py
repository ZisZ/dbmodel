#!/usr/bin/env python
# -*- coding:utf8 -*-

__author__ = 'zky@msn.cn (CarreyZhan)'

import sys
sys.path.append('../')
sys.path.append('../third_party/')
sys.path.append('../third_party/hbase/gen-py/')

import traceback
import MySQLdb

from base.singleton import Singleton
from base.gflags import *
from util.customlog import GloLog

from base.thrift_base import ThriftClient
from hbase.ttypes import *
from hbase import Hbase

DEFINE_string('rdb_user', 'data_matrix', 'user name of rdb')
DEFINE_string('rdb_password', 'data_matrix', 'user password of rdb')
DEFINE_string('rdb_name', 'data_matrix', 'name of rdb')
DEFINE_string('rdb_host', '', 'host of rdb')

DEFINE_string('hbase_thrift_host', '', 'host of hbase thrift server')
DEFINE_string('hbase_thrift_port', '', 'port of hbase thrift server')

'''
    construct a record with a dict
'''
class Record(object):
    def __init__(self, dic, **kwargs):
        self.attr_dict = dic

    def __getattr__(self, attr):
        return self.attr_dict[attr]

    def __str__(self):
        return str(self.attr_dict)

    def __getitem__(self, attr):
        return self.attr_dict[attr]

    def __setitem__(self, attr, value):
        self.attr_dict[attr] = value

class ModelBase(object):
    __metaclass__ = Singleton

    def _filter(self, **kwargs):
        ret = []
        return ret

    def _save(self, records):
        self.logger.error("Not implemented.")
        return 0

    def _delete(self, records):
        self.logger.error("Not implemented.")
        return 0

    @classmethod
    def filter(cls, **kwargs):
        ret = []
        obj = cls()
        records = obj._filter(**kwargs)
        for record in records:
            if isinstance(record, list) or isinstance(record, tuple):
                dic = {}
                for i in range(len(cls.fields)):
                    dic[cls.fields[i]] = record[i]
                ret.append(Record(dic))
            elif isinstance(record, dict):
                ret.append(Record(record))
            else:
                obj.logger.error("illegal record type %s" % type(record))
                return None
        return ret

    @classmethod
    def save(cls, records):
        obj = cls()
        return obj._save(records)

    @classmethod
    def delete(cls, records):
        obj = cls()
        return obj._delete(records)

class RDBModel(ModelBase):
    def __init__(self, **kwargs):
        # db conf
        self.logger = kwargs.get('logger', GloLog())
        self.config = {}
        self.config['user'] = kwargs.get('user', FLAGS.rdb_user)
        self.config['passwd'] = kwargs.get('passwd', FLAGS.rdb_password)
        self.config['host'] = kwargs.get('host', FLAGS.rdb_host)
        self.config['db'] = kwargs.get('db', FLAGS.rdb_name)

        # table conf
        if hasattr(self.__class__, 'table_name'):
            self.table_name = self.__class__.table_name
        else:
            self.table_name = classname

        self.logger.info("connect to db with config: %s" % self.config)
        try:
            self.dbcon_ = MySQLdb.connect(user=self.config['user'],
                                          passwd=self.config['passwd'],
                                          db=self.config['db'],
                                          host=self.config['host'],
                                          charset='utf8')
            self.dbcur_ = self.dbcon_.cursor()
        except Exception,e:
            self.logger.error("Could not connect to rdb")
            self.logger.error(e)

    def _filter(self, **kwargs):
        ret = []
        sql = 'SELECT * FROM %s' % self.table_name
        first = True
        for key in kwargs:
            if first:
                sql += ' WHERE'
                first = False
            else:
                sql += ' AND'
            sql += ' `%s`=\'%s\'' % (key, kwargs[key])
        self.dbcur_.execute(sql)
        if self.dbcur_.rowcount == 0:
            return ret

        for row in self.dbcur_.fetchall():
            ret.append(row)
        return ret

class HBaseModel(ModelBase):
    def __init__(self, **kwargs):
        # hbase thrift conf
        self.logger = kwargs.get('logger', GloLog())
        self.config = {}
        self.config['thrift_host'] = kwargs.get('host', FLAGS.hbase_thrift_host)
        self.config['thrift_port'] = kwargs.get('port', FLAGS.hbase_thrift_port)

        # table conf
        if hasattr(self.__class__, 'table_name'):
            self.table_name = self.__class__.table_name
        else:
            self.table_name = classname

        try:
            self.service_ = ThriftClient(host=self.config['thrift_host'],
                                         port=self.config['thrift_port'],
                                         service_class=Hbase,
                                         transport_class='TBufferedTransport')
            self.service_ = self.service_.get_service()
        except Exception,e:
            self.logger.error("Could not connect to hbase thrift server")
            self.logger.error(e)
            self.logger.debug(traceback.format_exc())

    def _filter(self, **kwargs):
        ret = []
        all_columns = self.__class__.fields[1:]
        start_row = kwargs.get('STARTROW', None)
        stop_row = kwargs.get('STOPROW', None)
        timestamp = kwargs.get('TIMESTAMP', None)
        # NOTE: DO NOT support COLUMNS param
        columns = kwargs.get('COLUMNS', self.__class__.fields[1:])
        for c in columns:
            if not c in all_columns:
                self.logger.error("Illegal filter column %s" % c)
                return ret
        #columns = self.__class__.fields[1:]
        caching = kwargs.get('CACHING', None)
        filter_string = kwargs.get('FILTER', None)
        batch_size = kwargs.get('BATCHSIZE', None)
        sort_columns = kwargs.get('SORTCOLUMNS', None)
        t_scan = TScan(start_row,
                       stop_row,
                       timestamp,
                       columns,
                       caching,
                       filter_string,
                       batch_size,
                       sort_columns)
        scanner = self.service_.scannerOpenWithScan(self.table_name, t_scan, None)

        while True:
            value = self.service_.scannerGet(scanner)
            if len(value) == 0:
                break
            # len(value) == 1
            row = value[0]
            r = {}
            r[self.__class__.fields[0]] = row.row
            for c in columns:
                r[c] = row.columns[c].value
            ret.append(r)
        self.service_.scannerClose(scanner)
        return ret

    def _save(self, records):
        ret = 0
        columns = self.__class__.fields[1:]
        row_batches = []
        for r in records:
            mutations = []
            for c in columns:
                mutations.append(Mutation(column=c, value=r[c]))
            row_batches.append(BatchMutation(row=r.rowkey, mutations=mutations))
            ret += 1
        self.service_.mutateRows(self.table_name, row_batches, None)
        return ret

    def _delete(self, records):
        ret = 0
        columns = self.__class__.fields[1:]
        row_batches = []
        for r in records:
            mutations = []
            for c in columns:
                mutations.append(Mutation(isDelete=True, column=c))
            row_batches.append(BatchMutation(row=r.rowkey, mutations=mutations))
            ret += 1
        self.service_.mutateRows(self.table_name, row_batches, None)
        return ret
