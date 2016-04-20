#!/usr/bin/env python
# -*- coding:utf8 -*-

__author__ = 'zky@msn.cn (CarreyZhan)'

import sys
sys.path.append('../')
sys.path.append('../third_party/')
sys.path.append('../third_party/hbase/gen-py/')

from model_base import *

class DemoTable(HBaseModel):
    table_name = 'demo_table'
    fields = ('rowkey',
              'entry:num',
              'entry:sqr')

if __name__ == '__main__':
    try:
        argv = FLAGS(sys.argv)
    except FlagsError, e:
        print '%s \\nUsage: %s ARGS\\n%s' % (e,sys.argv[-1], FLAGS)
        sys.exit(1)

    #datas = DemoTable.filter()
    #datas = DemoTable.filter(STARTROW='00005', STOPROW='00008')
    #datas = DemoTable.filter(STARTROW='00005', COLUMNS=['entry:num'])
    datas = DemoTable.filter(FILTER="PrefixFilter ('0001')")
    for d in datas:
        print d
        if d.rowkey == '00000':
            DemoTable.delete([d])
        if d.rowkey == '00001':
            d['entry:sqr'] = '11111'
            DemoTable.save([d])
        if d.rowkey == '00002':
            print d['entry:sqr']
            d['entry:sqr'] = '2中文2'
            DemoTable.save([d])
