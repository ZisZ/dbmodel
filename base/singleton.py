#!/usr/bin/env python

__author__ = 'zky@msn.cn'

class Singleton(type):
  _instances = {}
  def __call__(cls, *args, **kwargs):
    if cls not in cls._instances:
      cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
    return cls._instances[cls]

if __name__ == '__main__':
  s_init_num = 0
  ns_init_num = 0

  class SObj(object):
    __metaclass__ = Singleton
  
    def __init__(self):
      global s_init_num
      s_init_num += 1
      assert s_init_num == 1
      print "Singleton: should only run once: %d" % s_init_num

  class NSObj(object):
    def __init__(self):
      global ns_init_num
      ns_init_num += 1
      #assert ns_init_num == 1 # assert fail
      print "Non-Singleton: may run multi times: %d" % ns_init_num

  def unittest():
    s1 = SObj()
    s2 = SObj()
    assert s1 == s2

    ns1 = NSObj()
    ns2 = NSObj()
    assert ns1 != ns2

  unittest()
