#!/usr/bin/python2.6

"""
Do thrift encapsulation works
"""

__author__ = 'zky@msn.cn (CarreyZhan)'

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TFramedTransport, TBufferedTransport
from thrift.protocol.TBinaryProtocol import TBinaryProtocol

def FromThriftToString(obj):
    membuffer = TMemoryBuffer()
    protocol = TBinaryProtocol(membuffer)
    obj.write(protocol)
    buf = membuffer.getvalue()
    return format(buf)

def FromStringToThrift(buf, obj):
    membuffer = TMemoryBuffer(buf)
    protocol = TBinaryProtocol(membuffer)
    obj.read(protocol)

TFRAMED_TRANSPORT_NAME = "TFramedTransport"
TBUFFERED_TRANSPORT_NAME = "TBufferedTransport"
DEFAULT_TCONNECTION_TIMEOUT = 500
DEFAULT_TRECEIVE_TIMEOUT = 1000
DEFAULT_TSEND_TIMEOUT = 1000

class ThriftClient(object):
    def __init__(self, host, port, service_class, **kwargs):
        self._host = host
        self._port = port
        self._connection_timeout = kwargs.get('connection_time', DEFAULT_TCONNECTION_TIMEOUT)
        self._receive_timeout = kwargs.get('receive_timeout', DEFAULT_TRECEIVE_TIMEOUT)
        self._send_timeout = kwargs.get('send_timeout', DEFAULT_TSEND_TIMEOUT)
        self._transport_class = kwargs.get('transport_class', TFRAMED_TRANSPORT_NAME)
        self._service_class = service_class
        self._connect = False

    def get_service(self):
        self.__connect_if_necessary()
        return self._client

    def __connect_if_necessary(self):
        if self._connect:
            return

        self._socket = TSocket(self._host, self._port)
        if (self._transport_class == TBUFFERED_TRANSPORT_NAME):
            self._transport = TBufferedTransport(self._socket)
        else:
            self._transport = TFramedTransport(self._socket)
        self._transport.open()
        self._protocol = TBinaryProtocol(self._transport)
        self._client = self._service_class.Client(self._protocol)
        self._connect = True
