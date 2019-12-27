#!/usr/bin/python
# -*- encoding:utf-8 -*-

"""
@author: xuanyu
@contact: xuanyu@126.com
"""

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import TScan


if __name__ == '__main__':

    #
    host = 'bigdata-training01.hpsk.com'
    port = 9090

    # Client
    transport = TTransport.TBufferedTransport(TSocket.TSocket(host, port))
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    client = Hbase.Client(protocol)
    transport.open()

    # 获取所有表
    table_names = client.getTableNames()
    for table_name in table_names:
        print table_name

    """
        =====================================================================
    """
    # 获取数据
    scan = TScan()
    scanner = client.scannerOpenWithScan('wc_count', scan, None)
    # 获取两条数据
    result_list = client.scannerGetList(scanner, 10)

    for result in result_list:
        # print result
        # # print '====================================='
        # print result.columns
        for (column, value) in result.columns.items():
            print column + ' -> ' + value.value
        print '====================================='
