#coding:utf-8
import random
import base64
from amazonFrontCrawl import settings
# from jiandan.db.db_helper import DB_Helper
from twisted.enterprise import adbapi
import MySQLdb
import MySQLdb.cursors
import logging
from scrapy.downloadermiddlewares.httpproxy import HttpProxyMiddleware

'''
这个类主要用于产生随机代理
'''

# 代理服务器
proxyServer = "http://proxy.abuyun.com:9020"

# 隧道身份信息
proxyUser = "H87KQVT974B8KG4D"
proxyPass = "39748092BAF77BA7"
proxyAuth = "Basic " + base64.urlsafe_b64encode(proxyUser + ":" + proxyPass)


class RandomProxyMiddleware(HttpProxyMiddleware):

    proxies = {}

    def __init__(self, auth_encoding='latin-1'):#初始化一下数据库连接
        """
        # install db
        self.db_helper = DB_Helper()
        self.count =self.db_helper.proxys.count()
        """

        self.auth_encoding = auth_encoding
        self.proxies[proxyServer] = proxyUser + proxyPass

    def process_request(self, request, spider):
        '''
        在请求上添加代理
        :param request:
        :param spider:
        :return:
        '''

        request.meta["proxy"] = proxyServer

        request.headers["Proxy-Authorization"] = proxyAuth


    # 错误处理方法
    def _handle_error(self, failue, item, spider):
        print '--------------database operation exception!!-----------------'
        print '-------------------------------------------------------------'
        print failue
        print '-------------------------------------------------------------'

