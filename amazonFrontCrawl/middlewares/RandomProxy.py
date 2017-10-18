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
        # self.dbpool = dbpool

        self.auth_encoding = auth_encoding
        self.proxies[proxyServer] = proxyUser + proxyPass

    # @classmethod
    # def from_settings(cls, settings):
    #     '''1、@classmethod声明一个类方法，而对于平常我们见到的则叫做实例方法。
    #        2、类方法的第一个参数cls（class的缩写，指这个类本身），而实例方法的第一个参数是self，表示该类的一个实例
    #        3、可以通过类来调用，就像C.f()，相当于java中的静态方法'''
    #     dbparams = dict(
    #         host=settings['MYSQL_HOST'],  # 读取settings中的配置
    #         db=settings['MYSQL_DBNAME'],
    #         user=settings['MYSQL_USER'],
    #         passwd=settings['MYSQL_PASSWD'],
    #         charset='utf8',  # 编码要加上，否则可能出现中文乱码问题
    #         cursorclass=MySQLdb.cursors.DictCursor,
    #         use_unicode=False,
    #     )
    #     dbpool = adbapi.ConnectionPool('MySQLdb', **dbparams)  # **表示将字典扩展为关键字参数,相当于host=xxx,db=yyy....
    #     logging.info("connect mysql db ......")
    #     return cls(dbpool)  # 相当于dbpool付给了这个类，self中可以得到

    # get proxy IP list from mysql
    # def fetch_proxy_ip_list(self, tx, item):
    #     logging.info("RandomProxy.py:select ip,port from amazon_front_crawl_proxies ......")
    #     try:
    #         sql = "select ip,port from amazon_front_crawl_proxies order by rand() limit 30"
    #         return tx.execute(sql)
    #     except MySQLdb.Error, e:
    #         logging.error("Error %d:%s" % (e.args[0], e.args[1]))

    def process_request(self, request, spider):
        '''
        在请求上添加代理
        :param request:
        :param spider:
        :return:
        '''

        request.meta["proxy"] = proxyServer

        request.headers["Proxy-Authorization"] = proxyAuth

        # idList = range(1,self.count+1)
        # id = random.choice(idList)
        # result = self.db_helper.findOneResult({'proxyId':id})



        # # logging.info("random choose proxies IP from settings......")
        # proxy = random.choice(settings.PROXIES)
        # if proxy['user_pass'] is not None:
        #     request.meta['proxy'] = "http://%s" % proxy['ip_port']
        #     encoded_user_pass = base64.encodestring(proxy['user_pass'])
        #     print(encoded_user_pass)
        #     request.headers['Proxy-Authorization'] = 'Basic ' + encoded_user_pass
        #     print "**************ProxyMiddleware have pass************" + proxy['ip_port']
        # else:
        #     print "**************ProxyMiddleware no pass************" + proxy['ip_port']
        #     request.meta['proxy'] = "http://%s" % proxy['ip_port']


        # random choose proxy IP from mysql
        # conn = MySQLdb.connect(user='root', passwd='root123', db='amazonFrontCrawl', host='127.0.0.1', charset="utf8",
        #                        use_unicode=True)
        # cursor = conn.cursor()
        # cursor.execute('select ip,port from amazon_front_crawl_proxies order by score desc,id desc limit 5;')
        # proxy_list = cursor.fetchall()
        # proxy = random.choice(proxy_list)
        # ip = proxy[0].encode('utf-8')+':'+str(proxy[1])
        # request.meta['proxy'] = "http://%s" % ip

        #request.meta['proxy'] =settings.HTTP_PROXY

    # 错误处理方法
    def _handle_error(self, failue, item, spider):
        print '--------------database operation exception!!-----------------'
        print '-------------------------------------------------------------'
        print failue
        print '-------------------------------------------------------------'

