# -*- coding: utf-8 -*-

from rake_nltk import Rake
from amazonFrontCrawl import settings
import logging
from twisted.enterprise import adbapi
import MySQLdb
import MySQLdb.cursors


class CompetitorAnalysisApp():
    def __init__(self, dbpool):
        self.dbpool = dbpool
        ''' 这里注释中采用写死在代码中的方式连接线程池，可以从settings配置文件中读取，更加灵活
            self.dbpool=adbapi.ConnectionPool('MySQLdb',
                                          host='127.0.0.1',
                                          db='amazonFrontCrawl',
                                          user='root',
                                          passwd='root123',
                                          cursorclass=MySQLdb.cursors.DictCursor,
                                          charset='utf8',
                                          use_unicode=False)'''

    @classmethod
    def from_settings(cls, settings):
        '''1、@classmethod声明一个类方法，而对于平常我们见到的则叫做实例方法。
           2、类方法的第一个参数cls（class的缩写，指这个类本身），而实例方法的第一个参数是self，表示该类的一个实例
           3、可以通过类来调用，就像C.f()，相当于java中的静态方法'''
        dbparams = dict(
            host=settings['MYSQL_HOST'],  # 读取settings中的配置
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            passwd=settings['MYSQL_PASSWD'],
            charset='utf8',  # 编码要加上，否则可能出现中文乱码问题
            cursorclass=MySQLdb.cursors.DictCursor,
            use_unicode=False,
        )
        dbpool = adbapi.ConnectionPool('MySQLdb', **dbparams)  # **表示将字典扩展为关键字参数,相当于host=xxx,db=yyy....
        logging.info("connect mysql db ......")
        return cls(dbpool)  # 相当于dbpool付给了这个类，self中可以得到

    def get_all_products_in_sales(self):
        pass


def get_top_n_keywords_from_text(text, n):
    r = Rake()
    r.extract_keywords_from_text(text)
    lst = list(r.get_ranked_phrases_with_scores())

    return lst[:n]

if __name__ == '__main__':
    text = 'LE 10W RGB LED Flood Lights, Outdoor Color Changing LED Security Light, 16 Colors & 4 Modes with Remote Control, IP66 Waterproof LED Floodlight, US 3-Plug, Wall Washer Light '
    n = 11
    for a,b in get_top_n_keywords_from_text(text, n):
        print(a,b)