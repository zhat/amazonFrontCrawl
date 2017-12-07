# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from twisted.enterprise import adbapi
import MySQLdb
import MySQLdb.cursors
import pymysql
import codecs
import json
import logging
import datetime
from items import AmazonProductInsalesItem, AmazonTop100BestSellersItem, AmazonTop100GiftIdeasItem, AmazonTop100MostWishedItem, AmazonTop100HotNewReleasesItem
from items import amazon_product_reviews, amazon_product_baseinfo, amazon_product_technical_details, amazon_product_category_sales_rank
from items import amazon_product_descriptions, amazon_product_pictures, amazon_product_bought_together_list, amazon_product_also_bought_list
from items import amazon_product_current_reviews, amazon_product_promotions, amazon_traffic_sponsored_products, amazon_traffic_buy_other_after_view
from items import amazon_traffic_similar_items, amazon_keyword_search_rank, amazon_keyword_search_sponsered,FeedbackItem
from items import amazon_product_review_percent_info, amazon_product_review_result, amazon_top_reviewers,TodayDealItem
from scrapy.pipelines.files import FilesPipeline
from scrapy.exceptions import DropItem
from amazonFrontCrawl import settings
import scrapy
import re

class JsonWithEncodingPipeline(object):
    '''保存到文件中对应的class
       1、在settings.py文件中配置
       2、在自己实现的爬虫类中yield item,会自动执行'''

    def __init__(self):
        self.file = codecs.open('info.json', 'w', encoding='utf-8')  # 保存为json文件

    def process_item(self, item, spider):
        line = json.dumps(dict(item)) + "\n"  # 转为json的
        self.file.write(line)  # 写入文件中
        return item

    def spider_closed(self, spider):  # 爬虫结束时关闭文件
        self.file.close()


class TodayDealsPipeline(object):
    '''保存到数据库中对应的class
       1、在settings.py文件中配置
       2、在自己实现的爬虫类中yield item,会自动执行'''

    def __init__(self):
        user = settings.MYSQL_USER
        passwd = settings.MYSQL_PASSWD
        db = settings.MYSQL_DBNAME
        host = settings.MYSQL_HOST
        self.conn = MySQLdb.connect(
            user=user,
            passwd=passwd,
            db=db,
            host=host,
            charset="utf8",
            use_unicode=True
        )
    # pipeline默认调用
    def process_item(self, item, spider):
        if isinstance(item, TodayDealItem):
            self._today_deal_insert(item)

    def close_spider(self, spider):
        self.conn.close()

    # amazon_today_deal 写入数据库中
    def _today_deal_insert(self,item):
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "insert into amazon_today_deal(date,zone,asin,page,page_index,deal_url,deal_type,create_time,update_time) " \
                  "values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        #item_list = item["deals"]
        params = []
        for item in item["deals"]:
            params.append((item["date"], item["zone"], item["asin"], item["page"], item["page_index"],
                      item["deal_url"], item["deal_type"], dt, dt))
        logging.info("insert data to amazon_keyword_search_sponsered ......")
        cursor = self.conn.cursor()
        cursor.executemany(sql, params)
        self.conn.commit()
        cursor.close()
        #tx.execute(sql, params)

    # 错误处理方法
    def _handle_error(self, failue, item, spider):
        print '--------------database operation exception!!-----------------'
        print '-------------------------------------------------------------'
        print failue
        print '-------------------------------------------------------------'

class KeywordSearchPipeline(object):
    '''保存到数据库中对应的class
       1、在settings.py文件中配置
       2、在自己实现的爬虫类中yield item,会自动执行'''

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

    # pipeline默认调用
    def process_item(self, item, spider):
        if isinstance(item, amazon_keyword_search_rank):
            query = self.dbpool.runInteraction(self._amazon_keyword_search_rank_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        elif isinstance(item, amazon_keyword_search_sponsered):
            query = self.dbpool.runInteraction(self._amazon_keyword_search_sponsered_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        else:
            pass
        return item

    # amazon_keyword_search_sponsered 写入数据库中
    def _amazon_keyword_search_sponsered_insert(self, tx, item):
        sql = "insert into amazon_keyword_search_sponsered(zone,asin,ref_id,pos_type,page_index,search_rank_index) " \
              "values(%s,%s,%s,%s,%s,%s)"
        params = (item["zone"], item["asin"], item["ref_id"], item["pos_type"], item["page_index"], item["search_rank_index"])
        logging.info("insert data to amazon_keyword_search_sponsered ......")
        tx.execute(sql, params)

    # amazon_keyword_search_rank 写入数据库中
    def _amazon_keyword_search_rank_insert(self, tx, item):
        sql = "insert into amazon_keyword_search_rank(zone,asin,ref_id,page_index,sponsored_flag,search_rank_index) " \
              "values(%s,%s,%s,%s,%s,%s)"
        params = (item["zone"], item["asin"], item["ref_id"], item["page_index"], item["sponsored_flag"], item["search_rank_index"])
        logging.info("insert data to amazon_keyword_search_rank ......")
        tx.execute(sql, params)

    # 错误处理方法
    def _handle_error(self, failue, item, spider):
        print '--------------database operation exception!!-----------------'
        print '-------------------------------------------------------------'
        print failue
        print '-------------------------------------------------------------'


class AmazonTopReviewerPipeline(object):
    '''保存到数据库中对应的class
       1、在settings.py文件中配置
       2、在自己实现的爬虫类中yield item,会自动执行'''

    def __init__(self, dbpool):
        self.dbpool = dbpool

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

    # pipeline默认调用
    def process_item(self, item, spider):
        if isinstance(item, amazon_top_reviewers):
            query = self.dbpool.runInteraction(self._amazon_top_reviewers_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        elif isinstance(item, object):
            pass
        else:
            pass
        return item

    # amazon_top_reviewers 写入数据库中
    def _amazon_top_reviewers_insert(self, tx, item):
        # print item['name']
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "insert into amazon_top_reviewers(zone, ind, reviewer_name, reviewer_url, total_reviews, helpful_votes, percent_helpful, page_index, time_id) " \
              "values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        params = (item["zone"], item["ind"], item["reviewer_name"], item["reviewer_url"], item["total_reviews"], item["helpful_votes"], item["percent_helpful"], item["page_index"], item["time_id"])
        logging.info("insert data to amazon_product_insales ......")
        tx.execute(sql, params)

    # 错误处理方法
    def _handle_error(self, failue, item, spider):
        print '--------------database operation exception!!-----------------'
        print '-------------------------------------------------------------'
        print failue
        print '-------------------------------------------------------------'



class ProductInsalesPipeline(object):
    '''保存到数据库中对应的class
       1、在settings.py文件中配置
       2、在自己实现的爬虫类中yield item,会自动执行'''

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

    # pipeline默认调用
    def process_item(self, item, spider):
        if isinstance(item, AmazonProductInsalesItem):
            query = self.dbpool.runInteraction(self._amazon_product_insales_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        elif isinstance(item, object):
            pass
        else:
            pass
        return item

    # AmazonProductInsalesItem 写入数据库中
    def _amazon_product_insales_insert(self, tx, item):
        # print item['name']
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "insert into amazon_product_insales(shop_id,zone,brand,asin,order_index,create_date, time_id) " \
              "values(%s,%s,%s,%s,%s,%s,%s)"
        params = (item["shop_id"], item["zone"], item["brand"], item["asin"], item["order_index"], dt, item["time_id"])
        logging.info("insert data to amazon_product_insales ......")
        tx.execute(sql, params)

    # 错误处理方法
    def _handle_error(self, failue, item, spider):
        print '--------------database operation exception!!-----------------'
        print '-------------------------------------------------------------'
        print failue
        print '-------------------------------------------------------------'

class Top100SeriesPipeline(object):
    '''保存到数据库中对应的class
       1、在settings.py文件中配置
       2、在自己实现的爬虫类中yield item,会自动执行'''

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

    # pipeline默认调用
    def process_item(self, item, spider):
        if isinstance(item, AmazonTop100BestSellersItem):
            query = self.dbpool.runInteraction(self._amazon_top100_best_sellers_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        elif isinstance(item, AmazonTop100GiftIdeasItem):
            query = self.dbpool.runInteraction(self._amazon_top100_gift_ideas_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        elif isinstance(item, AmazonTop100MostWishedItem):
            query = self.dbpool.runInteraction(self._amazon_top100_most_wished_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        elif isinstance(item, AmazonTop100HotNewReleasesItem):
            query = self.dbpool.runInteraction(self._amazon_top100_hot_new_releases_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        else:
            pass
        return item

    # amazon_top100_best_sellers 写入数据库中
    def _amazon_top100_best_sellers_insert(self, tx, item):
        # print item['name']
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "insert into amazon_top100_best_sellers(zone,category_name,category_url,asin,order_index,batch_number,create_date) " \
              "values(%s,%s,%s,%s,%s,%s,%s)"
        # print(sql)
        params = (item["zone"], item["category_name"], item["category_url"], item["asin"], item["order_index"],
                  item["batch_number"], dt)
        # print(params)
        logging.info("insert data to amazon_top100_best_sellers ......")
        tx.execute(sql, params)

    # amazon_top100_gift_ideas 写入数据库中
    def _amazon_top100_gift_ideas_insert(self, tx, item):
        # print item['name']
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "insert into amazon_top100_gift_ideas(zone,category_name,category_url,asin,order_index,batch_number,create_date) " \
              "values(%s,%s,%s,%s,%s,%s,%s)"
        print(sql)
        params = (item["zone"], item["category_name"], item["category_url"], item["asin"], item["order_index"],
                  item["batch_number"], dt)
        print(params)
        logging.info("insert data to amazon_top100_gift_ideas ......")
        tx.execute(sql, params)

    # amazon_top100_hot_new_releases 写入数据库中
    def _amazon_top100_hot_new_releases_insert(self, tx, item):
        # print item['name']
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "insert into amazon_top100_hot_new_releases(zone,category_name,category_url,asin,order_index,batch_number,create_date) " \
              "values(%s,%s,%s,%s,%s,%s,%s)"
        print(sql)
        params = (item["zone"], item["category_name"], item["category_url"], item["asin"], item["order_index"],
                  item["batch_number"], dt)
        print(params)
        logging.info("insert data to amazon_top100_hot_new_releases ......")
        tx.execute(sql, params)

    # amazon_top100_most_wished 写入数据库中
    def _amazon_top100_most_wished_insert(self, tx, item):
        # print item['name']
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "insert into amazon_top100_most_wished(zone,category_name,category_url,asin,order_index,batch_number,create_date) " \
              "values(%s,%s,%s,%s,%s,%s,%s)"
        print(sql)
        params = (item["zone"], item["category_name"], item["category_url"], item["asin"], item["order_index"],
                  item["batch_number"], dt)
        print(params)
        logging.info("insert data to amazon_top100_most_wished ......")
        tx.execute(sql, params)

    # 错误处理方法
    def _handle_error(self, failue, item, spider):
        print '--------------database operation exception!!-----------------'
        print '-------------------------------------------------------------'
        print failue
        print '-------------------------------------------------------------'


class ProductListingPipeline(object):
    '''保存到数据库中对应的class
       1、在settings.py文件中配置
       2、在自己实现的爬虫类中yield item,会自动执行'''

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

    # pipeline默认调用
    def process_item(self, item, spider):
        if isinstance(item, amazon_product_baseinfo):
            query = self.dbpool.runInteraction(self._amazon_product_baseinfo_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        elif isinstance(item, amazon_product_technical_details):
            query = self.dbpool.runInteraction(self._amazon_product_technical_details_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        elif isinstance(item, amazon_product_category_sales_rank):
            query = self.dbpool.runInteraction(self._amazon_product_category_sales_rank_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        elif isinstance(item, amazon_product_descriptions):
            query = self.dbpool.runInteraction(self._amazon_product_descriptions_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        elif isinstance(item, amazon_product_pictures):
            query = self.dbpool.runInteraction(self._amazon_product_pictures_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        elif isinstance(item, amazon_product_bought_together_list):
            query = self.dbpool.runInteraction(self._amazon_product_bought_together_list_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        elif isinstance(item, amazon_product_also_bought_list):
            query = self.dbpool.runInteraction(self._amazon_product_also_bought_list_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        elif isinstance(item, amazon_product_current_reviews):
            query = self.dbpool.runInteraction(self._amazon_product_current_reviews_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        elif isinstance(item, amazon_product_promotions):
            query = self.dbpool.runInteraction(self._amazon_product_promotions_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        elif isinstance(item, amazon_traffic_sponsored_products):
            query = self.dbpool.runInteraction(self._amazon_traffic_sponsored_products_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        elif isinstance(item, amazon_traffic_buy_other_after_view):
            query = self.dbpool.runInteraction(self._amazon_traffic_buy_other_after_view_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        elif isinstance(item, amazon_traffic_similar_items):
            query = self.dbpool.runInteraction(self._amazon_traffic_similar_items_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        else:
            pass
        return item

    # amazon_traffic_similar_items 写入数据库中
    def _amazon_traffic_similar_items_insert(self, tx, item):
        # print item['name']
        # dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = "insert into amazon_traffic_similar_items(zone, asin, ref_id, product_list) " \
              "values(%s,%s,%s,%s)"
        params = (
            item["zone"], item["asin"], item["ref_id"], item["product_list"]
        )
        print("insert data to amazon_traffic_similar_items ......%s") % item["asin"]
        logging.info("insert data to amazon_traffic_similar_items ......")
        tx.execute(sql, params)

    # amazon_traffic_buy_other_after_view 写入数据库中
    def _amazon_traffic_buy_other_after_view_insert(self, tx, item):
        # print item['name']
        # dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = "insert into amazon_traffic_buy_other_after_view(zone, asin, ref_id, product_list) " \
              "values(%s,%s,%s,%s)"
        params = (
            item["zone"], item["asin"], item["ref_id"], item["product_list"]
        )
        print("insert data to amazon_traffic_buy_other_after_view ......%s") % item["asin"]
        logging.info("insert data to amazon_traffic_buy_other_after_view ......")
        tx.execute(sql, params)

    # amazon_traffic_sponsored_products 写入数据库中
    def _amazon_traffic_sponsored_products_insert(self, tx, item):
        # print item['name']
        # dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = "insert into amazon_traffic_sponsored_products(zone, asin, ref_id, type, product_list) " \
              "values(%s,%s,%s,%s,%s)"
        params = (
            item["zone"], item["asin"], item["ref_id"], item["type"], item["product_list"]
        )
        print("insert data to amazon_traffic_sponsored_products ......%s") % item["asin"]
        logging.info("insert data to amazon_traffic_sponsored_products ......")
        tx.execute(sql, params)

    # amazon_product_promotions 写入数据库中
    def _amazon_product_promotions_insert(self, tx, item):
        # print item['name']
        # dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = "insert into amazon_product_promotions(zone, asin, ref_id, promotion_list, md5_promotion) " \
              "values(%s,%s,%s,%s,%s)"
        params = (
            item["zone"], item["asin"], item["ref_id"], item["promotion_list"], item["md5_promotion"]
        )
        print("insert data to amazon_product_promotions ......%s") % item["asin"]
        logging.info("insert data to amazon_product_promotions ......")
        tx.execute(sql, params)

    # amazon_product_current_reviews 写入数据库中
    def _amazon_product_current_reviews_insert(self, tx, item):
        # print item['name']
        # dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = "insert into amazon_product_current_reviews(zone, asin, ref_id, review_order_type," \
              " order_index, review_star, is_verified_purchase, votes, is_top_reviewer, cnt_imgs, cnt_vedios) " \
              "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        params = (
            item["zone"], item["asin"], item["ref_id"], item["review_order_type"],
            item["order_index"], item["review_star"], item["is_verified_purchase"], item["votes"],
            item["is_top_reviewer"], item["cnt_imgs"], item["cnt_vedios"]
        )
        print("insert data to amazon_product_current_reviews ......%s") % item["asin"]
        logging.info("insert data to amazon_product_current_reviews ......")
        tx.execute(sql, params)

    # amazon_product_also_bought_list 写入数据库中
    def _amazon_product_also_bought_list_insert(self, tx, item):
        # print item['name']
        # dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = "insert into amazon_product_also_bought_list(zone, asin, ref_id, also_bought_list) " \
              "values(%s,%s,%s,%s)"
        params = (
            item["zone"], item["asin"], item["ref_id"], item["also_bought_list"]
        )
        print("insert data to amazon_product_also_bought_list ......%s") % item["asin"]
        logging.info("insert data to amazon_product_also_bought_list ......")
        tx.execute(sql, params)

    # amazon_product_bought_together_list 写入数据库中
    def _amazon_product_bought_together_list_insert(self, tx, item):
        # print item['name']
        # dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = "insert into amazon_product_bought_together_list(zone, asin, ref_id, bought_together_list) " \
              "values(%s,%s,%s,%s)"
        params = (
            item["zone"], item["asin"], item["ref_id"], item["bought_together_list"]
        )
        print("insert data to amazon_product_bought_together_list ......%s") % item["asin"]
        logging.info("insert data to amazon_product_bought_together_list ......")
        tx.execute(sql, params)

    # amazon_product_pictures 写入数据库中
    def _amazon_product_pictures_insert(self, tx, item):
        # print item['name']
        # dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = "insert into amazon_product_pictures(zone, asin, ref_id, img_ind, img_url, md5_img_url) " \
              "values(%s,%s,%s,%s,%s,%s)"
        params = (
            item["zone"], item["asin"], item["ref_id"], item["img_ind"], item["img_url"], item["md5_img_url"]
        )
        print("insert data to amazon_product_pictures ......%s") % item["asin"]
        logging.info("insert data to amazon_product_pictures ......")
        tx.execute(sql, params)

    # amazon_product_descriptions 写入数据库中
    def _amazon_product_descriptions_insert(self, tx, item):
        # print item['name']
        # dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = "insert into amazon_product_descriptions(zone, asin, ref_id, description_type, desc_content, md5_desc_content) " \
              "values(%s,%s,%s,%s,%s,%s)"
        params = (
            item["zone"], item["asin"], item["ref_id"], item["description_type"], item["desc_content"], item["md5_desc_content"]
        )
        print("insert data to amazon_product_descriptions ......%s") % item["asin"]
        logging.info("insert data to amazon_product_descriptions ......")
        tx.execute(sql, params)

    # amazon_product_category_sales_rank 写入数据库中
    def _amazon_product_category_sales_rank_insert(self, tx, item):
        # print item['name']
        # dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = "insert into amazon_product_category_sales_rank(zone, asin, ref_id, category_name, sales_rank) " \
              "values(%s,%s,%s,%s,%s)"
        params = (
            item["zone"], item["asin"], item["ref_id"], item["category_name"], item["sales_rank"]
        )
        print("insert data to amazon_product_category_sales_rank ......%s") % item["asin"]
        logging.info("insert data to amazon_product_category_sales_rank ......")
        tx.execute(sql, params)

    # amazon_product_technical_details 写入数据库中
    def _amazon_product_technical_details_insert(self, tx, item):
        # print item['name']
        # dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = "insert into amazon_product_technical_details(zone, asin, ref_id, key_name, value_data) " \
              "values(%s,%s,%s,%s,%s)"
        params = (
            item["zone"], item["asin"], item["ref_id"], item["key_name"], item["value_data"]
        )
        print("insert data to amazon_product_technical_details ......%s") % item["asin"]
        logging.info("insert data to amazon_product_technical_details ......")
        try:
            tx.execute(sql, params)
        except Exception as e:
            print(e)


    # amazon_product_baseinfo 写入数据库中
    def _amazon_product_baseinfo_insert(self, tx, item):
        # print item['name']
        # dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = "insert into amazon_product_baseinfo(zone, asin, ref_id, seller_name, seller_url, brand, brand_url, " \
              "is_fba, stock_situation, category_name, original_price, in_sale_price, review_cnt, review_avg_star, " \
              "percent_5_star, percent_4_star, percent_3_star, percent_2_star, percent_1_star, cnt_qa, offers_url, lowest_price) " \
              "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        params = (
            item["zone"], item["asin"], item["ref_id"], item["seller_name"], item["seller_url"], item["brand"], item["brand_url"],
            item["is_fba"], item["stock_situation"], item["category_name"], item["original_price"], item["in_sale_price"], item["review_cnt"], item["review_avg_star"],
            item["percent_5_star"], item["percent_4_star"], item["percent_3_star"], item["percent_2_star"], item["percent_1_star"], item["cnt_qa"], item["offers_url"], item["lowest_price"]
            )
        print("insert data to amazon_product_baseinfo ......%s") % item["asin"]
        logging.info("insert data to amazon_product_baseinfo ......")
        tx.execute(sql, params)

    # 错误处理方法
    def _handle_error(self, failue, item, spider):
        print '--------------database operation exception!!-----------------'
        print '-------------------------------------------------------------'
        print failue
        print '-------------------------------------------------------------'


class ProductReviewPipeline(object):
    '''保存到数据库中对应的class
       1、在settings.py文件中配置
       2、在自己实现的爬虫类中yield item,会自动执行'''

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
            charset='utf8mb4',  # 编码要加上，否则可能出现中文乱码问题
            cursorclass=pymysql.cursors.DictCursor,
            use_unicode=False,
        )
        dbpool = adbapi.ConnectionPool('pymysql', **dbparams)  # **表示将字典扩展为关键字参数,相当于host=xxx,db=yyy....
        logging.info("connect mysql db ......")
        return cls(dbpool)  # 相当于dbpool付给了这个类，self中可以得到

    # pipeline默认调用
    def process_item(self, item, spider):
        if isinstance(item, amazon_product_reviews):
            query = self.dbpool.runInteraction(self._amazon_product_reviews_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        elif isinstance(item, amazon_product_review_percent_info):
            query = self.dbpool.runInteraction(self._amazon_product_review_percent_info_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        elif isinstance(item, amazon_product_review_result):
            query = self.dbpool.runInteraction(self._amazon_product_review_result_update, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        else:
            pass
        return item

    # amazon_product_reviews 写入数据库中
    def _amazon_product_review_result_update(self, tx, item):
        # print item['name']
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = "update amazon_ref_product_list set crawl_status = %s and update_date = %s" \
              " where zone = %s and asin = %s and status = 1 "
        params = (item["result_flag"], dt, item["zone"], item["asin"])
        print("update amazon_ref_product_list ......%s 's crawl status") % item["asin"]
        logging.info("update amazon_ref_product_list ......%s 's crawl status" % item["asin"])
        tx.execute(sql, params)

    # amazon_product_reviews 写入数据库中
    def _amazon_product_reviews_insert(self, tx, item):
        # print item['name']
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = "insert into amazon_product_reviews(zone, asin, ref_id, review_id, review_url, review_title, review_text, reviewer_name, reviewer_url, review_date, item_package_quantity, " \
              "item_color_size_info, order_index, review_star," \
        "is_verified_purchase, votes, comments, top_reviewer_info, cnt_imgs, cnt_vedios, create_date) " \
              "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        # print('test' * 18)
        # print(item["review_star"])
        params = (item["zone"], item["asin"], item["ref_id"], item["review_id"], item["review_url"], item["review_title"],
                  item["review_text"], item["reviewer_name"], item["reviewer_url"], item["review_date"], item["item_package_quantity"],
                  item["item_color_size_info"], item["order_index"], item["review_star"], item["is_verified_purchase"], item["votes"],
                  item["comments"], item["top_reviewer_info"], item["cnt_imgs"], item["cnt_vedios"]
                  , dt)
        print("insert data to amazon_product_reviews ......%s") % item["asin"]
        logging.info("insert data to amazon_product_reviews ......")
        tx.execute(sql, params)
        print("insert data into amazon_product_reviews success ......")

    # amazon_product_review_percent_info 写入数据库中
    def _amazon_product_review_percent_info_insert(self, tx, item):
        # print item['name']
        # dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = "insert into amazon_product_review_percent_info(zone, asin, ref_id, review_avg_star, percent_5_star, percent_4_star, percent_3_star, percent_2_star, percent_1_star) " \
              "values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        params = (
            item["zone"], item["asin"], item["ref_id"], item["review_avg_star"],
            item["percent_5_star"], item["percent_4_star"], item["percent_3_star"], item["percent_2_star"],item["percent_1_star"]
            )
        print("insert data to amazon_product_review_percent_info ......%s") % item["asin"]
        logging.info("insert data to amazon_product_review_percent_info ......")
        tx.execute(sql, params)

    # 错误处理方法
    def _handle_error(self, failue, item, spider):
        print '--------------database operation exception!!-----------------'
        print '-------------------------------------------------------------'
        print failue
        print '-------------------------------------------------------------'


class OffersCatchPipeline(FilesPipeline):

    def get_media_requests(self, item, info):
        file_url = item['offer_url']
        file_name = re.findall(r"offer-listing/(.+?)/ref=", str(file_url))
        zone = re.findall(r"amazon\.(.+?)/gp/", str(file_url))
        yield scrapy.Request(file_url, meta={'file_name': file_name, 'zone': zone})

    def file_path(self, request, response=None, info=None):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        zone = request.meta['zone'][0]
        path_date = now.split(' ')[0]
        path_time = now.split(' ')[1]
        asin = request.meta['file_name'][0]

        file_path = zone + '#' + path_date + '#' + asin + '#' + path_time + '.html'

        # print(file_path)
        return file_path

    def item_completed(self, results, item, info):
        file_paths = [x['path'] for ok, x in results if ok]
        if not file_paths:
            raise DropItem("Item contains no files")
        item['file_paths'] = file_paths
        return item


class FeedbackPipeline():
    '''保存到数据库中对应的class
       1、在settings.py文件中配置
       2、在自己实现的爬虫类中yield item,会自动执行'''

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

    # pipeline默认调用
    def process_item(self, item, spider):
        if isinstance(item, FeedbackItem):
            query = self.dbpool.runInteraction(self._feedback_insert, item)  # 调用插入的方法
            query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        else:
            pass
        return item

    # feedback 写入数据库中
    def _feedback_insert(self, tx, item):
        # print item['name']
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql_cmd = r'select * from feedback where date="%s" and zone="%s" and shop_name="%s"'%(item["date"],
                                                                         item["zone"], item["shop_name"])
        tx.execute(sql_cmd)
        if not tx.fetchall():

            sql = r'insert into feedback (date,zone,shop_name,last_30_days,last_90_days,' \
              r'last_12_months,lifetime,create_time,update_time) values("%s","%s","%s",%d,%d,%d,%d,"%s","%s");'%(item["date"],
                    item["zone"], item["shop_name"], item["last_30_days"], item["last_90_days"], item["last_12_months"],
                  item["lifetime"], dt, dt)
            print("insert data to monitor_feedback ......%s"%item["shop_name"])
            logging.info("insert data to monitor_feedback ......")
            tx.execute(sql)
            print("insert data into mLonitor_feedback success ......")
        else:
            print("There is data in the table ......")

    # 错误处理方法
    def _handle_error(self, failue, item, spider):
        print ('--------------database operation exception!!-----------------')
        print ('-------------------------------------------------------------')
        print (failue)
        print ('------------------------------------------------------------')

