# -*- coding: utf-8 -*-
import scrapy
import logging
from scrapy.selector import Selector
from amazonFrontCrawl.items import AmazonProductInsalesItem
from scrapy.http import HtmlResponse,Request
import MySQLdb
from amazonFrontCrawl import settings
import time

class GetshopproductsSpider(scrapy.Spider):
    name = "getShopProducts"
    zone = 'us'
    # allowed_domains = ["www.amazon.com","www.amazon.com"]
    # start_urls = ["https://www.amazon.com/s/ref=sr_pg_1?me=AAF37WJS3P6BT&rh=p_4%3ALighting+EVER&ie=UTF8&qid=1493190597"]
    # start_urls = []

    # custom settings , will overwrite the setting in settings.py
    custom_settings = {
        'ITEM_PIPELINES': {'amazonFrontCrawl.pipelines.ProductInsalesPipeline': 300}
    }

    def start_requests(self):
        user = settings.MYSQL_USER
        passwd = settings.MYSQL_PASSWD
        db = settings.MYSQL_DBNAME
        host = settings.MYSQL_HOST
        conn = MySQLdb.connect(
            user=user,
            passwd=passwd,
            db=db,
            host=host,
            charset="utf8",
            use_unicode=True
        )
        cursor = conn.cursor()
        cursor.execute(
            'SELECT distinct shop_url as url FROM '+settings.AMAZON_REF_SHOP_LIST+' where zone = "'+self.zone+'" and type = "product";'
        )

        rows = cursor.fetchall()
        time_id = int(time.time())
        for row in rows:
            # yield self.make_requests_from_url(row[0])
            yield Request(row[0], callback=self.parse, meta={'time_id': time_id})
        conn.close()


    # def start_requests(self):
    #     conn = MySQLdb.connect(
    #         user=settings['MYSQL_USER'],
    #         passwd=settings['MYSQL_PASSWD'],
    #         db=settings['MYSQL_DBNAME'],
    #         host=settings['MYSQL_HOST'],
    #         charset="utf8",
    #         use_unicode=True
    #     )
    #     cursor = conn.cursor()
    #     cursor.execute(
    #         'SELECT distinct url FROM amazon_start_urls where spider_name = '+self.name+';'
    #     )
    #
    #     rows = cursor.fetchall()
    #
    #     for row in rows:
    #         self.start_urls.append(row[0])
    #     conn.close()

    def parse(self, response):
        logging.info("GetshopproductsSpider parse start .....")

        time_id = response.meta['time_id']

        se = Selector(response)

        # get all products
        products = se.xpath("//li[contains(@id, 'result_')]")

        for i in range(len(products)):
            product = products[i]

            shop_id = 1

            zone = 'us'

            brand_str_list = product.xpath(".//span[contains(@class,'a-size-small a-color-secondary')]/text()").extract()
            brand_str = brand_str_list[0] + brand_str_list[1]
            brand = brand_str.encode('utf-8')

            asin_unicode = product.xpath("./@data-asin").extract()
            asin = asin_unicode[0].encode('utf-8')

            result_id_unicode = product.xpath("./@id").extract()
            result_id_str = result_id_unicode[0].encode('utf-8')
            order_index = int(filter(str.isdigit, result_id_str))

            item = AmazonProductInsalesItem()
            item["shop_id"] = shop_id
            item["zone"] = zone
            item["brand"] = brand
            item["asin"] = asin
            item["order_index"] = order_index
            item["time_id"] = time_id
            yield item


        # get next page link
        pagn_next_link = se.xpath("//a[@id='pagnNextLink']/@href").extract()
        if pagn_next_link is not None and len(pagn_next_link) > 0:
            pagn_next_link_str = pagn_next_link[0].encode('utf-8')
            if pagn_next_link_str.startswith("/"):
                yield Request("http://www.amazon.com" + pagn_next_link_str, callback=self.parse, meta={'time_id': time_id})
