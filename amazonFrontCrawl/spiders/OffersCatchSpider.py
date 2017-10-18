# -*- coding: utf-8 -*-
import scrapy
import logging
from scrapy.selector import Selector
from amazonFrontCrawl.items import amazon_product_offers_catch
from scrapy.http import HtmlResponse, Request
import MySQLdb
from amazonFrontCrawl import settings
import re
from amazonFrontCrawl.tools.amazonCrawlTools import StringUtilTool
import ast
import hashlib


class OffersCatchSpider(scrapy.Spider):
    name = "OffersCatchSpider"
    # allowed_domains = ["www.amazon.com"]
    # start_urls = ['http://www.amazon.com/']

    # custom settings , will overwrite the setting in settings.py
    custom_settings = {
        'ITEM_PIPELINES': {'amazonFrontCrawl.pipelines.OffersCatchPipeline': 2}
    }

    def start_requests(self):
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
        self.cursor = self.conn.cursor()
        self.cursor.execute(
            'SELECT distinct url,asin,ref_id FROM ' + settings.AMAZON_REF_PRODUCT_LIST + ' where STATUS = "1" ;'
        )

        rows = self.cursor.fetchall()
        for row in rows:
            yield Request(row[0], callback=self.parse_product_listing, meta={'ref_id': row[2]})
        self.conn.close()

    def parse(self, response):
        logging.info("productListing parse start .....")

    def parse_product_listing(self, response):
        logging.info('---------------------start parse productListing-------------------------')

        ref_id = response.meta['ref_id']

        se = Selector(response)
        url = response.request.url

        product_offers = amazon_product_offers_catch()

        # ------- amazon_product_baseinfo -------
        zone = StringUtilTool.getZoneFromUrl(url)  # modify

        asin = url.split('/')[-1]

        offer_url = ''
        # offers_url
        try:
            if se.xpath("//*[@id='olp_feature_div']/div/span[1]/a/@href"):
                offer_url = se.xpath("//*[@id='olp_feature_div']/div/span[1]/a/@href").extract()[0].encode('utf-8')
            else:
                offer_url = 'Unknown'
        except Exception as e:
            offer_url = 'Unknown'
            logging.error("//*[@id='olp_feature_div']/div/span[1]/a/@href:" + e.message)

        if offer_url != '' and offer_url != 'Unknown':
            product_offers["offer_url"] = 'https://www.amazon.com' + offer_url

            yield product_offers
        else:
            return

