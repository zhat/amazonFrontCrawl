# -*- coding: utf-8 -*-
import scrapy
import logging
from scrapy.selector import Selector
from scrapy.http import HtmlResponse,Request
import MySQLdb
from amazonFrontCrawl import settings
import re
from amazonFrontCrawl.items import AmazonTop100BestSellersItem, AmazonTop100GiftIdeasItem, AmazonTop100MostWishedItem, AmazonTop100HotNewReleasesItem

class Top100SeriesSpider(scrapy.Spider):
    name = "top100Series"
    allowed_domains = ["www.amazon.com","www.amazon.com"]
    # start_urls = ["https://www.amazon.com/s/ref=sr_pg_1?me=AAF37WJS3P6BT&rh=p_4%3ALighting+EVER&ie=UTF8&qid=1493190597"]
    # start_urls = []

    # custom settings , will overwrite the setting in settings.py
    custom_settings = {
        'ITEM_PIPELINES': {'amazonFrontCrawl.pipelines.Top100SeriesPipeline': 300}
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
            'SELECT distinct url FROM '+settings.AMAZON_REF_START_URLS+' where substr(spider_name,1,12) = "'+self.name+'";'
        )

        rows = cursor.fetchall()
        # print('***********************************')
        # print(len(rows))
        # print('***********************************')
        for row in rows:
            print(row[0])
            yield self.make_requests_from_url(row[0])
        conn.close()

    def parse(self, response):
        logging.info("Top100SeriesSpider parse start .....")

        if 'Best-Sellers' in response.request.url or 'bestsellers' in response.request.url:
            # self.parse_best_sellers(response)
            yield Request(response.request.url, callback=self.parse_best_sellers)
        elif 'new-releases' in response.request.url:
            # self.parse_new_releases(response)
            yield Request(response.request.url, callback=self.parse_new_releases)
        elif 'most-wished' in response.request.url:
            # self.parse_most_wished(response)
            yield Request(response.request.url, callback=self.parse_most_wished)
        elif 'most-gifted' in response.request.url:
            # self.parse_most_gifted(response)
            yield Request(response.request.url, callback=self.parse_most_gifted)



    def parse_best_sellers(self, response):
        logging.info('---------------------start parse Best-Sellers-------------------------')
        se = Selector(response)

        # get all products
        products = se.xpath("//*[@id='zg_centerListWrapper']//div[@class='zg_itemImmersion']")

        logging.info('---------------------get Best-Sellers products-------------------------')

        for i in range(len(products)):
            product = products[i]

            zone = 'us'

            try:
                category_name = product.xpath('//*[@id="zg_listTitle"]/span[@class="category"]/text()').extract()[
                    0].encode(
                    'utf-8')

                category_url = response.request.url

                asin_str = product.xpath(".//@data-p13n-asin-metadata").extract()[0].encode('utf-8')
                asin = re.findall(r'asin":"(.*?)"}', asin_str)[0]

                order_index_str = \
                product.xpath(".//*[@class='zg_itemWrapper']//a[@class='a-link-normal']/@href").extract()[
                    0].encode('utf-8')
                order_index = re.findall(r'zg_bs_\d*_(.*?)/', order_index_str)[0]
            except Exception as e:
                pass

            batch_number = '1'

            item = AmazonTop100BestSellersItem()
            item["zone"] = zone
            item["category_name"] = category_name
            item["category_url"] = category_url
            item["asin"] = asin
            item["order_index"] = order_index
            item["batch_number"] = batch_number
            yield item

            # get next page link
            pagn_next_link = ''
            pagn_now_link = se.xpath('//li[@class="zg_page zg_selected"]/@id').extract()[0].encode('utf-8')
            if pagn_now_link <> 'zg_page5':
                if pagn_now_link == 'zg_page1':
                    pagn_next_link = 'zg_page2'
                elif pagn_now_link == 'zg_page2':
                    pagn_next_link = 'zg_page3'
                elif pagn_now_link == 'zg_page3':
                    pagn_next_link = 'zg_page4'
                elif pagn_now_link == 'zg_page4':
                    pagn_next_link = 'zg_page5'

            if pagn_next_link <> '' and len(pagn_next_link) > 0:
                pagn_next_link_str = se.xpath('//li[@id="' + pagn_next_link + '"]/a/@href').extract()[0].encode('utf-8')
                yield Request(pagn_next_link_str, callback=self.parse_best_sellers)

    def parse_new_releases(self, response):
        logging.info('---------------------start parse new-releases-------------------------')
        se = Selector(response)

        # get all products
        products = se.xpath("//*[@id='zg_centerListWrapper']//div[@class='zg_itemImmersion']")

        for i in range(len(products)):
            product = products[i]

            zone = 'us'

            category_name = product.xpath('//*[@id="zg_listTitle"]/span[@class="category"]/text()').extract()[0].encode(
                'utf-8')

            category_url = response.request.url

            asin_str = product.xpath(".//@data-p13n-asin-metadata").extract()[0].encode('utf-8')
            asin = re.findall(r'asin":"(.*?)"}', asin_str)[0]

            order_index_str = product.xpath(".//*[@class='zg_itemWrapper']//a[@class='a-link-normal']/@href").extract()[
                0].encode('utf-8')
            order_index = re.findall(r'zg_bsnr_\d*_(.*?)/', order_index_str)[0]

            batch_number = '1'

            item = AmazonTop100HotNewReleasesItem()
            item["zone"] = zone
            item["category_name"] = category_name
            item["category_url"] = category_url
            item["asin"] = asin
            item["order_index"] = order_index
            item["batch_number"] = batch_number
            yield item

            # get next page link
            pagn_next_link = ''
            pagn_now_link = se.xpath('//li[@class="zg_page zg_selected"]/@id').extract()[0].encode('utf-8')
            if pagn_now_link <> 'zg_page5':
                if pagn_now_link == 'zg_page1':
                    pagn_next_link = 'zg_page2'
                elif pagn_now_link == 'zg_page2':
                    pagn_next_link = 'zg_page3'
                elif pagn_now_link == 'zg_page3':
                    pagn_next_link = 'zg_page4'
                elif pagn_now_link == 'zg_page4':
                    pagn_next_link = 'zg_page5'

            if pagn_next_link <> '' and len(pagn_next_link) > 0:
                pagn_next_link_str = se.xpath('//li[@id="' + pagn_next_link + '"]/a/@href').extract()[0].encode('utf-8')
                yield Request(pagn_next_link_str, callback=self.parse_new_releases)

    def parse_most_wished(self, response):
        logging.info('---------------------start parse most-wished-------------------------')
        se = Selector(response)

        # get all products
        products = se.xpath("//*[@id='zg_centerListWrapper']//div[@class='zg_itemImmersion']")

        for i in range(len(products)):
            product = products[i]

            zone = 'us'

            category_name = product.xpath('//*[@id="zg_listTitle"]/span[@class="category"]/text()').extract()[0].encode(
                'utf-8')

            category_url = response.request.url

            asin_str = product.xpath(".//@data-p13n-asin-metadata").extract()[0].encode('utf-8')
            asin = re.findall(r'asin":"(.*?)"}', asin_str)[0]

            order_index_str = product.xpath(".//*[@class='zg_itemWrapper']//a[@class='a-link-normal']/@href").extract()[
                0].encode('utf-8')
            order_index = re.findall(r'zg_mw_\d*_(.*?)/', order_index_str)[0]
            batch_number = '1'

            item = AmazonTop100MostWishedItem()
            item["zone"] = zone
            item["category_name"] = category_name
            item["category_url"] = category_url
            item["asin"] = asin
            item["order_index"] = order_index
            item["batch_number"] = batch_number
            yield item

            # get next page link
            pagn_next_link = ''
            pagn_now_link = se.xpath('//li[@class="zg_page zg_selected"]/@id').extract()[0].encode('utf-8')
            if pagn_now_link <> 'zg_page5':
                if pagn_now_link == 'zg_page1':
                    pagn_next_link = 'zg_page2'
                elif pagn_now_link == 'zg_page2':
                    pagn_next_link = 'zg_page3'
                elif pagn_now_link == 'zg_page3':
                    pagn_next_link = 'zg_page4'
                elif pagn_now_link == 'zg_page4':
                    pagn_next_link = 'zg_page5'

            if pagn_next_link <> '' and len(pagn_next_link) > 0:
                pagn_next_link_str = se.xpath('//li[@id="' + pagn_next_link + '"]/a/@href').extract()[0].encode('utf-8')
                yield Request(pagn_next_link_str, callback=self.parse_most_wished)

    def parse_most_gifted(self, response):
        logging.info('---------------------start parse most-gifted-------------------------')
        se = Selector(response)

        # get all products
        products = se.xpath("//*[@id='zg_centerListWrapper']//div[@class='zg_itemImmersion']")

        for i in range(len(products)):
            product = products[i]

            zone = 'us'

            category_name = product.xpath('//*[@id="zg_listTitle"]/span[@class="category"]/text()').extract()[0].encode(
                'utf-8')

            category_url = response.request.url

            asin_str = product.xpath(".//@data-p13n-asin-metadata").extract()[0].encode('utf-8')
            asin = re.findall(r'asin":"(.*?)"}', asin_str)[0]

            order_index_str = product.xpath(".//*[@class='zg_itemWrapper']//a[@class='a-link-normal']/@href").extract()[
                0].encode('utf-8')
            order_index = re.findall(r'zg_mg_\d*_(.*?)/', order_index_str)[0]
            batch_number = '1'

            item = AmazonTop100GiftIdeasItem()
            item["zone"] = zone
            item["category_name"] = category_name
            item["category_url"] = category_url
            item["asin"] = asin
            item["order_index"] = order_index
            item["batch_number"] = batch_number
            yield item

            # get next page link
            pagn_next_link = ''
            pagn_now_link = se.xpath('//li[@class="zg_page zg_selected"]/@id').extract()[0].encode('utf-8')
            if pagn_now_link <> 'zg_page5':
                if pagn_now_link == 'zg_page1':
                    pagn_next_link = 'zg_page2'
                elif pagn_now_link == 'zg_page2':
                    pagn_next_link = 'zg_page3'
                elif pagn_now_link == 'zg_page3':
                    pagn_next_link = 'zg_page4'
                elif pagn_now_link == 'zg_page4':
                    pagn_next_link = 'zg_page5'

            if pagn_next_link <> '' and len(pagn_next_link) > 0:
                pagn_next_link_str = se.xpath('//li[@id="' + pagn_next_link + '"]/a/@href').extract()[0].encode('utf-8')
                yield Request(pagn_next_link_str, callback=self.parse_most_gifted)

