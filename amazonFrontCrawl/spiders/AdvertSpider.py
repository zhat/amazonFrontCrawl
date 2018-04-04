# --*-- coding:utf-8 --*--
import math
import scrapy
import logging
from scrapy.selector import Selector
from scrapy.http import Request
import MySQLdb
from amazonFrontCrawl import settings
import re
from amazonFrontCrawl.tools.amazonCrawlTools import StringUtilTool
from amazonFrontCrawl.items import TodayDealItem,AdvertItem
from datetime import datetime

class AdvertSpider(scrapy.Spider):
    name = 'AdvertSpider'
    # allowed_domains = ["www.amazon.com", "www.amazon.com"]

    # custom settings , will overwrite the setting in settings.py
    custom_settings = {
        'ITEM_PIPELINES': {'amazonFrontCrawl.pipelines.AdvertPipeline': 300},
        'CONCURRENT_REQUESTS':1,
    }

    def start_requests(self):
        today_deals_url = "https://www.amazon.com/"
        print(today_deals_url)
        # yield SplashRequest(today_deals_url, callback=self.parse, args={'wait': 0.5, 'html': 1,})
        request = self.make_requests_from_url(today_deals_url)
        request.meta['Advert'] = True
        request.meta['keyword'] = "keyword"
        request.meta['page'] = 1
        request.meta['zone'] = "US"
        yield request

    def parse(self, response):
        logging.info("AdvertSpider parse start .....")
        keyword = response.meta['keyword']
        page = response.meta['page']
        zone = response.meta['zone']
        se = Selector(response)
        url = response.request.url
        now = datetime.now()
        # 侧栏
        # date keyword position page serial_num asin is_sponsored create_time update_time
        side_summary = se.xpath('//*[@id="desktop-rhs-carousels_sxwds-rbp"]/div/div')
        serial_num = 0
        for side in side_summary:
            href = side.xpath('./div[1]/a/@href').extract()
            if not href:
                continue
            print(href)
            href = href[0].encode("utf-8")
            print(href)
            asin = re.findall(r'dp/(.*)/ref=',href)
            if not asin:
                continue
            asin = asin[0]
            serial_num += 1
            item = AdvertItem()
            item['date'] = now.strftime("%Y-%m-%d")
            item['zone'] = zone
            item['keyword'] = keyword
            item['is_sponsored'] = 0
            item['page'] = page
            item['create_time'] = now
            item['update_time'] = now
            item['serial_num'] = serial_num
            item['position'] = "side"
            item['asin'] = asin
            # print(item)
            yield item

        #顶部
        # '//*[@id="pdagDesktopSparkleAsinsContainer"]/div[1]'
        top_summary = se.xpath('//*[@id="pdagDesktopSparkleAsinsContainer"]/div')
        serial_num = 0
        for top in top_summary:
            # print('top',top)
            asin = top.xpath('./div[1]/div/@data-asin').extract()
            if not asin:
                continue
            asin = asin[0].encode('utf-8')
            serial_num += 1
            item = AdvertItem()
            item['date'] = now.strftime("%Y-%m-%d")
            item['zone'] = zone
            item['keyword'] = keyword
            item['is_sponsored'] = 0
            item['page'] = page
            item['create_time'] = now
            item['update_time'] = now
            item['serial_num'] = serial_num
            item['position'] = "top"
            item['asin'] = asin
            yield item
        # date抓取日期 关键字 位置(top left text) 页码 序号 asin is_sponsored create_time update_time
        #正文
        text_summary = se.xpath("//li[contains(@id, 'result_')]")
        print(len(text_summary))
        serial_num = 0
        for text in text_summary:
            asin = text.xpath('./@data-asin').extract()
            if not asin:
                continue
            serial_num += 1
            asin = asin[0].encode('utf-8')
            sponsored = text.xpath('.//h5/text()').extract()
            print(sponsored)
            is_sponsored = 0
            if sponsored and sponsored[0]=="Sponsored":
                is_sponsored = 1
            item = AdvertItem()
            item['date'] = now.strftime("%Y-%m-%d")
            item['zone'] = zone
            item['keyword'] = keyword
            item['is_sponsored'] = is_sponsored
            item['page'] = page
            item['create_time'] = now
            item['update_time'] = now
            item['serial_num'] = serial_num
            item['position'] = "text"
            item['asin'] = asin
            print(item)
            yield item