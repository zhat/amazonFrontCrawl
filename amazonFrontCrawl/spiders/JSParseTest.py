# -*- coding: utf-8 -*-
import scrapy
import logging
from scrapy.selector import Selector
from scrapy.http import HtmlResponse,Request
import MySQLdb
from amazonFrontCrawl import settings
import re
from amazonFrontCrawl.items import AmazonTop100BestSellersItem, AmazonTop100GiftIdeasItem, AmazonTop100MostWishedItem, AmazonTop100HotNewReleasesItem
from lxml import etree
from scrapy.crawler import CrawlerProcess
from scrapy.spiders import Spider
from scrapy.utils.project import get_project_settings
from scrapy.utils.response import get_base_url

class JSParseTest(scrapy.Spider):
    name = "dynamicDataParse"
    allowed_domains = ["amazon.com"]
    start_urls = ["https://www.amazon.com/dp/B00HSF65EA"]
    # start_urls = ["https://item.jd.com/3133817.html"]
    # custom settings , will overwrite the setting in settings.py
    # custom_settings = {
    #     'ITEM_PIPELINES': {'amazonFrontCrawl.pipelines.Top100SeriesPipeline': 300}
    # }

    def parse(self, response):
        print("-----------------------------------Java Script Data Parse-----------------------------")
        # print(response.body)
        content = response.body
        etr = etree.HTML(content)
        purchases = etr.xpath("//*[@id='purchase-sims-feature']/div/@data-a-carousel-options")

        if 'id_list' in purchases[0]:
            print("parse dynamic data success...")
        else:
            print("parse dynamic data failed...")
        print("-----------------------------------Java Script Data Parse-----------------------------")

if __name__ == '__main__':
    settings = get_project_settings()

    process = CrawlerProcess(settings)

    process.crawl(JSParseTest)

    process.start()