# -*-coding:utf-8-*-

from selenium import webdriver
from scrapy.http import HtmlResponse
import time
import logging
from amazonFrontCrawl.middlewares import downloader

class JavaScriptMiddleware(object):

    def process_request(self, request, spider):
        if spider.name == "dynamicDataParse":
            logging.info("PhantomJS is starting...")

            url = str(request.url)

            dl = downloader.CustomDownloader()

            content = dl.VisitPage(url)

            return HtmlResponse(url, status=200, body=content, encoding='utf-8', request=request)
        else:
            return

    def __del__(self):
        self.driver.quit()
        # self.driver.service.process.send_signal(signal.SIGTERM)

