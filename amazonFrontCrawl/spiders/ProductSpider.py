# -*-coding:utf-8-*-

import codecs
import json
import sys
import time
from scrapy.crawler import CrawlerProcess
from scrapy.spiders import Spider
from scrapy.utils.project import get_project_settings
from scrapy.utils.response import get_base_url
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.wait import WebDriverWait
from amazonFrontCrawl.items import EtaoItem

from lib2to3.pgen2.tokenize import Ignore

reload(sys)

sys.setdefaultencoding('utf-8')


class ProductSpider(Spider):
    name = "product1_spider"

    allowed_domains = ["gov.cn"]

    start_urls = [
        "http://app1.sfda.gov.cn/datasearch/face3/base.jsp?tableId=120&tableName=TABLE120&title=%CA%B3%C6%B7%C9%FA%B2%FA%D0%ED%BF%C9%BB%F1%D6%A4%C6%F3%D2%B5%28SC%29&bcId=145275419693611287728573704379",
    ]

    def __init__(self):

        self.file = codecs.open("webdata" + time.strftime('%Y-%m-%d %X', time.localtime()).replace(':', '-') + ".json",
                                'w', encoding='utf-8')

        # self.driver = webdriver.Firefox()
        self.driver = webdriver.PhantomJS()

    def parse(self, response):

        print get_base_url(response)

        self.driver.get(response.url)

        # 获取二级select

        all_options = self.driver.find_element_by_id("s20").find_elements_by_tag_name("option")

        for option in all_options:
            print "Valueis: %s" % option.get_attribute("value")
            if (option.get_attribute("value") == "120,145275419693611287728573704379"):
                option.click()

        # 等3秒，让self.driver能够取到内容
        time.sleep(5)
        self.get_item()

        # 以下为取下一页的内容，直到所有页被取完为止
        while True:

            # 获取下一页的按钮点击

            tables = self.driver.find_elements_by_xpath("//div[@id='content']/table")

            pagedown = (tables[3].find_elements_by_xpath("descendant::img"))[2]

            # 首先判断按钮是否失效，失效即当前已是最后一页，直接退出

            if pagedown.get_attribute("onclick") == None:
                break
            else:
                pagedown.click()

        time.sleep(5)
        self.get_item()

        self.driver.close()


    def close(self, spider):
        self.file.close()


    def get_item(self):
        WebDriverWait(self.driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, "//div[@id='content']/table")))

        tables = self.driver.find_elements_by_xpath("//div[@id='content']/table")

        aaa = tables[1].find_elements_by_xpath("descendant::a")

        for a in aaa:
            item = EtaoItem()

            item['name'] = a.text

            contents = a.get_attribute('href').split(",")

            item['url'] = "http://app1.sfda.gov.cn/datasearch/face3/" + contents[1]

            # printa.text,contents[1]

            line = json.dumps(dict(item), ensure_ascii=False) + "\n"

            # print line
            self.file.write(line)
            # yield item



if __name__ == '__main__':
    settings = get_project_settings()

    process = CrawlerProcess(settings)

    process.crawl(ProductSpider)

    process.start()