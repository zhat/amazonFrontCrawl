# -*- coding: utf-8 -*-
import time
from scrapy.exceptions import IgnoreRequest
from scrapy.http import HtmlResponse, Response
from selenium import webdriver
import selenium.webdriver.support.ui as ui
from selenium.common.exceptions import TimeoutException
from datetime import datetime

class CustomDownloader(object):
    def __init__(self):
        # use any browser you wish
        cap = webdriver.DesiredCapabilities.PHANTOMJS
        cap["phantomjs.page.settings.resourceTimeout"] = 1000
        cap["phantomjs.page.settings.loadImages"] = False
        cap["phantomjs.page.settings.disk-cache"] = True
        # cap["phantomjs.page.customHeaders.Cookie"] = 'SINAGLOBAL=3955422793326.2764.1451802953297; '
        cap["phantomjs.page.settings.userAgent"] = "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:50.0) Gecko/20100101 Firefox/50.0"
        cap["phantomjs.page.customHeaders.User-Agent"] = 'Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:50.0) Gecko/20100101 Firefox/50.0'

        self.driver = webdriver.PhantomJS(desired_capabilities=cap)
        self.driver.set_page_load_timeout(10)
        # wait = ui.WebDriverWait(self.driver,10)

    def VisitPage(self, url):
        print('正在加载网站.....')
        self.driver.implicitly_wait(5)
        self.driver.set_page_load_timeout(5)

        time.sleep(3)

        try:
            self.driver.get(url)
        except TimeoutException as e:
            print(e)
            now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            self.driver.get_screenshot_as_file('screenshot-%s.png' % now)
        finally:
            self.driver.quit()

        time.sleep(3)

        # 翻到底，详情加载
        self.driver.set_window_size(1280, 2400)  # option, or use the next two line code
        # js = "var q=document.documentElement.scrollTop=10000"
        # self.driver.execute_script(js) # execute js, scroll to down

        time.sleep(3)
        # content = self.driver.page_source.encode('gbk', 'ignore')
        content = self.driver.page_source

        print('网页加载完毕.....')
        return content
