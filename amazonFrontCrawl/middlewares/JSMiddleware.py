# --*-- coding:utf-8 --*--

from selenium import webdriver
from scrapy.http import HtmlResponse
import time
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import random

class PhantomJSMiddleware(object):

    @classmethod
    def process_request(cls, request, spider):

        if request.meta.has_key('PhantomJS'):
            proxy_server = request.meta["proxy"]
            authorization = request.headers["Proxy-Authorization"]
            user_agent = request.headers['User-Agent']
            print("PhantomJS")
            print(user_agent)
            print(authorization)
            print(proxy_server)
            # phantomjs_path = r"./phantomjs"
            dcap = DesiredCapabilities.CHROME
            dcap["phantomjs.page.settings.userAgent"] = user_agent
            dcap["phantomjs.page.settings.proxy"] = proxy_server
            dcap["phantomjs.page.settings.Proxy-Authorization"] = authorization

            service_args = []
            service_args.append('--load-images=no')  ##关闭图片加载
            #service_args.append('--disk-cache=yes')  ##开启缓存
            #service_args.append('--ignore-ssl-errors=true')  ##忽略https错误

            driver = webdriver.PhantomJS(desired_capabilities=dcap, service_args=service_args)  # ,service_args=service_args
            driver.set_window_size(1920, 30000)
            #driver.maximize_window()
            driver.get(request.url)
            #driver.execute_script("window.scrollBy(0,3000)")
            #time.sleep(random.randint(3,5))
            #print(driver.current_url)
            print(driver.title)

            content = driver.page_source.encode('utf-8')
            #print(content)
            driver.quit()
            return HtmlResponse(request.url, encoding='utf-8', body=content, request=request)