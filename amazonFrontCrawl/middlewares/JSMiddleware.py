# --*-- coding:utf-8 --*--

from selenium import webdriver
from scrapy.http import HtmlResponse
import time
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import random


js = """
function scrollToBottom() {

    var Height = document.body.clientHeight,  //文本高度
        screenHeight = window.innerHeight,  //屏幕高度
        INTERVAL = 100,  // 滚动动作之间的间隔时间
        delta = 500,  //每次滚动距离
        curScrollTop = 0;    //当前window.scrollTop 值

    var scroll = function () {
        curScrollTop = document.body.scrollTop;
        window.scrollTo(0,curScrollTop + delta);
    };

    var timer = setInterval(function () {
        var curHeight = curScrollTop + screenHeight;
        if (curHeight >= Height){   //滚动到页面底部时，结束滚动
            clearInterval(timer);
        }
        scroll();
    }, INTERVAL)
}
"""

class PhantomJSMiddleware(object):

    @classmethod
    def process_request(cls, request, spider):

        if request.meta.has_key('PhantomJS'):
            print("PhantomJS")
            # driver = webdriver.PhantomJS()
            # using in crontab, must add "executable_path"
            # dcap = dict(DesiredCapabilities.PHANTOMJS)
            # dcap["phantomjs.page.settings.loadImages"] = False
            # driver = webdriver.PhantomJS(executable_path='/usr/local/bin/phantomjs', desired_capabilities=dcap)

            # driver = webdriver.PhantomJS(executable_path='/usr/local/bin/phantomjs')
            driver = webdriver.Firefox()

            driver.maximize_window()
            driver.get(request.url)
            time.sleep(random.randint(8, 15))
            # driver.execute_script(js)

            print("页面渲染中····开始自动下拉页面")
            indexPage = 1000
            pageSize = driver.execute_script("return document.body.offsetHeight")
            print('*' * 120)
            print(pageSize)
            print('*' * 120)
            # time.sleep(10)

            # while indexPage < driver.execute_script("return document.body.offsetHeight"):
            #     driver.execute_script("scroll(0," + str(indexPage) + ")")
            #     indexPage = indexPage + 1000
            #     print(indexPage)
            #     time.sleep(10)

            # js = "var q=document.documentElement.scrollTop=%s" % int(round(pageSize/1000)*1000)
            js = "var q=document.documentElement.scrollTop=10000"

            driver.execute_script(js)
            # time.sleep(random.randint(8, 15))  # 等待JS执行
            # print('waiting for 10 s')
            content = driver.page_source.encode('utf-8')
            print(content)
            driver.quit()
            return HtmlResponse(request.url, encoding='utf-8', body=content, request=request)
