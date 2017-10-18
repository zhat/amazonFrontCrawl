# -*- coding: utf-8 -*-  
from scrapy import Request
from scrapy.spiders import Spider
from scrapy_splash import SplashRequest
from scrapy_splash import SplashMiddleware
from scrapy.http import Request, HtmlResponse
from scrapy.selector import Selector


class SplashSpider(Spider):
    name = 'scrapy_splash'
    # main address since it has the fun list of the products  
    start_urls = [
        'https://item.jd.com/2600240.html'
    ]

    # allowed_domains = [  
    #     'sogou.com'  
    # ]  

    # def __init__(self, *args, **kwargs):  
    #      super(WeiXinSpider, self).__init__(*args, **kwargs)  

    # request需要封装成SplashRequest
    def start_requests(self):
        # text/html; charset=utf-8  
        for url in self.start_urls:
            yield SplashRequest(url
                                , self.parse
                                , args={'wait': '0.5'}
                                # ,endpoint='render.json'  
                                )
        pass

    def parse(self, response):
        print "############" + response._url

        fo = open("html.txt", "wb")
        fo.write(response.body);  # 写入文件  
        fo.close();
        # 本文只抓取一个京东链接，此链接为京东商品页面，价格参数是ajax生成的。会把页面渲染后的html存在html.txt
        # 如果想一直抓取可以使用CrawlSpider，或者把下面的注释去掉
        '''''site = Selector(response) 
        links = site.xpath('//a/@href') 
        for link in links: 
            linkstr=link.extract() 
            print "*****"+linkstr 
            yield SplashRequest(linkstr, callback=self.parse)'''  