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
from amazonFrontCrawl.items import TodayDealItem
from datetime import datetime

class TodayDealsSpider(scrapy.Spider):
    name = 'TodayDealsSpider'
    # allowed_domains = ["www.amazon.com", "www.amazon.com"]

    # custom settings , will overwrite the setting in settings.py
    custom_settings = {
        'ITEM_PIPELINES': {'amazonFrontCrawl.pipelines.TodayDealsPipeline': 300},
        'CONCURRENT_REQUESTS':1,
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
            'SELECT distinct url FROM ' + settings.AMAZON_REF_TODAY_DEALS + ' where STATUS = "2" limit 1;'
        )

        rows = cursor.fetchall()
        for row in rows:
            today_deals_url = row[0]
            print(today_deals_url)
            # yield SplashRequest(today_deals_url, callback=self.parse, args={'wait': 0.5, 'html': 1,})
            request = self.make_requests_from_url(today_deals_url)
            request.meta['PhantomJS'] = True
            yield request
        conn.close()

    def parse(self, response):
        logging.info("TodayDealsSpider parse start .....")
        se = Selector(response)
        url = response.request.url
        all_summary = se.xpath('//*[@id="FilterItemView_all_summary"]/div/span[1]/text()').extract()
        #print(all_summary)
        pages = 1
        per_page = 48
        if all_summary:
            nums = re.findall(r'Showing 1-(\d{1,2})\s*of\s*(\d+)\s*results\s*for',all_summary[0].encode('utf-8'))
            print(nums)
            per_page, total = nums[0]
            pages = int(math.ceil(float(total) / float(per_page)))
            print("pages:", pages)
        # zone
        zone = StringUtilTool.getZoneFromUrl(url)

        today_date = datetime.now().strftime("%Y-%m-%d")
        deals_item = self._get_deals_item(se, zone, 1, today_date)
        yield deals_item

        page = re.findall(r"_page_(\d+)",url)[0]
        page = int(page)
        next_page = page+1
        next_url = url.replace("_page_{}".format(page), "_page_{}".format(next_page))
        next_url = next_url.replace(",page:{},".format(page), ",page:{},".format(next_page))
        print(next_url)
        request = Request(url=next_url, callback=self.parse_today_deals,meta={'page':next_page,'pages':pages}, dont_filter=True)
        request.meta['PhantomJS'] = True
        yield request


    def parse_today_deals(self, response):
        logging.info("TodayDealsSpider parse start .....")

        se = Selector(response)
        url = response.request.url
        page = response.meta['page']
        pages = response.meta['pages']
        page = int(page)

        # zone
        zone = StringUtilTool.getZoneFromUrl(url)

        today_date = datetime.now().strftime("%Y-%m-%d")
        deals_item = self._get_deals_item(se,zone,page,today_date)
        yield deals_item

        next_page = int(page) + 1
        if next_page <= pages:
            next_url = url.replace("_page_{}".format(page), "_page_{}".format(next_page))
            next_url = next_url.replace(",page:{},".format(page), ",page:{},".format(next_page))
            print(next_url)
            request = Request(url=next_url, callback=self.parse_today_deals, meta={'page': next_page, 'pages': pages},
                          dont_filter=True)
            request.meta['PhantomJS'] = True
            yield request


    def _get_deals_item(self,se,zone,page,date):
        deal_lst = se.xpath("//div[contains(@id, '100_dealView_')]")
        print("deal_lst:", len(deal_lst))
        item_list = []
        for deal in deal_lst:
            if not deal.xpath("./*"):
                break
            id_str = deal.xpath("./@id").extract()[0].encode('utf-8')
            index = re.findall(r'100_dealView_(\d{1,2})', id_str)
            page_index = index[0]
            # 四种状态Deal of the Day  Lightning Deals  Savings & Sales  Coupons
            deal_type = ""
            # DEAL OF THE DAY 为当日交易
            deal_of_the_day_xpath = './/div/div[2]/div/div/div[1]/div/span[1]/text()'  # 当日交易
            deal_of_the_day = deal.xpath(deal_of_the_day_xpath).extract()
            if deal_of_the_day:
                if deal_of_the_day[0].encode('utf-8') == "DEAL OF THE DAY":
                    deal_type = "Deal of the Day"
            print(deal_of_the_day, len(deal_of_the_day), bool(deal_of_the_day))

            # 如果有倒计时 且没有 DEAL OF THE DAY，为限时交易
            # if deal_clock and not deal_of_the_day  #闪电交易  限时秒杀
            deal_clock = deal.xpath('.//span[@id="{}_dealClock"]'.format(id_str))  # 倒计时
            if deal_clock and not deal_type:
                deal_type = "Lightning Deals"  # 限时秒杀

            # 抢优惠券  优惠券   OFF  Clip coupon  按钮
            off_xpath = './div/div[2]/div/div/div[2]/div/span'
            off = deal.xpath('./div/div[2]/div/div/div[2]/div/span').extract()
            if off and "Off" in off[0]:
                deal_type = "Coupons"  # 优惠券

            if not deal_type:
                deal_type = "Savings & Sales"
            # button显示 文字
            # button = deal.xpath('.//div[contains(@class,"stackToBottom")]/div/span/span/span//text()').extract()
            # button_text = ""
            # if button:
            #    button_text = button[0].encode('utf-8').strip()
            #    print(button,button_text)

            # if button_text == "See details":
            #    '//*[@id="100_dealView_13"]/div/div[2]/div/div/div[5]/div/a'

            # 得到asin
            asin = ''
            if deal.xpath(".//a[@aria-labelledby='totalReviews']/@href"):
                asin_str = deal.xpath(".//a[@aria-labelledby='totalReviews']/@href").extract()[0].encode('utf-8')
                # print(asin_str)
                asin = re.findall(r"product-reviews/(.+?)/ref=", asin_str)[0]

            deal_url = ""
            if deal.xpath('.//*[@id="dealImage"]/@href').extract():
                deal_url = deal.xpath('.//*[@id="dealImage"]/@href').extract()[0]

            item = {'date': date, 'asin': asin, 'page': page, 'page_index': page_index,
                    'deal_type': deal_type, 'zone': zone, 'deal_url': deal_url}
            print(item)
            item_list.append(item)
        deals_item = TodayDealItem(deals=item_list)

        return deals_item