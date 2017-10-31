# -*- coding: utf-8 -*-
import scrapy
import re
import logging
from scrapy.selector import Selector
from amazonFrontCrawl.items import FeedbackItem
from scrapy.http import HtmlResponse,Request
import pymysql
from amazonFrontCrawl import settings
import time
from datetime import datetime,timedelta

class GetshopproductsSpider(scrapy.Spider):
    name = "feedback"
    #zone = 'us'

    # custom settings , will overwrite the setting in settings.py
    custom_settings = {
        'ITEM_PIPELINES': {'amazonFrontCrawl.pipelines.FeedbackPipeline': 300}
    }

    def start_requests(self):
        user = settings.MYSQL_USER
        passwd = settings.MYSQL_PASSWD
        db = settings.MYSQL_DBNAME
        host = settings.MYSQL_HOST
        conn = pymysql.connect(
            user=user,
            passwd=passwd,
            db=db,
            host=host,
            charset="utf8",
            use_unicode=True
        )
        cursor = conn.cursor()
        cursor.execute(
            'SELECT distinct shop_url as url,shop_name,zone FROM '+settings.AMAZON_REF_SHOP_LIST+' where type = "feedback";'
        )

        rows = cursor.fetchall()
        time_id = int(time.time())
        #print(rows)
        for row in rows:
            # yield self.make_requests_from_url(row[0])
            yield Request(row[0], callback=self.parse, meta={'time_id': time_id,'shop_name':row[1],'zone':row[2]})
        conn.close()

    def parse(self, response):
        logging.info("GetshopproductsSpider parse start .....")

        time_id = response.meta['time_id']
        shop_name = response.meta['shop_name']
        zone = response.meta['zone']
        se = Selector(response)
        print(response)
        #print(se)
        # get all products

        feedback_table = se.xpath('//*[@id="feedback-summary-table"]//tr[5]//text()')
        feedback_data=[]
        for td in feedback_table:
            td_text = td.extract()
            td_text = re.sub("\D", "", td_text)
            if td_text:
                feedback_data.append(int(td_text))
        item = FeedbackItem()
        now = datetime.now()
        item['date'] = now.strftime("%Y-%m-%d")
        item['zone'] = zone
        item['shop_name'] = shop_name
        item['last_30_days'],item['last_90_days'],item['last_12_months'],item['lifetime'] = feedback_data
        item['create_time'] = time_id
        item['update_time'] = time_id
        yield item