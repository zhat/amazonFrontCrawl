# -*- coding: utf-8 -*-
import scrapy
import logging
from scrapy.selector import Selector
from amazonFrontCrawl.items import amazon_top_reviewers
from scrapy.http import HtmlResponse,Request
import MySQLdb
from amazonFrontCrawl import settings
import time

class AmazonTopReviewerSpider(scrapy.Spider):
    name = "AmazonTopReviewer"
    # zone = 'us'
    # allowed_domains = ["www.amazon.com","www.amazon.com"]
    # start_urls = ["https://www.amazon.com/s/ref=sr_pg_1?me=AAF37WJS3P6BT&rh=p_4%3ALighting+EVER&ie=UTF8&qid=1493190597"]
    # start_urls = []

    # custom settings , will overwrite the setting in settings.py
    custom_settings = {
        'ITEM_PIPELINES': {'amazonFrontCrawl.pipelines.AmazonTopReviewerPipeline': 300}
    }

    def __init__(self, zone=None, *args, **kwargs):
        super(AmazonTopReviewerSpider, self).__init__(*args, **kwargs)
        self.zone = zone

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
            'SELECT distinct top_reviewer_url as url FROM '+settings.AMAZON_REF_TOP_REVIEWER_LIST+' where zone = "'+self.zone+'";'
        )

        rows = cursor.fetchall()
        time_id = int(time.time())
        for row in rows:
            # yield self.make_requests_from_url(row[0])
            yield Request(row[0], callback=self.parse, meta={'time_id': time_id})
        conn.close()


    # def start_requests(self):
    #     conn = MySQLdb.connect(
    #         user=settings['MYSQL_USER'],
    #         passwd=settings['MYSQL_PASSWD'],
    #         db=settings['MYSQL_DBNAME'],
    #         host=settings['MYSQL_HOST'],
    #         charset="utf8",
    #         use_unicode=True
    #     )
    #     cursor = conn.cursor()
    #     cursor.execute(
    #         'SELECT distinct url FROM amazon_start_urls where spider_name = '+self.name+';'
    #     )
    #
    #     rows = cursor.fetchall()
    #
    #     for row in rows:
    #         self.start_urls.append(row[0])
    #     conn.close()

    def parse(self, response):
        logging.info("AmazonTopReviewerSpider parse start .....")

        time_id = response.meta['time_id']

        se = Selector(response)

        # get all reviewers
        reviewers = se.xpath("//tr[contains(@id, 'reviewer')]")

        for i in range(len(reviewers)):
            reviewer = reviewers[i]

            reviewer_id = reviewer.xpath("./@id").extract()[0].encode('utf-8')

            zone = self.zone

            ind = int(reviewer_id.replace('reviewer','').replace(',',''))

            reviewer_name = reviewer.xpath("//*[@id='"+reviewer_id+"']/td[3]/a/b/text()").extract()[0].encode('utf-8')

            reviewer_url = reviewer.xpath("//*[@id='"+reviewer_id+"']/td[3]/a/@href").extract()[0].encode('utf-8')

            total_reviews_str = reviewer.xpath("//*[@id='"+reviewer_id+"']/td[4]/text()").extract()[0].encode('utf-8').replace(' ', '')
            total_reviews = int(filter(str.isdigit, total_reviews_str))

            helpful_votes_str = reviewer.xpath("//*[@id='"+reviewer_id+"']/td[5]/text()").extract()[0].encode('utf-8').replace(' ', '')
            helpful_votes = int(filter(str.isdigit, helpful_votes_str))

            percent_helpful = reviewer.xpath("//*[@id='"+reviewer_id+"']/td[6]/text()").extract()[0].encode('utf-8').replace(' ', '')

            page_index = 1
            url = response.request._get_url()
            if 'page=' not in url:
                page_index = 1
            else:
                page_index = int(url.split('page=')[-1])


            item = amazon_top_reviewers()
            item["zone"] = zone
            item["ind"] = ind
            item["reviewer_name"] = reviewer_name
            item["reviewer_url"] = reviewer_url
            item["total_reviews"] = total_reviews

            item["helpful_votes"] = helpful_votes
            item["percent_helpful"] = percent_helpful
            item["page_index"] = page_index

            item["time_id"] = time_id
            yield item


        # get next page link
        pagn_next_link = reviewer.xpath("//table[@class='CMpaginateBar']")[0].xpath(".//a/@href")[-1].extract().encode('utf-8')
        if pagn_next_link is not None and len(pagn_next_link) > 0:
            yield Request(pagn_next_link, callback=self.parse, meta={'time_id': time_id})
