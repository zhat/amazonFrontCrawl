import scrapy
import logging
from scrapy.selector import Selector
from scrapy.http import HtmlResponse, Request
import MySQLdb
from amazonFrontCrawl import settings
import re
from tld import get_tld
from amazonFrontCrawl.items import amazon_keyword_search_rank, amazon_keyword_search_sponsered
from datetime import datetime
import ast
from amazonFrontCrawl.tools.amazonCrawlTools import StringUtilTool
import time


class KeywordSearchSpider(scrapy.Spider):
    name = 'KeywordSearchSpider'
    allowed_domains = ["www.amazon.com", "www.amazon.com"]

    # custom settings , will overwrite the setting in settings.py
    custom_settings = {
        'ITEM_PIPELINES': {'amazonFrontCrawl.pipelines.KeywordSearchPipeline': 300}
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
            'SELECT distinct keyword, zone FROM ' + settings.AMAZON_REF_KEYWORD_LIST + ' where STATUS = "1" limit 1;'
        )

        rows = cursor.fetchall()
        for row in rows:
            keyword_search_url = StringUtilTool.generate_keyword_search_url(row)
            print(keyword_search_url)
            yield self.make_requests_from_url(keyword_search_url)
        conn.close()

    def parse(self, response):
        logging.info("keyword search rank parse start .....")

        yield Request(response.request.url, callback=self.parse_keyword_search_rank)

    def parse_keyword_search_rank(self, response):
        logging.info("KeywordSearchSpider parse start .....")

        # time.sleep(15)

        se = Selector(response)
        url = response.request.url

        # print('*' * 100)
        # print(url)

        # search page index
        page_index = self.get_page_index_from_url(url)

        # zone
        zone = StringUtilTool.getZoneFromUrl(url)

        search_result_xpath = search_asin_xpath = se.xpath("//li[contains(@id, 'result_')]")
        if search_result_xpath:
            for search_asin_xpath in search_result_xpath:
                t_id = search_asin_xpath.xpath("./@id").extract()[0].encode('utf-8')

                # Sponsored
                sponsored_text_lst = search_asin_xpath.xpath(".//*[contains(text(), 'Sponsored')]")
                if len(sponsored_text_lst) > 0:
                    sponsored_flag = 1
                else:
                    sponsored_flag = 0

                # asin
                asin = search_asin_xpath.xpath("./@data-asin").extract()[0].encode('utf-8')

                # ref id
                ref_id = -1 # default value

                # search rank index
                search_rank_index = int(re.findall(r"\d+", t_id)[0])

                keyword_search_rank_item = amazon_keyword_search_rank()
                keyword_search_rank_item["zone"] = zone
                keyword_search_rank_item["asin"] = asin
                keyword_search_rank_item["ref_id"] = ref_id
                keyword_search_rank_item["page_index"] = page_index
                keyword_search_rank_item["sponsored_flag"] = sponsored_flag
                keyword_search_rank_item["search_rank_index"] = search_rank_index
                yield keyword_search_rank_item

        # sponsered right part
        # sponsered_right_part_xpath = se.xpath("//*[@id='desktop-rhs-carousels_click_within_right']//div[contains(@class, 'pa-ad-details')]")
        # if sponsered_right_part_xpath:
        #     for t_i, t_xpath in enumerate(sponsered_right_part_xpath):
        #         t_url = t_xpath.xpath("./div/a/@href").extract()[0].encode('utf-8')
        #         t_asin = re.findall("%2Fdp%2F(.+)%2Fref%3D", t_url)[0]
        #         t_index = t_i
        #         t_ref_id = -1 # default value
        #
        #         sponsored_right_item = amazon_keyword_search_sponsered()
        #         sponsored_right_item["zone"] = zone
        #         sponsored_right_item["asin"] = t_asin
        #         sponsored_right_item["ref_id"] = t_ref_id
        #         sponsored_right_item["pos_type"] = 'right'
        #         sponsored_right_item["page_index"] = page_index
        #         sponsored_right_item["search_rank_index"] = t_index
        #         yield sponsored_right_item


        # sponsered up part
        # sponsored_asin_up_xpath = se.xpath("//*[@id='pdagDesktopSparkleAsinsContainer']//a/@href")
        # if sponsored_asin_up_xpath:
        #     for t_i, t_xpath in enumerate(sponsored_asin_up_xpath):
        #         t_url = t_xpath.extract().encode('utf-8')
        #         t_asin = re.findall("/dp/(.+)\?", t_url)[0]
        #         t_index = t_i
        #         t_ref_id = -1 # default value
        #
        #         sponsored_up_item = amazon_keyword_search_sponsered()
        #         sponsored_up_item["zone"] = zone
        #         sponsored_up_item["asin"] = t_asin
        #         sponsored_up_item["ref_id"] = t_ref_id
        #         sponsored_up_item["pos_type"] = 'up'
        #         sponsored_up_item["page_index"] = page_index
        #         sponsored_up_item["search_rank_index"] = t_index
        #         yield sponsored_up_item


        # KEYWORD_SEARCH_PAGES_MAX
        cur_page_path = se.xpath("//*[@id='pagn']/span[@class='pagnCur']/text()")
        if cur_page_path:
            cur_page_cnt = int(cur_page_path.extract()[0].encode('utf-8'))
            if cur_page_cnt < settings.KEYWORD_SEARCH_PAGES_MAX:
                # get next page link
                pagn_next_link_yes = se.xpath("//*[@id='pagnNextLink']/@href")

                if len(pagn_next_link_yes) > 0:
                    pagn_next_link = pagn_next_link_yes.extract()[0].encode('utf-8')
                    pagn_next_link_str = StringUtilTool.zone_to_domain(zone.upper()) + pagn_next_link
                    # time.sleep(5)
                    yield Request(url=pagn_next_link_str, callback=self.parse_keyword_search_rank)


    def get_page_index_from_url(self, url):
        try:
            page_index = re.findall('&page=(.+)&keywords=', url.extract()[0].encode('utf-8'))[0]
            return int(page_index)
        except Exception as e:
            return 1