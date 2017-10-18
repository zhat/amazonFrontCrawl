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
from scrapy_splash import SplashRequest


class TodayDealsSpider(scrapy.Spider):
    name = 'TodayDealsSpider'
    # allowed_domains = ["www.amazon.com", "www.amazon.com"]

    # custom settings , will overwrite the setting in settings.py
    custom_settings = {
        'ITEM_PIPELINES': {'amazonFrontCrawl.pipelines.TodayDealsPipeline': 300}
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
            'SELECT distinct url FROM ' + settings.AMAZON_REF_TODAY_DEALS + ' where STATUS = "1" limit 1;'
        )

        rows = cursor.fetchall()
        for row in rows:
            today_deals_url = row[0]
            print(today_deals_url)
            # yield SplashRequest(today_deals_url, callback=self.parse, args={'wait': 0.5, 'html': 1,})
            yield self.make_requests_from_url(today_deals_url)
        conn.close()

    def parse(self, response):
        logging.info("TodayDealsSpider parse start .....")

        url = response.request.url
        request = Request(url=url, callback=self.parse_today_deals, dont_filter=True)
        request.meta['PhantomJS'] = True
        yield request


    def parse_today_deals(self, response):
        logging.info("TodayDealsSpider parse start .....")

        se = Selector(response)
        url = response.request.url

        # search page index
        page_index = self.get_page_index_from_url(url)

        # zone
        zone = StringUtilTool.getZoneFromUrl(url)

        deal_lst = se.xpath("//*[contains(@id, '100_dealView_')]")

        for deal in deal_lst:
            id_str = deal.xpath("./@id").extract()[0].encode('utf-8')
            asin = 'un1'
            if deal.xpath(".//a[@aria-labelledby='totalReviews']/@href"):
                asin_str = deal.xpath(".//a[@aria-labelledby='totalReviews']/@href").extract()[0].encode('utf-8')
                asin = re.findall(r"product-reviews/(.+?)/ref=", asin_str)[0]

            image_url = 'un2'
            if deal.xpath(".//a[@id='dealImage']/@href"):
                image_url = deal.xpath(".//a[@id='dealImage']/@href").extract()[0].encode('utf-8')

            print('{}-{}-{}'.format(id_str, asin, image_url))

        i = 0
        while i <= 30:
            t_id = 'result_' + str(i)
            search_asin_xpath = se.xpath("//*[@id='{}']".format(t_id))
            if len(search_asin_xpath) > 0:
                print('{}-{}'.format(i, search_asin_xpath.xpath("./@data-asin").extract()[0].encode('utf-8')))

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

                # brand
                # price
                # review_star_cnt
                # review_star_avg
                # Amazon's Choice Flag
                # Best Seller Flag

                # search rank index
                search_rank_index = i

                keyword_search_rank_item = amazon_keyword_search_rank()
                keyword_search_rank_item["zone"] = zone
                keyword_search_rank_item["asin"] = asin
                keyword_search_rank_item["ref_id"] = ref_id
                keyword_search_rank_item["page_index"] = page_index
                keyword_search_rank_item["sponsored_flag"] = sponsored_flag
                keyword_search_rank_item["search_rank_index"] = search_rank_index
                yield keyword_search_rank_item

            else:
                break
            i += 1

        # sponsered right part
        sponsered_right_part_xpath = se.xpath("//*[@id='desktop-rhs-carousels_click_within_right']//div[contains(@class, 'pa-ad-details')]")
        if sponsered_right_part_xpath:
            for t_i, t_xpath in enumerate(sponsered_right_part_xpath):
                t_url = t_xpath.xpath("./div/a/@href").extract()[0].encode('utf-8')
                t_asin = re.findall("%2Fdp%2F(.+)%2Fref%3D", t_url)[0]
                t_index = t_i
                t_ref_id = -1 # default value

                sponsored_right_item = amazon_keyword_search_sponsered()
                sponsored_right_item["zone"] = zone
                sponsored_right_item["asin"] = t_asin
                sponsored_right_item["ref_id"] = t_ref_id
                sponsored_right_item["pos_type"] = 'right'
                sponsored_right_item["page_index"] = page_index
                sponsored_right_item["search_rank_index"] = t_index
                yield sponsored_right_item


        # sponsered up part
        sponsored_asin_up_xpath = se.xpath("//*[@id='pdagDesktopSparkleAsinsContainer']//a/@href")
        if sponsored_asin_up_xpath:
            for t_i, t_xpath in enumerate(sponsored_asin_up_xpath):
                t_url = t_xpath.extract().encode('utf-8')
                t_asin = re.findall("/dp/(.+)\?", t_url)[0]
                t_index = t_i
                t_ref_id = -1 # default value

                sponsored_up_item = amazon_keyword_search_sponsered()
                sponsored_up_item["zone"] = zone
                sponsored_up_item["asin"] = t_asin
                sponsored_up_item["ref_id"] = t_ref_id
                sponsored_up_item["pos_type"] = 'up'
                sponsored_up_item["page_index"] = page_index
                sponsored_up_item["search_rank_index"] = t_index
                yield sponsored_up_item


        # get next page link
        pagn_next_link_yes = se.xpath("//*[@id='pagnNextLink']/@href")

        if len(pagn_next_link_yes) > 0:
            pagn_next_link = pagn_next_link_yes.extract()[0].encode('utf-8')
            pagn_next_link_str = StringUtilTool.zone_to_domain(zone) + pagn_next_link
            yield Request(pagn_next_link_str, callback=self.parse)


    def get_page_index_from_url(self, url):
        try:
            page_index = re.findall('&page=(.+)&keywords=', url.extract()[0].encode('utf-8'))[0]
            return int(page_index)
        except Exception as e:
            return 1