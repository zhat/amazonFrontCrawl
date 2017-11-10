import scrapy
import logging
from scrapy.selector import Selector
from scrapy.http import Request
import MySQLdb
from amazonFrontCrawl import settings
import re
from tld import get_tld
from amazonFrontCrawl.items import amazon_product_reviews
from datetime import datetime

class ProductReviewSpider(scrapy.Spider):
    name = 'ProductReviewSpider'
    custom_settings = {
        'ITEM_PIPELINES': {'amazonFrontCrawl.pipelines.ProductReviewPipeline': 300}
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
            'SELECT distinct url,asin,ref_id FROM '+settings.AMAZON_REF_PRODUCT_LIST+' where STATUS = "1" ;'
        )

        rows = cursor.fetchall()
        for row in rows:
            review_url = self.product_url_to_review_url(row[0])
            yield Request(review_url, callback=self.parse, meta={'ref_id': row[2]})
        conn.close()

    def parse(self, response):
        logging.info("ProductReviewSpider parse start .....")

        se = Selector(response)
        url = response.request.url

        ref_id = response.meta['ref_id']

        domain = get_tld(response.request.url)
        zone = self.domain_to_zone(domain)
        asin_list = re.findall(r"product-reviews/(.+?)/ref=cm_cr_arp_d", str(response.request.url))
        asin = asin_list[0]

        review_list = se.xpath("//*[@id='cm_cr-review_list']//div[@data-hook='review']")
        
        for review in review_list:

            review_item = amazon_product_reviews()

            review_id = review.xpath("./@id").extract()[0].encode('utf-8')

            review_item["zone"] = zone
            review_item["asin"] = asin

            review_item["review_id"] = review_id

            review_item["review_url"] = review.xpath(".//a[@data-hook='review-title']/@href").extract()[0].encode('utf-8')

            review_item["review_star"] = review.xpath(".//i[@data-hook='review-star-rating']/span/text()").extract()[0].encode('utf-8')

            review_item["review_title"] = review.xpath(".//a[@data-hook='review-title']/text()").extract()[0].encode('utf-8')
            print(review_item["review_title"])
            review_item["reviewer_name"] = review.xpath(".//a[@data-hook='review-author']/text()").extract()[0].encode('utf-8')

            review_item["reviewer_url"] = review.xpath(".//a[@data-hook='review-author']/@href").extract()[0].encode('utf-8')

            review_date_str = review.xpath(".//span[@data-hook='review-date']/text()").extract()[0].encode('utf-8').replace('on ', '')
            if zone.upper() == 'US':
                review_item["review_date"] = datetime.strftime(datetime.strptime(review_date_str, '%B %d, %Y'), '%Y-%m-%d')
            else:
                review_item["review_date"] = review_date_str

            review_item["ref_id"] = ref_id
            review_item["order_index"] = 0

            review_text = ''
            review_text_list = review.xpath(".//span[@data-hook='review-body']/text()").extract()
            for review_temp in review_text_list:
                review_text += review_temp.encode('utf-8')

            review_item["review_text"] = review_text
            print(review_text)
            review_item['is_verified_purchase'] = 'N'
            try:
                if len(review.xpath(".//span[@data-hook='avp-badge']/text()")) > 0:
                    review_item['is_verified_purchase'] = 'Y'
            except Exception as e:
                pass

            review_item['item_package_quantity'] = -1
            try:
                review_item['item_package_quantity'] = -1
            except Exception as e:
                pass

            review_item['top_reviewer_info'] = ''
            try:
                review_item['top_reviewer_info'] = review.xpath(
                    ".//span[@class='a-size-mini a-color-link c7yBadgeAUI c7yTopDownDashedStrike c7y-badge-text a-text-bold']/text()").extract()[
                    0].encode('utf-8')
            except Exception as e:
                pass

            review_item["comments"] = ''
            try:
                comments_ = review.xpath(".//span[@class='review-comment-total aok-hidden']/text()")
                if len(comments_) > 0:
                    review_item["comments"] = comments_.extract()[0].encode('utf-8')
            except Exception as e:
                pass

            review_item["votes"] = ''
            try:
                votes_ = review.xpath(".//span[@data-hook='helpful-vote-statement']/text()")
                if(len(votes_)) > 0:
                    votes_str = votes_.extract()[0].encode('utf-8')
                review_item["votes"] = re.sub("\D", "", votes_str)
            except Exception as e:
                pass

            review_item["item_color_size_info"] = ''
            try:
                item_color_size_info_ = review.xpath(".//span[@data-hook='format-strip-linkless']")
                if len(item_color_size_info_) > 0:
                    review_item["item_color_size_info"] = item_color_size_info_.extract()[0].encode('utf-8')
            except Exception as e:
                pass

            review_item["cnt_imgs"] = 0
            try:
                images_list = review.xpath(".//img[@alt='review image']")
                cnt_imgs = len(images_list)
                review_item["cnt_imgs"] = cnt_imgs
            except Exception as e:
                pass

            review_item["cnt_vedios"] = 0
            try:
                video_id = 'videl-block-' + review_id
                video_path = './/div[@id="%s"]' % video_id
                video_list = review.xpath(video_path)
                cnt_vedios = len(video_list)
                review_item["cnt_vedios"] = cnt_vedios
            except Exception as e:
                pass

            yield review_item

        # get next page link
        pagn_next_link_yes = se.xpath("//div[@id='cm_cr-pagination_bar']//li[@class='a-last']/a/@href")

        if len(pagn_next_link_yes) > 0:
            pagn_next_link = pagn_next_link_yes.extract()[0].encode('utf-8')
            pagn_next_link_str = self.zone_to_domain(zone) + '/product-reviews/' +pagn_next_link.split('/product-reviews/')[1]
            yield Request(pagn_next_link_str, callback=self.parse, meta={'ref_id': ref_id})


    def zone_to_domain(self, zone):
        switcher = {
            'US': 'https://www.amazon.com',
            'UK': 'https://www.amazon.com.uk',
            'DE': 'https://www.amazon.de',
            'JP': 'https://www.amazon.jp',
            'CA': 'https://www.amazon.ca',
            'ES': 'https://www.amazon.es',
            'IT': 'https://www.amazon.it',
            'FR': 'https://www.amazon.fr',
        }
        return switcher.get(zone, 'error zone')


    def domain_to_zone(self, domain):
        switcher = {
            'amazon.com': 'US',
            'com.uk': 'UK',
            'amazon.de': 'DE',
            'amazon.jp': 'JP',
            'amazon.ca': 'CA',
            'amazon.es': 'ES',
            'amazon.it': 'IT',
            'amazon.fr': 'FR',

        }
        return switcher.get(domain, 'error domain')

    def product_url_to_review_url(self, product_url, type='Top'):
        # asin = str(product_url).replace('http://www.amazon.com/dp/', '')
        asin = str(product_url).split('/')[-1]

        domain = get_tld(product_url)
        zone = self.domain_to_zone(domain)
        domain_str = self.zone_to_domain(zone)
        if type == 'Top':
            review_url = '%s/product-reviews/%s/ref=cm_cr_arp_d_viewopt_rvwer?ie=UTF8&reviewerType=all_reviews&pageNumber=1' % (domain_str, asin)
        else:
            review_url = '%s/product-reviews/%s/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&pageNumber=1&sortBy=recent' % (domain_str, asin)

        return review_url