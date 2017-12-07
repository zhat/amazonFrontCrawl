# -*- coding: utf-8 -*-
import scrapy
import logging
from scrapy.selector import Selector
from amazonFrontCrawl.items import amazon_product_baseinfo, amazon_product_technical_details, amazon_product_category_sales_rank, amazon_product_descriptions, amazon_product_pictures
from amazonFrontCrawl.items import amazon_product_bought_together_list, amazon_product_also_bought_list, amazon_product_current_reviews
from amazonFrontCrawl.items import amazon_product_promotions, amazon_traffic_sponsored_products, amazon_traffic_buy_other_after_view, amazon_traffic_similar_items
from scrapy.http import HtmlResponse,Request
import MySQLdb
from amazonFrontCrawl import settings
import re
from amazonFrontCrawl.tools.amazonCrawlTools import StringUtilTool
import ast
import hashlib
import json

class ProductlistingSpider(scrapy.Spider):
    name = "product_listing"
    allowed_domains = ["www.amazon.com"]
    # start_urls = ['http://www.amazon.com/']

    # custom settings , will overwrite the setting in settings.py
    custom_settings = {
        'ITEM_PIPELINES': {'amazonFrontCrawl.pipelines.ProductListingPipeline': 300}
    }

    def start_requests(self):
        user = settings.MYSQL_USER
        passwd = settings.MYSQL_PASSWD
        db = settings.MYSQL_DBNAME
        host = settings.MYSQL_HOST
        self.conn = MySQLdb.connect(
            user=user,
            passwd=passwd,
            db=db,
            host=host,
            charset="utf8",
            use_unicode=True
        )
        self.cursor = self.conn.cursor()

        self.cursor.execute(
            "delete from amazon_ref_product_list;"
        )

        result = self.cursor.execute(
            "insert into amazon_ref_product_list(zone, asin, url, status, crawl_status, ref_id)"
            " select zone"
            "      , asin"
            "      , case when zone = 'us' then concat('http://www.amazon.com/dp/', asin) "
            "             when zone = 'uk' then concat('http://www.amazon.com.uk/dp/', asin) "
            "             when zone = 'de' then concat('http://www.amazon.de/dp/', asin) "
            "             when zone = 'jp' then concat('http://www.amazon.jp/dp/', asin) "
            "             when zone = 'ca' then concat('http://www.amazon.ca/dp/', asin) "
            "             when zone = 'es' then concat('http://www.amazon.es/dp/', asin) "
            "             when zone = 'it' then concat('http://www.amazon.it/dp/', asin) "
            "             when zone = 'fr' then concat('http://www.amazon.fr/dp/', asin) "
            "         end as url"
            "      , 1"
            "      , 0"
            "      , id"
            "  from report_productinfo a"
            " where a.zone='US' AND a.date = (select max(date) from report_productinfo);"
        )
        print(result)
        result = self.cursor.execute(
            "insert into amazon_ref_product_list(zone, asin, url, status, crawl_status, ref_id)"
            " select zone"
            "      , competitive_product_asin"
            "      , case when zone = 'us' then concat('http://www.amazon.com/dp/', competitive_product_asin) "
            "             when zone = 'uk' then concat('http://www.amazon.com.uk/dp/', competitive_product_asin) "
            "             when zone = 'de' then concat('http://www.amazon.de/dp/', competitive_product_asin) "
            "             when zone = 'jp' then concat('http://www.amazon.jp/dp/', competitive_product_asin) "
            "             when zone = 'ca' then concat('http://www.amazon.ca/dp/', competitive_product_asin) "
            "             when zone = 'es' then concat('http://www.amazon.es/dp/', competitive_product_asin) "
            "             when zone = 'it' then concat('http://www.amazon.it/dp/', competitive_product_asin) "
            "             when zone = 'fr' then concat('http://www.amazon.fr/dp/', competitive_product_asin) "
            "         end as url"
            "      , 1"
            "      , 0"
            "      , id"
            "  from report_competitiveproduct a"
            " where a.competitive_product_asin != '' AND a.zone = 'US';"
        )
        self.conn.commit()
        print(result)
        self.cursor.execute(
            'SELECT distinct url,asin,ref_id FROM '+settings.AMAZON_REF_PRODUCT_LIST+' where STATUS = "1" ;')
        rows = self.cursor.fetchall()
        print(len(rows))
        for row in rows:
            yield Request(row[0], callback=self.parse_product_listing, meta={'ref_id': row[2]})
        self.conn.close()

    def parse(self, response):
        logging.info("productListing parse start .....")

    def parse_product_listing(self, response):
        logging.info('---------------------start parse productListing-------------------------')

        ref_id = response.meta['ref_id']
        
        se = Selector(response)
        url = response.request.url

        product_baseinfo_item = amazon_product_baseinfo()

        # ------- amazon_product_baseinfo -------
        zone = StringUtilTool.getZoneFromUrl(url)  # modify
        product_baseinfo_item["zone"] = zone

        asin = url.split('/')[-1]

        product_baseinfo_item["asin"] = asin

        product_baseinfo_item["ref_id"] = ref_id # default value

        # seller_name
        if se.xpath("//*[@id='merchant-info']/a[1]/text()"):
            seller_name = se.xpath("//*[@id='merchant-info']/a[1]/text()").extract()[0].encode('utf-8')
        else:
            seller_name = 'Unknown'
        product_baseinfo_item["seller_name"] = seller_name

        # seller_url
        if se.xpath("//*[@id='merchant-info']/a[1]/@href"):
            seller_url = se.xpath("//*[@id='merchant-info']/a[1]/@href").extract()[0].encode('utf-8')
        else:
            seller_url = 'Unknown'
        product_baseinfo_item["seller_url"] = seller_url

        # brand
        if se.xpath("//*[@id='bylineInfo']/text()"):
            brand = se.xpath("//*[@id='bylineInfo']/text()").extract()[0].encode('utf-8')
        else:
            brand = 'Unknown'
        product_baseinfo_item["brand"] = brand

        # brand_url
        if se.xpath("//*[@id='bylineInfo']/@href"):
            brand_url = se.xpath("//*[@id='bylineInfo']/@href").extract()[0].encode('utf-8')
        else:
            brand_url = 'Unknown'
        product_baseinfo_item["brand_url"] = brand_url

        # is_fba
        if se.xpath("//*[@id='merchant-info']/a[2]/text()"):
            is_fba_str = se.xpath("//*[@id='merchant-info']/a[2]/text()").extract()[0].encode('utf-8')
            is_fba = StringUtilTool.getFbaFromStr(is_fba_str)
        else:
            is_fba = 'Unknown'
        product_baseinfo_item["is_fba"] = is_fba

        # stock_situation stop here
        if se.xpath("normalize-space(//*[@id='availability']/span/text())"):
            stock_situation = se.xpath("normalize-space(//*[@id='availability']/span/text())").extract()[0].encode(
                    'utf-8')
        else:
            stock_situation = 'Unknown'
        product_baseinfo_item["stock_situation"] = stock_situation

        # category_name
        if se.xpath("//*[@id='wayfinding-breadcrumbs_feature_div']//a[@class='a-link-normal a-color-tertiary']"):
            category_name_list = se.xpath(
                    "//*[@id='wayfinding-breadcrumbs_feature_div']//a[@class='a-link-normal a-color-tertiary']")
            category_name = ' > '.join(
                    [node.xpath("normalize-space(./text())").extract()[0].encode('utf-8') for node in
                     category_name_list])
        else:
            category_name = 'Unknown'
        product_baseinfo_item["category_name"] = category_name

        # in_sale_price
        # "priceblock_ourprice"  priceblock_saleprice
        if se.xpath("//*[@id='priceblock_ourprice']/text()"):
            in_sale_price = float('%.2f' % float(
                    se.xpath("//*[@id='priceblock_ourprice']/text()").extract()[0].encode('utf-8').replace('$', '')))
            print("in_sale_price",in_sale_price)
        elif se.xpath("//*[@id='priceblock_dealprice']/text()"):
            in_sale_price = float('%.2f' % float(
                    se.xpath("//*[@id='priceblock_dealprice']/text()").extract()[0].encode('utf-8').replace('$', '')))
        else:
            in_sale_price = 0.00
        product_baseinfo_item["in_sale_price"] = in_sale_price

        # regularprice_savings
        if se.xpath(
                "normalize-space(//*[@id='regularprice_savings']//td[@class='a-span12 a-color-price a-size-base']/text())"):
            regularprice_savingsmessage = se.xpath(
            "normalize-space(//*[@id='regularprice_savings']//td[@class='a-span12 a-color-price a-size-base']/text())").extract()[0].replace('$', '').split("(")[0].strip()
            if len(regularprice_savingsmessage) > 0:
                    # regularprice_savings = float('%.2f' % float(re.findall(r"\d+\.\d+", regularprice_savingsmessage)[0]))
                regularprice_savings = float(regularprice_savingsmessage)
            else:
                regularprice_savingsmessage = se.xpath(
                        "normalize-space(//*[@id='dealprice_savings']//td[@class='a-span12 a-color-price a-size-base']/text())").extract()[
                        0].replace('$', '').split("(")
                if regularprice_savingsmessage[0].strip():
                    regularprice_savings = float(regularprice_savingsmessage[0].strip())
                else:
                    regularprice_savings = 0.00
        else:
            regularprice_savings = 0.00
        original_price = float('%.2f' % (in_sale_price + regularprice_savings))
        product_baseinfo_item["original_price"] = original_price

        # review_cnt
        if se.xpath("//*[@id='acrCustomerReviewText']/text()"):
            review_cnt_str = se.xpath("//*[@id='acrCustomerReviewText']/text()").extract()[0].encode(
                    'utf-8').replace(',', '')
            review_cnt = int(re.findall(r"\d+", review_cnt_str)[0])
        else:
            review_cnt = 0
        product_baseinfo_item["review_cnt"] = review_cnt

        # review_avg_star
        review_avg_star = 0.0
        if se.xpath("//*[@id='acrPopover']/span[1]/a/i[1]/span/text()"):
            review_avg_star_str = se.xpath("//*[@id='acrPopover']/span[1]/a/i[1]/span/text()")[0].extract().encode('utf-8')
            review_avg_star = float(review_avg_star_str.split(' out of ')[0])
        product_baseinfo_item["review_avg_star"] = review_avg_star

        # percent_data
        percent_xpath = se.xpath("//*[@id='histogramTable']//tr")
        print("percent_xpath:",percent_xpath)
        if percent_xpath:
            for i,percent in enumerate(percent_xpath):
                percent_data = percent.xpath("./td[3]//text()").extract()
                print(percent_data)
                key = "percent_{}_star".format(5-i)
                if percent_data:
                    star = percent_data[0].decode('utf-8').strip()[:-1]
                    if star:
                        product_baseinfo_item[key] = star
                    else:
                        product_baseinfo_item[key] = 0
                else:
                    product_baseinfo_item[key] = 0
        else:
            product_baseinfo_item["percent_5_star"] = 0
            product_baseinfo_item["percent_4_star"] = 0
            product_baseinfo_item["percent_3_star"] = 0
            product_baseinfo_item["percent_2_star"] = 0
            product_baseinfo_item["percent_1_star"] = 0

        # cnt_qa
        if se.xpath("//*[@id='askATFLink']/span/text()"):
            cnt_qa_str = se.xpath("//*[@id='askATFLink']/span/text()")[0].extract().encode('utf-8')
            cnt_qa = int(re.findall(r"\d+", cnt_qa_str)[0])
        else:
            cnt_qa = 0
        product_baseinfo_item["cnt_qa"] = cnt_qa

        # lowest_price
        if se.xpath("//*[@id='olp_feature_div']/div/span[1]/a/text()"):
            lowest_price_str = \
                se.xpath("//*[@id='olp_feature_div']/div/span[1]/a/text()").extract()[0].encode('utf-8').split('from')[-1]
            lowest_price = float('%.2f' % float(re.findall(r"\d+\.\d+", lowest_price_str)[0]))
        else:
            lowest_price = 0.00
        product_baseinfo_item["lowest_price"] = lowest_price

        # offers_url
        if se.xpath("//*[@id='olp_feature_div']/div/span[1]/a/@href"):
            offers_url = se.xpath("//*[@id='olp_feature_div']/div/span[1]/a/@href").extract()[0].encode('utf-8')
        else:
            offers_url = 'Unknown'
        product_baseinfo_item["offers_url"] = offers_url

        product_baseinfo_item["ref_id"] = ref_id # default value

        yield product_baseinfo_item

        # ------- amazon_product_technical_details -------
        product_details = se.xpath("//*[@id='productDetails_techSpec_section_1']")      #技术细节
        if not product_details:
            product_details = se.xpath("//*[@id='technical-data']")

        if product_details:
            keys = map(StringUtilTool.clean_str, product_details.xpath(".//text()").extract())
            values = map(StringUtilTool.clean_str, product_details.xpath(".//text()").extract())
            dict_details = dict(zip(keys, values))

            for key, value in dict_details.iteritems():
                product_details_item = amazon_product_technical_details()
                product_details_item["zone"] = zone
                product_details_item["asin"] = asin
                product_details_item["key_name"] = key
                product_details_item["value_data"] = value
                product_details_item["ref_id"] = ref_id  # default value
                yield product_details_item

        # ------- amazon_product_category_sales_rank -------
        detailBullets = se.xpath("//*[@id='productDetails_detailBullets_sections1']")       #产品信息
        if detailBullets:
            category_ranks = detailBullets.xpath(".//th[contains(text(), 'Best Sellers Rank')]/../td/span/span")
            for category_rank in category_ranks:
                category_sales_rank_item = amazon_product_category_sales_rank()
                str_list = category_rank.xpath("./text()").extract()[0].encode('utf-8')
                sub_category = category_rank.xpath(".//a")
                for ind, sub_category_name in enumerate(sub_category):
                    if ind == 0:
                        str_list += sub_category_name.xpath("./text()").extract()[0].encode('utf-8')
                    else:
                        str_list += ' > ' + sub_category_name.xpath("./text()").extract()[0].encode('utf-8')
                str_list = str_list.replace(',', '')
                category_name_real = str_list.split(' in ')[1]
                category_sales_rank = int('%d' % int(re.findall(r"\d+", str_list.split(' in ')[0])[0]))
                # print('{}-{}'.format(category_name_real, category_sales_rank))

                category_sales_rank_item["zone"] = zone
                category_sales_rank_item["asin"] = asin
                category_sales_rank_item["category_name"] = category_name_real
                category_sales_rank_item["sales_rank"] = category_sales_rank
                category_sales_rank_item["ref_id"] = ref_id  # default value
                yield category_sales_rank_item
        # ------- amazon_product_descriptions -------
        product_title_item = amazon_product_descriptions()
        title = se.xpath("//*[@id='productTitle']")         #产品标题
        if title:
            title_str = title.xpath("./text()").extract()[0].encode('utf-8').strip()
            title_md5_str = hashlib.md5(title_str).hexdigest()
            product_title_item["zone"] = zone
            product_title_item["asin"] = asin
            product_title_item["description_type"] = 'title'
            product_title_item["desc_content"] = title_str
            product_title_item["md5_desc_content"] = title_md5_str
            product_title_item["ref_id"] = ref_id  # default value
            yield product_title_item

        ###################amazon_product_descriptions#############
        product_bullet_item = amazon_product_descriptions()             #产品描述 特点
        bullet = se.xpath("//*[@id='feature-bullets']")
        if bullet:
            bullets = bullet.xpath(".//span[@class='a-list-item']")
            bullets_str = ''
            for bul in bullets:
                bullets_str += bul.xpath("./text()").extract()[0].encode('utf-8').strip() + '\n'

            bullets_md5_str = hashlib.md5(bullets_str).hexdigest()
            product_bullet_item["zone"] = zone
            product_bullet_item["asin"] = asin
            product_bullet_item["description_type"] = 'bullet'
            product_bullet_item["desc_content"] = bullets_str
            product_bullet_item["md5_desc_content"] = bullets_md5_str
            product_bullet_item["ref_id"] = ref_id  # default value
            yield product_bullet_item

        # ------- amazon_product_descriptions -------
        product_description_item = amazon_product_descriptions()
        description = se.xpath("//*[@id='productDescription']")                 #产品描述
        if description:
            descriptions = description.xpath(".//text()")
            description_str = ''
            for desc in descriptions:
                description_str += desc.extract().strip()

            description_md5_str = hashlib.md5(description_str).hexdigest()
            product_description_item["zone"] = zone
            product_description_item["asin"] = asin
            product_description_item["description_type"] = 'description'
            product_description_item["desc_content"] = description_str
            product_description_item["md5_desc_content"] = description_md5_str
            product_description_item["ref_id"] = ref_id  # default value
            yield product_description_item

        # ------- amazon_product_pictures -------
        image_block = se.xpath("//*[@id='imageBlock_feature_div']")         #产品图片
        if image_block:
            images = image_block.xpath(".//img/@src")
            for ind, image in enumerate(images):
                image_item = amazon_product_pictures()
                if (image.extract().startswith('http')):
                    image_ind = ind + 1
                    image_url = image.extract()
                    image_md5 = hashlib.md5(image_url).hexdigest()
                image_item["zone"] = zone
                image_item["asin"] = asin
                image_item["img_ind"] = image_ind
                image_item["img_url"] = image_url
                image_item["md5_img_url"] = image_md5
                image_item["ref_id"] = ref_id  # default value
                yield image_item

        # ------- amazon_product_bought_together_list -------
        bought_together = se.xpath("//*[@id='sims-fbt-form']")      #经常一起买组合套餐
        if bought_together:
            bought_together_lst = bought_together.xpath(".//li/@data-p13n-asin-metadata")
            also_str_lst = ''
            for item in bought_together_lst:
                asin_t = ast.literal_eval(item.extract())["asin"] + '-'
                also_str_lst += asin_t

            also_str_lst = also_str_lst[: -1]

            bought_together_item = amazon_product_bought_together_list()
            bought_together_item["zone"] = zone
            bought_together_item["asin"] = asin
            bought_together_item["bought_together_list"] = also_str_lst
            bought_together_item["ref_id"] = ref_id  # default value
            yield bought_together_item

        # ------- amazon_product_also_bought_list -------
        #买这个东西的顾客也买了的商品列表
        also_bought_lst_xpath = se.xpath("//*[@id='purchase-sims-feature']/div/@data-a-carousel-options")

        if also_bought_lst_xpath:
            also_bought_item = amazon_product_also_bought_list()
            also_bought_lst_str = also_bought_lst_xpath.extract()[0].encode('utf-8')
            also_bought_dict = ast.literal_eval(also_bought_lst_str)
            also_bought_lst = also_bought_dict["ajax"]["id_list"]

            also_bought_item["zone"] = zone
            also_bought_item["asin"] = asin
            also_bought_item["also_bought_list"] = '-'.join(also_bought_lst)
            also_bought_item["ref_id"] = ref_id  # default value
            yield also_bought_item

        # ------- amazon_product_current_reviews type:top-------
        top_customer_reviews = se.xpath(
                "//div[@id='cm-cr-dp-review-list']/div[@data-hook='review']")  # top-customer-reviews-widget 置顶客户评论
        if top_customer_reviews:
                # top_customer_reviews_lst = top_customer_reviews.extract()
            for i, r in enumerate(top_customer_reviews):
                review_id = r.xpath("./@id").extract()[0].encode('utf-8')
                top_review = amazon_product_current_reviews()
                top_review["zone"] = zone
                top_review["asin"] = asin
                top_review["ref_id"] = ref_id  # default value
                top_review["review_order_type"] = 'top'
                top_review["order_index"] = i + 1

                top_review["review_star"] = \
                        r.xpath(".//i[@data-hook='review-star-rating']/span/text()").extract()[0].encode(
                                'utf-8').split(' out of ')[0]

                if r.xpath(".//span[@data-hook='avp-badge-linkless']/text()").extract():
                    if r.xpath(".//span[@data-hook='avp-badge-linkless']/text()").extract()[0].encode('utf-8') == 'Verified Purchase':
                        top_review["is_verified_purchase"] = 1
                    else:
                        top_review["is_verified_purchase"] = 0
                else:
                    top_review["is_verified_purchase"] = 0

                votes_ = r.xpath(".//span[@data-hook='helpful-vote-statement']/text()")
                if votes_:
                    votes_str = votes_.extract()[0].encode('utf-8')
                    top_review["votes"] = re.sub("\D", "", votes_str) if re.sub("\D", "", votes_str) else 0
                else:
                    top_review["votes"] = 0
                is_top_review=r.xpath(
                    ".//span[@class='a-size-mini a-color-link c7yBadgeAUI c7yTopDownDashedStrike c7y-badge-text a-text-bold']/text()")
                if is_top_review:
                    top_review["is_top_reviewer"] = is_top_review.extract()[0].encode('utf-8')
                else:
                    top_review["is_top_reviewer"] = ""

                top_review["cnt_imgs"] = 0
                images_list = r.xpath(".//img")
                cnt_imgs = len(images_list)
                top_review["cnt_imgs"] = cnt_imgs

                top_review["cnt_vedios"] = 0
                video_id = 'videl-block-' + review_id
                video_path = './/div[@id="%s"]' % video_id
                video_list = r.xpath(video_path)
                cnt_vedios = len(video_list)
                top_review["cnt_vedios"] = cnt_vedios

                yield top_review

        # ------- amazon_product_current_reviews type:recent-------
        recent_customer_reviews = se.xpath(
                "//div[@id='most-recent-reviews-content']/div[@data-hook='recent-review']")  # 最近的客户评论
        if recent_customer_reviews:
            for i, r in enumerate(recent_customer_reviews):
                review_id = r.xpath("./@id").extract()[0].encode('utf-8')
                recent_review = amazon_product_current_reviews()
                recent_review["zone"] = zone
                recent_review["asin"] = asin
                recent_review["ref_id"] = ref_id  # default value
                recent_review["review_order_type"] = 'recent'
                recent_review["order_index"] = i + 1

                recent_review["review_star"] = \
                        r.xpath(".//i[@data-hook='review-star-rating-recent']/span/text()").extract()[0].encode(
                                'utf-8').split(' out of ')[0]

                if r.xpath(".//span[@data-hook='avp-badge-linkless']/text()").extract():
                    if r.xpath(".//span[@data-hook='avp-badge-linkless']/text()").extract()[0].encode(
                                    'utf-8') == 'Verified Purchase':
                        recent_review["is_verified_purchase"] = 1
                else:
                    recent_review["is_verified_purchase"] = 0

                votes_ = r.xpath(".//span[@data-hook='helpful-vote-statement']/text()")
                if votes_:
                    votes_str = votes_.extract()[0].encode('utf-8')
                    recent_review["votes"] = re.sub("\D", "", votes_str) if re.sub("\D", "", votes_str) else 0
                else:
                    recent_review["votes"] = 0
                is_top_reviewer = r.xpath(
                        ".//span[@class='a-size-mini a-color-link c7yBadgeAUI c7yTopDownDashedStrike c7y-badge-text a-text-bold']/text()").extract()
                if is_top_reviewer:
                    recent_review["is_top_reviewer"] = is_top_reviewer[0].encode('utf-8')
                else:
                    recent_review["is_top_reviewer"] = ""
                recent_review["cnt_imgs"] = 0
                images_list = r.xpath(".//img")
                cnt_imgs = len(images_list)
                recent_review["cnt_imgs"] = cnt_imgs

                recent_review["cnt_vedios"] = 0
                video_id = 'videl-block-' + review_id
                video_path = './/div[@id="%s"]' % video_id
                video_list = r.xpath(video_path)
                cnt_vedios = len(video_list)
                recent_review["cnt_vedios"] = cnt_vedios

                yield recent_review
        # ------- amazon_product_promotions -------
        promotion_list = ''
        promotions_xpath = se.xpath("//*[@id='quickPromoBucketContent']//li")
        for promotion in promotions_xpath:
            if len(promotion.xpath("./text()").extract())>0:
                promotion_list += promotion.xpath("./text()").extract()[0].encode('utf-8')  #优惠信息
        md5_promotion = hashlib.md5(promotion_list).hexdigest()
        promotion_item = amazon_product_promotions()
        promotion_item["zone"] = zone
        promotion_item["asin"] = asin
        promotion_item["promotion_list"] = promotion_list
        promotion_item["md5_promotion"] = md5_promotion
        promotion_item["ref_id"] = ref_id  # default value
        yield promotion_item

        # ------- amazon_traffic_sponsored_products_1 -------
        sponsored_xpath = se.xpath("//*[@id='sp_detail']/@data-a-carousel-options")
        if sponsored_xpath.extract() and re.findall('initialSeenAsins(.+)circular',
                                                    sponsored_xpath.extract()[0].encode('utf-8')):
            sponsored_lst = re.findall('initialSeenAsins(.+)circular', sponsored_xpath.extract()[0].encode('utf-8'))[
                0].replace('\\"', '').replace('"', '').replace(
                ' ', '').replace(':', '')                                              #与本项目有关的商品1
            sponsored_item = amazon_traffic_sponsored_products()
            sponsored_item["zone"] = zone
            sponsored_item["asin"] = asin
            sponsored_item["type"] = 1
            sponsored_item["product_list"] = sponsored_lst
            sponsored_item["ref_id"] = ref_id  # default value
            yield sponsored_item

        # ------- amazon_traffic_sponsored_products_2 -------
        sponsored_xpath = se.xpath("//*[@id='sp_detail2']/@data-a-carousel-options")  #与本项目有关的赞助产品2
        sponsoreds = sponsored_xpath.extract()
        if sponsoreds:
            if re.findall('initialSeenAsins(.+)circular',sponsoreds[0]):
                sponsored_lst = re.findall('initialSeenAsins(.+)circular',sponsoreds[0])[0].replace('\\"', '').\
                replace('"', '').replace(' ', '').replace(':', '')
        sponsored_item = amazon_traffic_sponsored_products()
        sponsored_item["zone"] = zone
        sponsored_item["asin"] = asin
        sponsored_item["type"] = 2
        sponsored_item["product_list"] = sponsored_lst
        sponsored_item["ref_id"] = ref_id  # default value
        yield sponsored_item

        # ------- amazon_traffic_buy_other_after_view -------
        buy_other_xpath = se.xpath("//*[@id='view_to_purchase-sims-feature']//a/@href").extract()
        buy_other_set = set()
        for temp in buy_other_xpath:
            temp_str = temp.encode('utf-8')
            if '/dp/' in temp_str and '/ref' in temp_str:
                buy_other_set.add(re.findall('/dp/(.+)/ref', temp_str)[0])
        buy_other_lst = ','.join(buy_other_set)

        buy_other_item = amazon_traffic_buy_other_after_view()

        buy_other_item["zone"] = zone
        buy_other_item["asin"] = asin

        buy_other_item["product_list"] = buy_other_lst    #顾客在看这个商品后购买了哪些商品？ asin,asin,asin,asin
        buy_other_item["ref_id"] = ref_id
        yield buy_other_item

        # ------- amazon_traffic_similar_items -------
        similar_item_xpath = se.xpath("//*[@id='HLCXComparisonTable']//a/@href").extract()
        similar_item_list = []
        for temp in similar_item_xpath:
            temp_str = temp.encode('utf-8')
            if '/dp/' in temp_str and '/ref' in temp_str:
                similar_item_list.append(re.findall('/dp/(.+)/ref', temp_str)[0])
        similar_item_lst = ','.join(similar_item_list)

        similar_item_item = amazon_traffic_similar_items()
        similar_item_item["zone"] = zone
        similar_item_item["asin"] = asin
        similar_item_item["product_list"] = similar_item_lst   #类似商品 asin,asin,asin,asin
        similar_item_item["ref_id"] = ref_id  # default value
        yield similar_item_item

