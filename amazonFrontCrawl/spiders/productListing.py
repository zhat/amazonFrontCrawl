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
    name = "productListing"
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
            'SELECT distinct url,asin,ref_id FROM '+settings.AMAZON_REF_PRODUCT_LIST+' where STATUS = "1" ;'
        )

        rows = self.cursor.fetchall()
        for row in rows:
            # print(row[0])
            # yield self.make_requests_from_url(row[0])
            yield Request(row[0], callback=self.parse_product_listing, meta={'ref_id': row[2]})
        self.conn.close()

    def parse(self, response):
        logging.info("productListing parse start .....")

        # user = settings.MYSQL_USER
        # passwd = settings.MYSQL_PASSWD
        # db = settings.MYSQL_DBNAME
        # host = settings.MYSQL_HOST
        # conn = MySQLdb.connect(
        #     user=user,
        #     passwd=passwd,
        #     db=db,
        #     host=host,
        #     charset="utf8",
        #     use_unicode=True
        # )
        # cursor = conn.cursor()
        # cursor.execute(
        #     'SELECT distinct url,asin,ref_id FROM '+settings.AMAZON_REF_PRODUCT_LIST+' where STATUS = "1" ;'
        # )
        #
        # rows = self.cursor.fetchall()
        # for row in rows:
        #     # print(row[0])
        #     # yield self.make_requests_from_url(row[0])
        #     yield Request(row[0], callback=self.parse_product_listing, meta={'ref_id': row[2]})
        # self.conn.close()
        
        # yield Request(response.request.url, callback=self.parse_product_listing)

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

        # print("*" * 150)
        # print(asin)
        # print("*" * 150)

        product_baseinfo_item["asin"] = asin

        product_baseinfo_item["ref_id"] = ref_id # default value

        # ref_id = response.meta['batch_number']

        # sku = scrapy.Field()
        # seller_name
        try:
            if se.xpath("//*[@id='merchant-info']/a[1]/text()"):
                seller_name = se.xpath("//*[@id='merchant-info']/a[1]/text()").extract()[0].encode('utf-8')
            else:
                seller_name = 'Unknown'
        except Exception as e:
            seller_name = 'Unknown'
            logging.error("//*[@id='merchant-info']/a[1]/text():" + e.message)
        product_baseinfo_item["seller_name"] = seller_name

        # seller_url
        try:
            if se.xpath("//*[@id='merchant-info']/a[1]/@href"):
                seller_url = se.xpath("//*[@id='merchant-info']/a[1]/@href").extract()[0].encode('utf-8')
            else:
                seller_url = 'Unknown'
        except Exception as e:
            seller_url = 'Unknown'
            logging.error("//*[@id='merchant-info']/a[1]/@href:" + e.message)
        product_baseinfo_item["seller_url"] = seller_url

        # brand
        try:
            if se.xpath("//*[@id='bylineInfo']/text()"):
                brand = se.xpath("//*[@id='bylineInfo']/text()").extract()[0].encode('utf-8')
            else:
                brand = 'Unknown'
        except Exception as e:
            brand = 'Unknown'
            logging.error("//*[@id='bylineInfo']/text():" + e.message)
        product_baseinfo_item["brand"] = brand

        # brand_url
        try:
            if se.xpath("//*[@id='bylineInfo']/@href"):
                brand_url = se.xpath("//*[@id='bylineInfo']/@href").extract()[0].encode('utf-8')
            else:
                brand_url = 'Unknown'
        except Exception as e:
            brand_url = 'Unknown'
            logging.error("//*[@id='bylineInfo']/@href:" + e.message)
        product_baseinfo_item["brand_url"] = brand_url

        # is_fba
        try:
            if se.xpath("//*[@id='merchant-info']/a[2]/text()"):
                is_fba_str = se.xpath("//*[@id='merchant-info']/a[2]/text()").extract()[0].encode('utf-8')
                is_fba = StringUtilTool.getFbaFromStr(is_fba_str)
            else:
                is_fba = 'Unknown'
        except Exception as e:
            is_fba = 'Unknown'
            logging.error("//*[@id='merchant-info']/a[2]/text():" + e.message)
        product_baseinfo_item["is_fba"] = is_fba

        # stock_situation stop here
        try:
            if se.xpath("normalize-space(//*[@id='availability']/span/text())"):
                stock_situation = se.xpath("normalize-space(//*[@id='availability']/span/text())").extract()[0].encode(
                    'utf-8')
            else:
                stock_situation = 'Unknown'
        except Exception as e:
            stock_situation = 'Unknown'
            logging.error("normalize-space(//*[@id='availability']/span/text()):" + e.message)
        product_baseinfo_item["stock_situation"] = stock_situation

        # category_name
        try:
            if se.xpath("//*[@id='wayfinding-breadcrumbs_feature_div']//a[@class='a-link-normal a-color-tertiary']"):
                category_name_list = se.xpath(
                    "//*[@id='wayfinding-breadcrumbs_feature_div']//a[@class='a-link-normal a-color-tertiary']")
                category_name = ' > '.join(
                    [node.xpath("normalize-space(./text())").extract()[0].encode('utf-8') for node in
                     category_name_list])
            else:
                category_name = 'Unknown'
        except Exception as e:
            category_name = 'Unknown'
            logging.error("//*[@id='wayfinding-breadcrumbs_feature_div']//a[@class='a-link-normal a-color-tertiary']:" + e.message)
        product_baseinfo_item["category_name"] = category_name

        # for node in category_name_list:
        #     category_name += node.xpath("normalize-space(./text())").extract()[0].encode('utf-8') + ' > '

        # in_sale_price
        try:
            if se.xpath("//*[@id='priceblock_saleprice']/text()"):
                in_sale_price = float('%.2f' % float(
                    se.xpath("//*[@id='priceblock_saleprice']/text()").extract()[0].encode('utf-8').replace('$', '')))
            elif se.xpath("//*[@id='priceblock_dealprice']/text()"):
                in_sale_price = float('%.2f' % float(
                    se.xpath("//*[@id='priceblock_dealprice']/text()").extract()[0].encode('utf-8').replace('$', '')))
            else:
                in_sale_price = 0.00
        except Exception as e:
            in_sale_price = 0.00
            logging.error("//*[@id='priceblock_saleprice']/text():" + e.message)
        product_baseinfo_item["in_sale_price"] = in_sale_price

        # saleprice_shipping
        # try:
        #     if se.xpath(
        #             "normalize-space(//*[@id='saleprice_shippingmessage']//span[@class='a-size-base a-color-secondary']/text())"):
        #         saleprice_shippingmessage = se.xpath(
        #             "normalize-space(//*[@id='saleprice_shippingmessage']//span[@class='a-size-base a-color-secondary']/text())").extract()[
        #             0].encode('utf-8').replace('$', '')
        #         if len(saleprice_shippingmessage) > 0:
        #             saleprice_shipping = float('%.2f' % float(re.findall(r"\d+\.\d+", saleprice_shippingmessage)[0]))
        #         else:
        #             saleprice_shipping = 0.00
        #     else:
        #         saleprice_shipping = 0.00
        # except Exception as e:
        #     saleprice_shipping = 0.00
        #     logging.error("normalize-space(//*[@id='saleprice_shippingmessage']//span[@class='a-size-base a-color-secondary']/text()):" + e.message)
        # in_sale_price += saleprice_shipping
        # product_baseinfo_item["in_sale_price"] = in_sale_price

        # regularprice_savings
        try:
            if se.xpath(
                    "normalize-space(//*[@id='regularprice_savings']//td[@class='a-span12 a-color-price a-size-base']/text())"):
                regularprice_savingsmessage = se.xpath(
                    "normalize-space(//*[@id='regularprice_savings']//td[@class='a-span12 a-color-price a-size-base']/text())").extract()[
                    0].encode('utf-8').replace('$', '').split("(")[0].strip()
                if len(regularprice_savingsmessage) > 0:
                    # regularprice_savings = float('%.2f' % float(re.findall(r"\d+\.\d+", regularprice_savingsmessage)[0]))
                    regularprice_savings = float(regularprice_savingsmessage)
                else:
                    regularprice_savingsmessage = se.xpath(
                        "normalize-space(//*[@id='dealprice_savings']//td[@class='a-span12 a-color-price a-size-base']/text())").extract()[
                        0].encode('utf-8').replace('$', '').split("(")[0].strip()
                    regularprice_savings = float(regularprice_savingsmessage)
            else:
                regularprice_savings = 0.00
        except Exception as e:
            regularprice_savings = 0.00
            logging.error("normalize-space(//*[@id='regularprice_savings']//td[@class='a-span12 a-color-price a-size-base']/text()):" + e.message)
        original_price = float('%.2f' % (in_sale_price + regularprice_savings))
        product_baseinfo_item["original_price"] = original_price

        # review_cnt
        try:
            if se.xpath("//*[@id='acrCustomerReviewText']/text()"):
                review_cnt_str = se.xpath("//*[@id='acrCustomerReviewText']/text()").extract()[0].encode(
                    'utf-8').replace(
                    ',', '')
                review_cnt = int(re.findall(r"\d+", review_cnt_str)[0])
            else:
                review_cnt = 0
        except Exception as e:
            review_cnt = 0
            logging.error("//*[@id='acrCustomerReviewText']/text():" + e.message)
        product_baseinfo_item["review_cnt"] = review_cnt

        # review_avg_star
        review_avg_star = 0.0
        try:
            if se.xpath("//*[@id='acrPopover']/span[1]/a/i[1]/span/text()"):
                review_avg_star_str = se.xpath("//*[@id='acrPopover']/span[1]/a/i[1]/span/text()")[0].extract().encode(
                    'utf-8')
                review_avg_star = float(review_avg_star_str.split(' out of ')[0])
        except Exception as e:
            review_avg_star = 0.0
            logging.error("//*[@id='acrPopover']/span[1]/a/i[1]/span/text():" + e.message)
        product_baseinfo_item["review_avg_star"] = review_avg_star

        # percent_data
        try:
            percent_data = se.xpath("//*[@id='histogramTable']//text()")
            if percent_data:
                percent_data_lst = percent_data.extract()
                for i, p in enumerate(percent_data_lst):
                    if i == 1:
                        percent_5_star = p.encode('utf-8')[:-1]
                    elif i == 3:
                        percent_4_star = p.encode('utf-8')[:-1]
                    elif i == 5:
                        percent_3_star = p.encode('utf-8')[:-1]
                    elif i == 7:
                        percent_2_star = p.encode('utf-8')[:-1]
                    elif i == 9:
                        percent_1_star = p.encode('utf-8')[:-1]
                product_baseinfo_item["percent_5_star"] = percent_5_star
                product_baseinfo_item["percent_4_star"] = percent_4_star
                product_baseinfo_item["percent_3_star"] = percent_3_star
                product_baseinfo_item["percent_2_star"] = percent_2_star
                product_baseinfo_item["percent_1_star"] = percent_1_star
        except Exception as e:
            logging.error("//*[@id='histogramTable']//text():" + e.message)

        # cnt_qa
        try:
            if se.xpath("//*[@id='askATFLink']/span/text()"):
                cnt_qa_str = se.xpath("//*[@id='askATFLink']/span/text()")[0].extract().encode('utf-8')
                cnt_qa = int(re.findall(r"\d+", cnt_qa_str)[0])
            else:
                cnt_qa = 0
        except Exception as e:
            cnt_qa = 0
            logging.error("//*[@id='askATFLink']/span/text():" + e.message)
        product_baseinfo_item["cnt_qa"] = cnt_qa

        # lowest_price
        try:
            if se.xpath("//*[@id='olp_feature_div']/div/span[1]/a/text()"):
                lowest_price_str = \
                    se.xpath("//*[@id='olp_feature_div']/div/span[1]/a/text()").extract()[0].encode('utf-8').split(
                        'from')[
                        -1]
                lowest_price = float('%.2f' % float(re.findall(r"\d+\.\d+", lowest_price_str)[0]))
            else:
                lowest_price = 0.00
        except Exception as e:
            lowest_price = 0.00
            logging.error("//*[@id='olp_feature_div']/div/span[1]/a/text():" + e.message)
        product_baseinfo_item["lowest_price"] = lowest_price

        # offers_url
        try:
            if se.xpath("//*[@id='olp_feature_div']/div/span[1]/a/@href"):
                offers_url = se.xpath("//*[@id='olp_feature_div']/div/span[1]/a/@href").extract()[0].encode('utf-8')
            else:
                offers_url = 'Unknown'
        except Exception as e:
            offers_url = 'Unknown'
            logging.error("//*[@id='olp_feature_div']/div/span[1]/a/@href:" + e.message)
        product_baseinfo_item["offers_url"] = offers_url

        product_baseinfo_item["ref_id"] = ref_id # default value

        yield product_baseinfo_item


        # ------- amazon_product_technical_details -------
        try:
            product_details = se.xpath("//*[@id='productDetails_techSpec_section_1']")
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
        except Exception as e:
            logging.error("//*[@id='productDetails_techSpec_section_1']:" + e.message)

        # ------- amazon_product_category_sales_rank -------
        if asin == 'B06XG5WNZ9':
            print('stop here')
            pass
        try:
            detailBullets = se.xpath("//*[@id='productDetails_detailBullets_sections1']")
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
            else:
                print('&' * 100)
                print(asin)
                print('&' * 100)
        except Exception as e:
            logging.error("//*[@id='productDetails_detailBullets_sections1']:" + e.message)

        # ------- amazon_product_descriptions -------
        try:
            product_title_item = amazon_product_descriptions()
            title = se.xpath("//*[@id='productTitle']")
            if title:
                title_str = title.xpath("./text()").extract()[0].encode('utf-8').strip()
                title_md5_str = hashlib.md5(title_str).hexdigest()
                product_title_item["zone"] = zone
                product_title_item["asin"] = asin
                product_title_item["description_type"] = 'title'
                product_title_item["desc_content"] = title_str
                product_title_item["md5_desc_content"] = title_md5_str
                product_title_item["ref_id"] = ref_id  # default value
                # print(title_str)
                # print(title_md5_str)
                yield product_title_item
        except Exception as e:
            logging.error("//*[@id='productTitle']:" + e.message)

        try:
            product_bullet_item = amazon_product_descriptions()
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
                # print(title_str)
                # print(title_md5_str)
                yield product_bullet_item
        except Exception as e:
            logging.error("//*[@id='productTitle']:" + e.message)

        try:
            product_description_item = amazon_product_descriptions()
            description = se.xpath("//*[@id='productDescription']")
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
                # print(title_str)
                # print(title_md5_str)
                yield product_description_item
        except Exception as e:
            logging.error("//*[@id='productDescription']:" + e.message)


        # ------- amazon_product_pictures -------
        try:
            image_block = se.xpath("//*[@id='imageBlock_feature_div']")
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
        except Exception as e:
            logging.error("//*[@id='imageBlock_feature_div']:" + e.message)


        # ------- amazon_product_bought_together_list -------
        try:
            bought_together = se.xpath("//*[@id='sims-fbt-form']")
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
        except Exception as e:
            logging.error("//*[@id='sims-fbt-form']:" + e.message)


        # ------- amazon_product_also_bought_list -------
        try:
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
        except Exception as e:
            logging.error("//*[@id='purchase-sims-feature']/div/@data-a-carousel-options:" + e.message)

        # ------- amazon_product_current_reviews type:top-------
        try:
            top_customer_reviews = se.xpath(
                "//div[@id='cm-cr-dp-review-list']/div[@data-hook='review']")  # top-customer-reviews-widget
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

                    try:
                        top_review["review_star"] = \
                            r.xpath(".//i[@data-hook='review-star-rating']/span/text()").extract()[0].encode(
                                'utf-8').split(' out of ')[0]
                    except Exception as e:
                        top_review["review_star"] = 0

                    try:
                        if r.xpath(".//span[@data-hook='avp-badge-linkless']/text()").extract()[0].encode('utf-8'):
                            if r.xpath(".//span[@data-hook='avp-badge-linkless']/text()").extract()[0].encode(
                                    'utf-8') == 'Verified Purchase':
                                top_review["is_verified_purchase"] = 1
                        else:
                            top_review["is_verified_purchase"] = 0
                    except Exception as e:
                        top_review["is_verified_purchase"] = 0

                    try:
                        votes_ = r.xpath(".//span[@data-hook='helpful-vote-statement']/text()")
                        if votes_:
                            votes_str = votes_.extract()[0].encode('utf-8')
                            top_review["votes"] = re.sub("\D", "", votes_str) if re.sub("\D", "", votes_str) else 0
                        else:
                            top_review["votes"] = 0
                    except Exception as e:
                        top_review["votes"] = 0

                    try:
                        is_top_reviewer = r.xpath(
                            ".//span[@class='a-size-mini a-color-link c7yBadgeAUI c7yTopDownDashedStrike c7y-badge-text a-text-bold']/text()").extract()[
                            0].encode('utf-8')
                    except Exception as e:
                        is_top_reviewer = ''
                    top_review["is_top_reviewer"] = is_top_reviewer

                    top_review["cnt_imgs"] = 0
                    try:
                        images_list = r.xpath(".//img")
                        cnt_imgs = len(images_list)
                        top_review["cnt_imgs"] = cnt_imgs
                    except Exception as e:
                        pass

                    top_review["cnt_vedios"] = 0
                    try:
                        video_id = 'videl-block-' + review_id
                        video_path = './/div[@id="%s"]' % video_id
                        video_list = r.xpath(video_path)
                        cnt_vedios = len(video_list)
                        top_review["cnt_vedios"] = cnt_vedios
                    except Exception as e:
                        pass

                    yield top_review
        except Exception as e:
            logging.error("//div[@id='cm-cr-dp-review-list']/div[@data-hook='review']:" + e.message)



        # ------- amazon_product_current_reviews type:recent-------
        try:
            recent_customer_reviews = se.xpath(
                "//div[@id='most-recent-reviews-content']/div[@data-hook='recent-review']")  # most-recent-reviews-content
            if recent_customer_reviews:
                for i, r in enumerate(recent_customer_reviews):
                    review_id = r.xpath("./@id").extract()[0].encode('utf-8')
                    recent_review = amazon_product_current_reviews()
                    recent_review["zone"] = zone
                    recent_review["asin"] = asin
                    recent_review["ref_id"] = ref_id  # default value
                    recent_review["review_order_type"] = 'recent'
                    recent_review["order_index"] = i + 1

                    try:
                        recent_review["review_star"] = \
                            r.xpath(".//i[@data-hook='review-star-rating-recent']/span/text()").extract()[0].encode(
                                'utf-8').split(' out of ')[0]
                    except Exception as e:
                        recent_review["review_star"] = 0

                    try:
                        if r.xpath(".//span[@data-hook='avp-badge-linkless']/text()").extract()[0].encode('utf-8'):
                            if r.xpath(".//span[@data-hook='avp-badge-linkless']/text()").extract()[0].encode(
                                    'utf-8') == 'Verified Purchase':
                                recent_review["is_verified_purchase"] = 1
                        else:
                            recent_review["is_verified_purchase"] = 0
                    except Exception as e:
                        recent_review["is_verified_purchase"] = 0

                    try:
                        votes_ = r.xpath(".//span[@data-hook='helpful-vote-statement']/text()")
                        if votes_:
                            votes_str = votes_.extract()[0].encode('utf-8')
                            recent_review["votes"] = re.sub("\D", "", votes_str) if re.sub("\D", "", votes_str) else 0
                        else:
                            recent_review["votes"] = 0
                    except Exception as e:
                        recent_review["votes"] = 0

                    try:
                        is_top_reviewer = r.xpath(
                            ".//span[@class='a-size-mini a-color-link c7yBadgeAUI c7yTopDownDashedStrike c7y-badge-text a-text-bold']/text()").extract()[
                            0].encode('utf-8')
                    except Exception as e:
                        is_top_reviewer = ''
                    recent_review["is_top_reviewer"] = is_top_reviewer

                    recent_review["cnt_imgs"] = 0
                    try:
                        images_list = r.xpath(".//img")
                        cnt_imgs = len(images_list)
                        recent_review["cnt_imgs"] = cnt_imgs
                    except Exception as e:
                        pass

                    recent_review["cnt_vedios"] = 0
                    try:
                        video_id = 'videl-block-' + review_id
                        video_path = './/div[@id="%s"]' % video_id
                        video_list = r.xpath(video_path)
                        cnt_vedios = len(video_list)
                        recent_review["cnt_vedios"] = cnt_vedios
                    except Exception as e:
                        pass

                    yield recent_review
        except Exception as e:
            logging.error("//div[@id='most-recent-reviews-content']/div[@data-hook='recent-review']:" + e.message)


        # ------- amazon_product_promotions -------
        try:
            promotion_list = ''
            promotions_xpath = se.xpath("//*[@id='quickPromoBucketContent']//li")
            for promotion in promotions_xpath:
                promotion_list += promotion.xpath("./text()").extract()[0].encode('utf-8')
            md5_promotion = hashlib.md5(promotion_list).hexdigest()
            promotion_item = amazon_product_promotions()
            promotion_item["zone"] = zone
            promotion_item["asin"] = asin
            promotion_item["promotion_list"] = promotion_list
            promotion_item["md5_promotion"] = md5_promotion
            promotion_item["ref_id"] = ref_id  # default value
            yield promotion_item

        except Exception as e:
            logging.error("//*[@id='quickPromoBucketContent']//li:" + e.message)


        # ------- amazon_traffic_sponsored_products_1 -------
        try:
            sponsored_xpath = se.xpath("//*[@id='sp_detail']/@data-a-carousel-options")
            sponsored_lst = re.findall('initialSeenAsins(.+)circular', sponsored_xpath.extract()[0].encode('utf-8'))[
                0].replace('\\"', '').replace('"', '').replace(
                ' ', '').replace(':', '')
            sponsored_item = amazon_traffic_sponsored_products()
            sponsored_item["zone"] = zone
            sponsored_item["asin"] = asin
            sponsored_item["type"] = 1
            sponsored_item["product_list"] = sponsored_lst
            sponsored_item["ref_id"] = ref_id  # default value
            yield sponsored_item
        except Exception as e:
            logging.error("//*[@id='sp_detail']/@data-a-carousel-options:" + e.message)


        # ------- amazon_traffic_sponsored_products_2 -------
        try:
            sponsored_xpath = se.xpath("//*[@id='sp_detail2']/@data-a-carousel-options")
            sponsored_lst = re.findall('initialSeenAsins(.+)circular', sponsored_xpath.extract()[0].encode('utf-8'))[
                0].replace('\\"', '').replace('"', '').replace(
                ' ', '').replace(':', '')
            sponsored_item = amazon_traffic_sponsored_products()
            sponsored_item["zone"] = zone
            sponsored_item["asin"] = asin
            sponsored_item["type"] = 2
            sponsored_item["product_list"] = sponsored_lst
            sponsored_item["ref_id"] = ref_id  # default value
            yield sponsored_item
        except Exception as e:
            logging.error("//*[@id='sp_detail']/@data-a-carousel-options:" + e.message)


        # ------- amazon_traffic_buy_other_after_view -------
        try:
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
            buy_other_item["product_list"] = buy_other_lst
            buy_other_item["ref_id"] = ref_id  # default value
            yield buy_other_item
        except Exception as e:
            logging.error("//*[@id='view_to_purchase-sims-feature']//a/@href:" + e.message)

        # ------- amazon_traffic_similar_items -------
        try:
            similar_item_xpath = se.xpath("//*[@id='HLCXComparisonTable']//a/@href").extract()
            similar_item_set = set()
            for temp in similar_item_xpath:
                temp_str = temp.encode('utf-8')
                if '/dp/' in temp_str and '/ref' in temp_str:
                    similar_item_set.add(re.findall('/dp/(.+)/ref', temp_str)[0])
                    similar_item_lst = ','.join(similar_item_set)
                    similar_item_item = amazon_traffic_similar_items()
                    similar_item_item["zone"] = zone
                    similar_item_item["asin"] = asin
                    similar_item_item["product_list"] = similar_item_lst
                    similar_item_item["ref_id"] = ref_id  # default value
            yield similar_item_item
        except Exception as e:
            logging.error("//*[@id='HLCXComparisonTable']//a/@href:" + e.message)

