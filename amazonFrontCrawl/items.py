# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class AmazonfrontcrawlItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

# products in sales in our shop
class AmazonProductInsalesItem(scrapy.Item):
    shop_id     = scrapy.Field()
    zone        = scrapy.Field()
    brand       = scrapy.Field()
    asin        = scrapy.Field()
    order_index = scrapy.Field()
    time_id     = scrapy.Field()
    create_date = scrapy.Field()
    update_date = scrapy.Field()


# ############################top 100 series############################
# amazon_top100_best_sellers
# amazon_top100_gift_ideas
# amazon_top100_hot_new_releases
# amazon_top100_most_wished
class AmazonTop100BestSellersItem(scrapy.Item):
    zone          = scrapy.Field()
    category_name = scrapy.Field()
    category_url  = scrapy.Field()
    asin          = scrapy.Field()
    order_index   = scrapy.Field()
    batch_number  = scrapy.Field()
    create_date   = scrapy.Field()
    update_date   = scrapy.Field()

class AmazonTop100GiftIdeasItem(scrapy.Item):
    zone          = scrapy.Field()
    category_name = scrapy.Field()
    category_url  = scrapy.Field()
    asin          = scrapy.Field()
    order_index   = scrapy.Field()
    batch_number  = scrapy.Field()
    create_date   = scrapy.Field()
    update_date   = scrapy.Field()

class AmazonTop100MostWishedItem(scrapy.Item):
    zone          = scrapy.Field()
    category_name = scrapy.Field()
    category_url  = scrapy.Field()
    asin          = scrapy.Field()
    order_index   = scrapy.Field()
    batch_number  = scrapy.Field()
    create_date   = scrapy.Field()
    update_date   = scrapy.Field()

class AmazonTop100HotNewReleasesItem(scrapy.Item):
    zone          = scrapy.Field()
    category_name = scrapy.Field()
    category_url  = scrapy.Field()
    asin          = scrapy.Field()
    order_index   = scrapy.Field()
    batch_number  = scrapy.Field()
    create_date   = scrapy.Field()
    update_date   = scrapy.Field()

# ############################product listing############################
class amazon_product_baseinfo(scrapy.Item):
    zone            = scrapy.Field()
    asin            = scrapy.Field()
    ref_id          = scrapy.Field()
    # sku             = scrapy.Field()
    seller_name     = scrapy.Field()
    seller_url      = scrapy.Field()
    brand           = scrapy.Field()
    brand_url       = scrapy.Field()
    is_fba          = scrapy.Field()
    stock_situation = scrapy.Field()
    category_name   = scrapy.Field()
    original_price  = scrapy.Field()
    in_sale_price   = scrapy.Field()
    review_cnt      = scrapy.Field()
    review_avg_star = scrapy.Field()
    percent_5_star  = scrapy.Field()
    percent_4_star  = scrapy.Field()
    percent_3_star  = scrapy.Field()
    percent_2_star  = scrapy.Field()
    percent_1_star  = scrapy.Field()
    cnt_qa          = scrapy.Field()
    offers_url      = scrapy.Field()
    lowest_price    = scrapy.Field()
    create_date     = scrapy.Field()
    update_date     = scrapy.Field()

# ############################product listing############################
class amazon_product_base(scrapy.Item):
    zone            = scrapy.Field()
    asin            = scrapy.Field()
    ref_id          = scrapy.Field()
    seller_name     = scrapy.Field()
    seller_url      = scrapy.Field()
    brand           = scrapy.Field()
    brand_url       = scrapy.Field()
    is_fba          = scrapy.Field()
    stock_situation = scrapy.Field()
    category_name   = scrapy.Field()
    original_price  = scrapy.Field()
    in_sale_price   = scrapy.Field()
    review_cnt      = scrapy.Field()
    review_avg_star = scrapy.Field()
    percent_5_star  = scrapy.Field()
    percent_4_star  = scrapy.Field()
    percent_3_star  = scrapy.Field()
    percent_2_star  = scrapy.Field()
    percent_1_star  = scrapy.Field()
    cnt_qa          = scrapy.Field()
    offers_url      = scrapy.Field()
    lowest_price    = scrapy.Field()
    create_date     = scrapy.Field()
    update_date     = scrapy.Field()


class amazon_product_review_percent_info(scrapy.Item):
    asin              = scrapy.Field()
    ref_id            = scrapy.Field()
    review_avg_star   = scrapy.Field()
    percent_5_star    = scrapy.Field()
    percent_4_star    = scrapy.Field()
    percent_3_star    = scrapy.Field()
    percent_2_star    = scrapy.Field()
    percent_1_star    = scrapy.Field()


class amazon_product_technical_details(scrapy.Item):
    zone                  = scrapy.Field()
    asin                  = scrapy.Field()
    ref_id                = scrapy.Field()
    # part_number           = scrapy.Field()
    key_name              = scrapy.Field()
    value_data            = scrapy.Field()
    create_date           = scrapy.Field()
    update_date           = scrapy.Field()

class amazon_product_descriptions(scrapy.Item):
    zone             = scrapy.Field()
    asin             = scrapy.Field()
    ref_id           = scrapy.Field()
    description_type = scrapy.Field()
    desc_content     = scrapy.Field()
    md5_desc_content = scrapy.Field()
    create_date      = scrapy.Field()
    update_date      = scrapy.Field()

class amazon_product_category_sales_rank(scrapy.Item):
    zone          = scrapy.Field()
    asin          = scrapy.Field()
    ref_id        = scrapy.Field()
    category_name = scrapy.Field()
    # category_url  = scrapy.Field()
    sales_rank    = scrapy.Field()
    create_date   = scrapy.Field()
    update_date   = scrapy.Field()

class amazon_product_bought_together_list(scrapy.Item):
    zone                 = scrapy.Field()
    asin                 = scrapy.Field()
    ref_id               = scrapy.Field()
    bought_together_list = scrapy.Field()
    create_date   = scrapy.Field()
    update_date   = scrapy.Field()

class amazon_product_also_bought_list(scrapy.Item):
    zone             = scrapy.Field()
    asin             = scrapy.Field()
    ref_id           = scrapy.Field()
    also_bought_list = scrapy.Field()
    create_date   = scrapy.Field()
    # update_date   = scrapy.Field()

class amazon_product_pictures(scrapy.Item):
    zone        = scrapy.Field()
    asin        = scrapy.Field()
    ref_id      = scrapy.Field()
    img_ind     = scrapy.Field()
    img_url     = scrapy.Field()
    md5_img_url = scrapy.Field()
    create_date = scrapy.Field()
    update_date = scrapy.Field()

class amazon_product_offers(scrapy.Item):
    zone              = scrapy.Field()
    asin              = scrapy.Field()
    ref_id            = scrapy.Field()
    offer_asin        = scrapy.Field()
    offer_price       = scrapy.Field()
    offer_status      = scrapy.Field()
    offer_shipment    = scrapy.Field()
    offer_seller_name = scrapy.Field()
    create_date       = scrapy.Field()
    update_date       = scrapy.Field()

# class amazon_product_sponsored_products_list(scrapy.Item):
#     zone         = scrapy.Field()
#     asin         = scrapy.Field()
#     ref_id       = scrapy.Field()
#     type         = scrapy.Field()
#     product_list = scrapy.Field()
#     create_date  = scrapy.Field()
#     update_date  = scrapy.Field()

class amazon_product_current_reviews(scrapy.Item):
    zone                 = scrapy.Field()
    asin                 = scrapy.Field()
    ref_id               = scrapy.Field()
    review_order_type    = scrapy.Field()
    order_index          = scrapy.Field()
    review_star          = scrapy.Field()
    is_verified_purchase = scrapy.Field()
    votes                = scrapy.Field()
    is_top_reviewer      = scrapy.Field()
    cnt_imgs             = scrapy.Field()
    cnt_vedios           = scrapy.Field()
    create_date          = scrapy.Field()
    update_date          = scrapy.Field()

class amazon_product_review_result(scrapy.Item):
    zone           = scrapy.Field()
    asin           = scrapy.Field()
    result_flag    = scrapy.Field()

class amazon_product_promotions(scrapy.Item):
    zone           = scrapy.Field()
    asin           = scrapy.Field()
    ref_id         = scrapy.Field()
    promotion_list = scrapy.Field()
    md5_promotion  = scrapy.Field()
    create_date    = scrapy.Field()
    update_date    = scrapy.Field()

class amazon_traffic_also_bought(scrapy.Item):
    zone         = scrapy.Field()
    asin         = scrapy.Field()
    ref_id       = scrapy.Field()
    product_list = scrapy.Field()
    create_date  = scrapy.Field()
    update_date  = scrapy.Field()

class amazon_traffic_sponsored_products(scrapy.Item):
    zone         = scrapy.Field()
    asin         = scrapy.Field()
    type         = scrapy.Field()
    ref_id       = scrapy.Field()
    product_list = scrapy.Field()
    create_date  = scrapy.Field()
    update_date  = scrapy.Field()

class amazon_traffic_buy_other_after_view(scrapy.Item):
    zone         = scrapy.Field()
    asin         = scrapy.Field()
    ref_id       = scrapy.Field()
    product_list = scrapy.Field()
    create_date  = scrapy.Field()
    update_date  = scrapy.Field()

class amazon_traffic_similar_items(scrapy.Item):
    zone         = scrapy.Field()
    asin         = scrapy.Field()
    ref_id       = scrapy.Field()
    product_list = scrapy.Field()
    create_date  = scrapy.Field()
    update_date  = scrapy.Field()




# ############################product reviews, reviewer############################
class amazon_product_reviews(scrapy.Item):
    zone                  = scrapy.Field()
    asin                  = scrapy.Field()
    ref_id                = scrapy.Field()
    review_id             = scrapy.Field()
    review_url            = scrapy.Field()
    review_title          = scrapy.Field()
    review_text           = scrapy.Field()
    reviewer_name         = scrapy.Field()
    reviewer_url          = scrapy.Field()
    review_date           = scrapy.Field()
    item_package_quantity = scrapy.Field()
    item_color_size_info  = scrapy.Field()
    order_index           = scrapy.Field()
    review_star           = scrapy.Field()
    is_verified_purchase  = scrapy.Field()
    votes                 = scrapy.Field()
    comments              = scrapy.Field()
    top_reviewer_info     = scrapy.Field()
    cnt_imgs              = scrapy.Field()
    cnt_vedios            = scrapy.Field()
    create_date           = scrapy.Field()
    update_date           = scrapy.Field()


# ############################search relation############################
class amazon_keyword_search_rank(scrapy.Item):
    zone              = scrapy.Field()
    asin              = scrapy.Field()
    ref_id            = scrapy.Field()
    page_index        = scrapy.Field()
    search_rank_index = scrapy.Field()
    sponsored_flag    = scrapy.Field()

class amazon_keyword_search_sponsered(scrapy.Item):
    zone              = scrapy.Field()
    asin              = scrapy.Field()
    ref_id            = scrapy.Field()
    pos_type          = scrapy.Field()
    page_index        = scrapy.Field()
    search_rank_index = scrapy.Field()

# ############################top reviewers############################
class amazon_top_reviewers(scrapy.Item):
    zone              = scrapy.Field()
    ind               = scrapy.Field()
    reviewer_name     = scrapy.Field()
    reviewer_url      = scrapy.Field()
    total_reviews     = scrapy.Field()
    helpful_votes     = scrapy.Field()
    percent_helpful   = scrapy.Field()
    page_index        = scrapy.Field()
    time_id           = scrapy.Field()

# ############################offers catch##############################
class amazon_product_offers_catch(scrapy.Item):
    offer_url  = scrapy.Field()
    file_paths = scrapy.Field()


# ############################test############################
class EtaoItem(object):
    name = scrapy.Field()
    url = scrapy.Field()

class FeedbackItem(scrapy.Item):
    date = scrapy.Field()
    zone = scrapy.Field()
    shop_name = scrapy.Field()
    last_30_days = scrapy.Field()
    last_90_days = scrapy.Field()
    last_12_months = scrapy.Field()
    lifetime = scrapy.Field()
    create_time = scrapy.Field()
    update_time = scrapy.Field()


class TodayDealItem(scrapy.Item):
    deals = scrapy.Field()