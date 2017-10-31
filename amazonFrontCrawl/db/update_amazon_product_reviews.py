# -*- coding: utf-8 -*-
import sys
import os
import MySQLdb

def update_amazon_product_reviews():
    user = MYSQL_USER
    passwd = MYSQL_PASSWD
    db = MYSQL_DBNAME
    host = MYSQL_HOST
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
        " insert into amazon_product_reviews_total ("
"  zone "
", asin "
", source_id "
", ref_id "
", review_url "
", review_id "
", review_title "
", review_text "
", reviewer_name "
", reviewer_url "
", review_date "
", item_package_quantity "
", item_color_size_info "
", order_index "
", review_star "
", is_verified_purchase "
", votes "
", comments "
", top_reviewer_info "
", cnt_imgs "
", cnt_vedios "
", create_date "
", update_date )"
        " select "
"  zone "
", asin "
", id"
", ref_id "
", review_url "
", review_id "
", review_title "
", review_text "
", reviewer_name "
", reviewer_url "
", substr(review_date,1,10) as review_date "
", item_package_quantity "
", item_color_size_info "
", order_index "
", review_star "
", is_verified_purchase "
", votes "
", comments "
", top_reviewer_info "
", cnt_imgs "
", cnt_vedios "
", create_date "
", update_date "
        "from amazon_product_reviews a "
        "where not exists (select 1 from amazon_product_reviews_total b where a.review_id = b.review_id)"
    )

    cursor.execute(
        " insert into amazon_product_reviews_dels ("
        "  zone "
        ", asin "
        ", source_id"
        ", ref_id "
        ", review_url "
        ", review_id "
        ", review_title "
        ", review_text "
        ", reviewer_name "
        ", reviewer_url "
        ", review_date "
        ", item_package_quantity "
        ", item_color_size_info "
        ", order_index "
        ", review_star "
        ", is_verified_purchase "
        ", votes "
        ", comments "
        ", top_reviewer_info "
        ", cnt_imgs "
        ", cnt_vedios )"
        " select "
        "  zone "
        ", asin "
        ", source_id"
        ", ref_id "
        ", review_url "
        ", review_id "
        ", review_title "
        ", review_text "
        ", reviewer_name "
        ", reviewer_url "
        ", review_date "
        ", item_package_quantity "
        ", item_color_size_info "
        ", order_index "
        ", review_star "
        ", is_verified_purchase "
        ", votes "
        ", comments "
        ", top_reviewer_info "
        ", cnt_imgs "
        ", cnt_vedios "
        "from amazon_product_reviews_total a "
        "where not exists (select 1 from amazon_product_reviews b where a.review_id = b.review_id)"
    )

    cursor.execute(
        "delete from amazon_product_reviews"
    )

    conn.close()

# execute

if __name__=='__main__':
    base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    print(base_path)
    sys.path.append(base_path)
    from amazonFrontCrawl import settings
    # MySQL configuration
    MYSQL_HOST = settings.MYSQL_HOST
    MYSQL_DBNAME = settings.MYSQL_DBNAME
    MYSQL_USER = settings.MYSQL_USER
    MYSQL_PASSWD = settings.MYSQL_PASSWD
    MYSQL_PORT = settings.MYSQL_PORT

    update_amazon_product_reviews()

