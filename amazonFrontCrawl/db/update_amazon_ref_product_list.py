# -*- coding: utf-8 -*-

import os
import sys
import MySQLdb
# MySQL configuration

def update_amazon_ref_product_list():
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
        "delete from amazon_ref_product_list"
    )

    cursor.execute(
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
        "  from amazon_product_insales a"
        " where a.time_id = (select max(time_id) as time_id from amazon_product_insales)"
    )
    conn.commit()
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
    update_amazon_ref_product_list()

