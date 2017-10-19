#! /bin/sh

export PATH=$PATH:/usr/local/bin

NOW_DATE=$(date +%Y-%m-%d-%H-%M)

cd /usr/projects/spider_project/amazonFrontCrawl/amazonFrontCrawl/db/
nohup /root/anaconda2/bin/python update_amazon_product_reviews.py >> /usr/projects/spider_project/crawl_logs/update_amazon_product_reviews/$NOW_DATE.log 2>&1 &


