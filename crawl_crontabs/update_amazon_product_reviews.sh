#! /bin/sh

export PATH=$PATH:/home/zz/anaconda3/bin
source activate python27
cd /home/zz/projects/amazonFrontCrawl/amazonFrontCrawl/db/

NOW_DATE=$(date +%Y-%m-%d)

nohup python update_amazon_product_reviews.py >> /home/zz/projects/crawl_logs/update_amazon_product_reviews/$NOW_DATE.log 2>&1 &

