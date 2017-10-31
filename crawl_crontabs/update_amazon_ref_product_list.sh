#! /bin/sh

export PATH=$PATH:/home/zz/anaconda3/bin
source activate python27
cd /home/zz/projects/amazonFrontCrawl/amazonFrontCrawl/db/

NOW_DATE=$(date +%Y-%m-%d)

nohup python update_amazon_ref_product_list.py >> /home/zz/projects/crawl_logs/update_amazon_ref_product_list/$NOW_DATE.log 2>&1 &

