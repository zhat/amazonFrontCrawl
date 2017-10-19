#! /bin/sh

export PATH=$PATH:/usr/local/bin

cd /usr/projects/spider_project/amazonFrontCrawl

NOW_DATE=$(date +%Y-%m-%d-%H-%M)

nohup /root/anaconda2/bin/scrapy crawl getShopProducts >> /usr/projects/spider_project/crawl_logs/getShopProducts/$NOW_DATE.log 2>&1 &

