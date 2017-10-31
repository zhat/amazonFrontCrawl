#! /bin/sh

export PATH=$PATH:/home/zz/anaconda3/bin
source activate python27

cd /home/zz/projects/amazonFrontCrawl
NOW_DATE=$(date +%Y-%m-%d)

nohup scrapy crawl feedback >> /home/zz/projects/crawl_logs/feedback/$NOW_DATE.log 2>&1 &d