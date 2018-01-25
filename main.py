# coding:utf-8

from scrapy import cmdline
from datetime import datetime

print(datetime.now())
# cmdline.execute("scrapy crawlall ".split())

# cmdline.execute("scrapy crawl getShopProducts".split()) # run test result : success

# cmdline.execute("scrapy crawl product".split()) # run test result : success

# cmdline.execute("scrapy crawl ProductReviewSpider".split()) # run test result : success

cmdline.execute("scrapy crawl product_listing".split()) # run test result : success

# cmdline.execute("scrapy crawl KeywordSearchSpider".split())

# cmdline.execute("scrapy crawl top100Series".split())

# cmdline.execute("scrapy crawl AmazonTopReviewer -a zone=ca".split())

# cmdline.execute("scrapy crawl TodayDealsSpider".split())

# cmdline.execute("scrapy crawl OffersCatchSpider".split())

# cmdline.execute("scrapy crawl feedback".split())

print(datetime.now())