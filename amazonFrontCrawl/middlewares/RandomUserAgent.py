# coding:utf-8

import random
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
import logging

'''
这个类主要用于产生随机UserAgent
'''

class RandomUserAgentMiddleware(object):

    def __init__(self,agents):
        self.agents = agents

    @classmethod
    def from_crawler(cls,crawler):
        return cls(crawler.settings.getlist('USER_AGENTS'))

    def process_request(self,request,spider):
        logging.info("random choose user agent ......")
        tmp_agent = random.choice(self.agents)
        print "**************UserAgentMiddleware choosing agent************" + tmp_agent
        request.headers.setdefault('User-Agent', tmp_agent)