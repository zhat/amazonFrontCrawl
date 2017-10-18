# -*- coding: utf-8 -*-

class StringUtilTool(object):

    @staticmethod
    def getZoneFromUrl(url):
        if '.com/' in url:
            return 'us'
        else:
            return 'Unkhnow'

    @staticmethod
    def getFbaFromStr(str):
        if str == 'Fulfilled by Amazon':
            return '1'
        else:
            return '0'

    @staticmethod
    def zone_to_domain(zone):
        switcher = {
            'US': 'https://www.amazon.com',
            'UK': 'https://www.amazon.com.uk',
            'DE': 'https://www.amazon.amazon.de',
            'JP': 'https://www.amazon.jp',
            'CA': 'https://www.amazon.ca',
            'ES': 'https://www.amazon.es',
            'IT': 'https://www.amazon.it',
            'FR': 'https://www.amazon.fr',

        }
        return switcher.get(zone, 'error zone')

    @staticmethod
    def domain_to_zone(domain):
        switcher = {
            'amazon.com': 'US',
            'com.uk': 'UK',
            'amazon.de': 'DE',
            'amazon.jp': 'JP',
            'amazon.ca': 'CA',
            'amazon.es': 'ES',
            'amazon.it': 'IT',
            'amazon.fr': 'FR',

        }
        return switcher.get(domain, 'error domain')

    @staticmethod
    def generate_keyword_search_url(keyword_info):

        keyword = keyword_info[0].encode('utf-8').strip().replace(' ', '+')
        zone = keyword_info[1].encode('utf-8').strip()

        if zone == 'us':
            return 'https://www.amazon.com/s/ref=nb_sb_noss?url=search-alias%3Daps&field-keywords=' + keyword
        else:
            return 'Nothing'

    @staticmethod
    def clean_str(str_t):
        return str_t.replace('\n', '').replace('\t', '').strip().encode('utf-8')