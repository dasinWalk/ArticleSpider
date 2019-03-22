# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from fake_useragent import UserAgent
from ArticleSpider.utils.crawl_xici_ip import GetIp
import logging


class ArticlespiderSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class ArticlespiderDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


from selenium import webdriver
from scrapy.http import HtmlResponse
import re


def get_mach_result(value):
    # 判断url是否为图片url
    mach_obj = re.match("(.*?\.png)", value)
    if mach_obj:
        return False
    else:
        mach_obj = re.match("(.*?\.PNG)", value)
        if mach_obj:
            return False
    mach_obj = re.match("(.*?\.gif)", value)
    if mach_obj:
        return False
    else:
        mach_obj = re.match("(.*?\.GIF)", value)
        if mach_obj:
            return False
    return True


class NextPageMiddleware(object):
    #通过chrom请求动态网页
    def process_request(self, request, spider):
        mach_obj = get_mach_result(request.url)
        if spider.name in ['chemy', 'common'] and 'jpg' not in request.url and 'JPG' not in request.url and mach_obj:
            meta = request.meta
            print(meta)
            spider.browser.get(request.url)
            if 'next_year' in meta:
                import time
                time.sleep(1)
                next_year = meta["next_year"]
                index = meta["index"]
                print(next_year)
                try:
                    spider.browser.find_element_by_css_selector('#year-list h2:nth-of-type({0}) '.format(index)).click()
                except Exception as e:
                    print(e)
                    spider.browser.get(request.url)
                    spider.browser.find_element_by_css_selector('#year-list h2:nth-of-type({0}) '.format(index)).click()
            if 'next_month' in meta:
                import time
                time.sleep(1)
                next_month = meta["next_month"]
                print(next_month)
                try:
                    spider.browser.find_element_by_css_selector('#year-list div[style="display: block;"] span:nth-child({0})'.format(next_month)).click()
                except Exception as e:
                    print(e)
                    spider.browser.get(request.url)
                    spider.browser.find_element_by_css_selector(
                        "#year-list div[style='display:block;'] span:nth-child('" + next_month + "') ").click()
            if 'next_page' in meta:
                import time
                time.sleep(1)
                next_page = meta["next_page"]
                print(next_page)
                try:
                    spider.browser.find_element_by_css_selector('#page_article a[data-page="' + next_page + '"]').click()
                except Exception as e:
                    spider.browser.get(request.url)
                    spider.browser.find_element_by_css_selector(
                        '#page_article a[data-page="' + next_page + '"]').click()
            import time
            time.sleep(1)
            return HtmlResponse(url=spider.browser.current_url, body=spider.browser.page_source, encoding="utf-8", request=request)


class RandomUserAgentMiddleware(object):
    #随机更换user-agent
    def __init__(self, crawler):
        super(RandomUserAgentMiddleware, self).__init__()
        self.crawler = crawler
        # self.ua = UserAgent(verify_ssl=False, use_cache_server=False)
        # self.ua.update()
        # self.ua_type = crawler.settings.get("RANDOM_UA_TYPE", "chrome")

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_request(self, request, spider):
        # def get_ua():
        #     return getattr(self.ua, self.ua_type)
        # user_agent = get_ua()
        user_agent_list = self.crawler.settings.get('MY_USER_AGENT')
        import random
        user_agent = random.choice(user_agent_list)
        request.headers.setdefault('User-Agent', user_agent)


class RandomProxyMiddleware(object):
    #动态设置ip代理
    def process_request(self, request, spider):
        get_ip = GetIp()
        request.meta["proxy"] = get_ip.get_random_ip()


class JSPageMiddleware(object):
    #通过chrom请求动态网页
    def process_request(self, request, spider):
        mach_obj = get_mach_result(request.url)

        if spider.name in ["kjjys", 'kjxwzx', 'kjxwzx_gwy', 'kxjsb_yw', 'gjzrkx', 'zgkxb', 'gjypjg',
                           'science', 'pharmnet', 'common'] and 'jpg' not in request.url and 'JPG' not in request.url and mach_obj:
            spider.browser.get(request.url)
            import time
            time.sleep(2)
            return HtmlResponse(url=spider.browser.current_url, body=spider.browser.page_source, encoding="utf-8", request=request)


class BlankPageMiddleware(object):
    def process_response(self, request, response, spider):
        text = response.text
        # regx = ".*?<body>(.*)?</body>.*"
        # match_obj = re.match(regx, text)
        if text.__len__() < 100:
            logging.info("重试" + text)
            import time
            time.sleep(2)
            return self._retry(request, "retry", spider) or response
        return response

    def _retry(self, request, reason, spider):
        retryreq = request.copy()
        retryreq.dont_filter = True
        retryreq.priority = request.priority
        return retryreq
