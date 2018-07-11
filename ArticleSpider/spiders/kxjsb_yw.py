# -*- coding: utf-8 -*-
import scrapy
import re
import requests
from scrapy.http import Request
from urllib import parse
from ArticleSpider.items import kjjysItem, kjjysItemLoader
from selenium import webdriver
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from ArticleSpider.utils.common import get_md5
import datetime


def get_real_date(value):
    # 去掉日期外面的括号
    if len(value) > 1:
        date = value[1].strip()
        date = date.replace("(", "")
        date = date.replace(")", "")
        return date
    else:
        date = value[0].strip()
        date = date.replace("(", "")
        date = date.replace(")", "")
        return date


class kxjsb_ywSpider(scrapy.Spider):
    name = 'kxjsb_yw'
    allowed_domains = ['www.most.gov.cn']
    start_urls = ['http://www.most.gov.cn/yw/index.htm']

    headers = {
        "HOST": "http://www.most.gov.cn/",
        "Referer": "http://www.most.gov.cn/yw/index.htm",
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36"
    }

    def __init__(self):
        self.browser = webdriver.Chrome(executable_path="E:/pythonDriver/chromedriver.exe")
        super(kxjsb_ywSpider, self).__init__()
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self,spider):
        #当爬虫退出时关闭chrom
        print("spider closed")
        self.browser.quit()

    def parse(self, response):
        """
                1. 获取文章列表页中的文章url并交给scrapy下载后并进行解析
                2. 获取下一页的url并交给scrapy进行下载， 下载完成后交给parse
                """
        # 解析列表页中的所有文章url并交给scrapy下载后并进行解析
        if response.status == 404:
            self.fail_urls.append(response.url)
            self.crawler.stats.inc_value("failed_url")
        post_nodes = response.css("#push_table")
        # type_name = response.css(".tit_s01 .tabg ::text").extract_first("")
        type_name = '要闻'
        for post_node in post_nodes:
            post_url = post_node.css("a::attr(href)").extract_first("")
            publish_date = post_node.css(".STYLE30 ::text").extract()
            publish_date = get_real_date(publish_date)
            print("innqerurl ===")
            print(response.url)
            compare_date = datetime.datetime.now()
            compare_date = datetime.datetime.strptime(compare_date.strftime("%Y-%m-%d"), "%Y-%m-%d").date()
            publishDate = datetime.datetime.strptime(publish_date, "%Y-%m-%d").date()
            if publishDate >= compare_date:
                yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers,
                              meta={"publish_date": publish_date, "type_name": type_name}, callback=self.parse_detail, dont_filter=True)

        # 提取下一页并交给scrapy进行下载
        # next_url = response.css(".next.page-numbers::attr(href)").extract_first("")
        if response.url == "http://www.most.gov.cn/yw/index.htm":
            next_url = 'http://www.most.gov.cn/yw/index_10032.htm'
            special_url = 'http://www.most.gov.cn/yw/yw_push_2j.htm'
            yield Request(url=next_url, headers=self.headers, callback=self.parse, dont_filter=True)
            yield Request(url=special_url, headers=self.headers, callback=self.parse, dont_filter=True)

    def parse_detail(self, response):
        # 通过item loader加载item
        type_name = response.meta.get("type_name", "")
        publish_date = response.meta.get("publish_date", "")  # 发布时间
        item_loader = kjjysItemLoader(item=kjjysItem(), response=response)
        image_url = response.css("#UCAP-CONTENT img::attr(src)").extract()
        title = response.css(".h-news .h-title ::text").extract()
        result = ""
        content = response.css(".main-aticle").extract_first("")
        if title:
            result = "".join(title)
            result = result.strip()
            print(result)
        else:
            title = response.css(".article.oneColumn.pub_border h1::text").extract()
            if title:
                result = "".join(title)
                result = result.strip()
                content = response.css("#UCAP-CONTENT p").extract()
                content = "".join(content)
                print(result)
            else:
                title = response.css(".bd1 tr:nth-child(3) td:nth-child(2) ::text").extract()
                content = response.css("#UCAP-CONTENT").extract_first("")
                result = "".join(title)
                result = result.strip()
                print(result)

        new_image_url = []
        if len(image_url) > 0:
            for in_url in image_url:
                in_url = parse.urljoin(response.url, in_url)
                new_image_url.append(in_url)
        else:
            item_loader.add_value("front_image_path", '--')

        item_loader.add_value("url", response.url)
        item_loader.add_value("url_object_id", get_md5(response.url))
        if len(new_image_url) > 0:
            item_loader.add_value("front_image_url", new_image_url)
        # else:
        #     item_loader.add_value("front_image_url", [""])
        item_loader.add_value("source_net", self.start_urls[0])
        item_loader.add_value("source_name", '中华人民共和国科学技术部')
        item_loader.add_value("type_name", type_name)
        item_loader.add_value("title", result)
        item_loader.add_value("content", content)

        item_loader.add_value("publish_time", publish_date)
        item_loader.add_value("crawl_time", datetime.datetime.now())
        article_item = item_loader.load_item()

        yield article_item
