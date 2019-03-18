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
import platform
from pyvirtualdisplay import Display


def get_real_date(value):
    # 去掉tags中提取的评论
    if len(value) > 1:
        return value[1].strip()
    else:
        return value[0].strip()

# 中华人民共和国国家卫生健康委员会数据爬取 现在网址已变更为http://www.nhc.gov.cn/ 需要重新修改爬虫逻辑
class kjxwzx_gwySpider(scrapy.Spider):
    name = 'kjxwzx_gwy'
    allowed_domains = ['www.gov.cn']
    start_urls = ['http://www.gov.cn/pushinfo/v150203/index.htm']

    headers = {
        "HOST": "http://www.gov.cn/",
        "Referer": "http://www.gov.cn/pushinfo/v150203/index.htm",
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36"
    }

    def __init__(self, **kwargs):
        sysstr = platform.system()
        if sysstr == 'Windows':
            self.browser = webdriver.Chrome(executable_path="E:/pythonDriver/chromedriver.exe")
        else:
            self.display = Display(visible=0, size=(800, 600))
            self.display.start()
            self.browser = webdriver.Chrome(executable_path="/usr/bin/chromedriver")
        super(kjxwzx_gwySpider, self).__init__()
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self,spider):
        #当爬虫退出时关闭chrom
        print("spider closed")
        sysstr = platform.system()
        if sysstr == 'Windows':
            self.browser.quit()
        else:
            self.browser.quit()
            self.display.stop()

    def parse(self, response):
        """
                1. 获取文章列表页中的文章url并交给scrapy下载后并进行解析
                2. 获取下一页的url并交给scrapy进行下载， 下载完成后交给parse
                """
        # 解析列表页中的所有文章url并交给scrapy下载后并进行解析
        if response.status == 404:
            self.fail_urls.append(response.url)
            self.crawler.stats.inc_value("failed_url")
        post_nodes = response.css(".list_2 ul li")
        # type_name = response.css(".tit_s01 .tabg ::text").extract_first("")
        type_name = response.css(".channel_tab .noline a::text").extract_first("")
        type_name = type_name.strip()
        for post_node in post_nodes:
            post_url = post_node.css("a::attr(href)").extract_first("")
            publish_date = post_node.css("span ::text").extract()
            publish_date = get_real_date(publish_date)
            print("innqerurl ===")
            print(response.url)
            compare_date = datetime.datetime.now()
            compare_date = datetime.datetime.strptime(compare_date.strftime("%Y-%m-%d"), "%Y-%m-%d").date()
            publishDate = datetime.datetime.strptime(publish_date, "%Y-%m-%d").date()
            if publishDate >= compare_date:
                title = post_node.css("a::text").extract_first("")
                yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers,
                              meta={"publish_date": publish_date, "type_name": type_name, "title": title}, callback=self.parse_detail)

        # 提取下一页并交给scrapy进行下载
        # next_url = response.css(".next.page-numbers::attr(href)").extract_first("")
        if response.url == "http://www.nhfpc.gov.cn/qjjys/new_index.shtml1":
            yield Request(url=response.url, headers=self.headers, callback=self.parse)

    def parse_detail(self, response):
        # 通过item loader加载item
        title = response.meta.get("title", "")
        type_name = response.meta.get("type_name", "")
        publish_date = response.meta.get("publish_date", "")  # 发布时间
        item_loader = kjjysItemLoader(item=kjjysItem(), response=response)

        image_url = response.css("#UCAP-CONTENT img::attr(src)").extract()
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
        item_loader.add_value("source_name", '中华人民共和国国家卫生健康委员会')
        item_loader.add_value("type_name", type_name)
        item_loader.add_value("title", title)
        item_loader.add_xpath("content", "//*[@id='UCAP-CONTENT']")

        item_loader.add_value("publish_time", publish_date)
        item_loader.add_value("crawl_time", datetime.datetime.now())
        article_item = item_loader.load_item()

        yield article_item
