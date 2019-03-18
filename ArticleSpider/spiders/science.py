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


def get_my_content(url, value):
    new_content = []
    articleorign = '<p><a href="{0}" target="_blank"><span style="color: #0070c0; text-decoration: underline;">' \
                   '原文链接</span></a></p></div>'.format(url)
    for cvalue in value:
        cvalue = cvalue[:-6]
        cvalue = cvalue + articleorign
        new_content.append(cvalue)
    return new_content


def remove_comment_tags(value):
    # 去掉tags中提取的评论
    if len(value) > 1:
        return value[1]
    else:
        return value[0]


# 科学网 头条和要闻 数据爬取
class ScienceSpider(scrapy.Spider):
    name = 'science'
    allowed_domains = ['news.sciencenet.cn']
    start_urls = ['http://news.sciencenet.cn/']

    headers = {
        "HOST": "www.news.sciencenet.cn",
        "Referer": "http://news.sciencenet.cn/",
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
        super(ScienceSpider, self).__init__()
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
        if response.url in ['http://news.sciencenet.cn/topnews.aspx','http://news.sciencenet.cn/indexyaowen.aspx']:
            post_nodes = response.xpath("//*[@id='mleft3']/table/tbody/tr/td/table/tbody/tr[last()]")
            type_name = response.css("#mleft2 ::text").extract_first("")
            type_name = type_name.strip()
            for post_node in post_nodes:
                post_url = post_node.css("a::attr(href)").extract_first("")
                title = post_node.css(" a::text").extract_first("")
                publish_date = post_node.xpath("td[3]/text()").extract_first("")
                publish_date = publish_date.strip()
                publish_date = publish_date.replace('/', '-')
                compare_date = datetime.datetime.now()
                compare_date = datetime.datetime.strptime(compare_date.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S").date()
                publishDate = datetime.datetime.strptime(publish_date, "%Y-%m-%d %H:%M:%S").date()
                if publishDate >= compare_date:
                    print("=======get it==========")
                    yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers, meta={"publish_date": publish_date, "type_name": type_name, "title": title}, callback=self.parse_detail)
        else:
            post_nodes = response.css(".ltitbg a")
            for post_node in post_nodes[0:3]:
                post_url = post_node.css("::attr(href)").extract_first("")
                print("out===url")
                print(post_url)
                yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers, callback=self.parse)

    def parse_detail(self, response):

        type_name = response.meta.get("type_name", "")
        publish_date = response.meta.get("publish_date", "")  # 发布时间
        item_loader = kjjysItemLoader(item=kjjysItem(), response=response)
        image_url = response.css("#content1 img::attr(src)").extract()
        title = response.meta.get("title", "")
        title = title.strip()

        content = response.css("#content1").extract()
        content = get_my_content(response.url, content)
        content = "".join(content)

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
        item_loader.add_value("source_name", '科学网')
        item_loader.add_value("type_name", type_name)
        item_loader.add_value("title", title)
        item_loader.add_value("content", content)

        item_loader.add_value("publish_time", publish_date)
        item_loader.add_value("crawl_time", datetime.datetime.now())
        article_item = item_loader.load_item()

        yield article_item
