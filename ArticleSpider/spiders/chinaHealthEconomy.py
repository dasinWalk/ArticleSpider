# -*- coding: utf-8 -*-
import scrapy
import re
import requests
from ArticleSpider.utils.PostParam import ChinaHealthEconomy
from scrapy.http import Request
from urllib import parse
from ArticleSpider.utils.common import get_list_to_text
from selenium import webdriver
from scrapy.selector import Selector
from ArticleSpider.items import ChemyItem
from scrapy import signals
import platform
from pyvirtualdisplay import Display
from scrapy.xlib.pydispatch import dispatcher
from ArticleSpider.utils.RedisUtils import RedisHelper
import json
obj = RedisHelper()

# 万方数据知识服务平台 中国经济周刊数据爬取 现在已爬取了2018，2017，2016，2015，2014年的数据 后续如有需要可以再爬取
class chinaHealthEconomy(scrapy.Spider):
    name = "chemy"
    allowed_domains = ['wanfangdata.com.cn']
    start_urls = ['http://www.wanfangdata.com.cn/magazine/paper.do?perio_id=zgwsjj']


    headers = {
        "HOST": "wanfangdata.com.cn",
        "Referer": "http://www.wanfangdata.com.cn/perio/detail.do?perio_id=zgwsjj",
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
        super(chinaHealthEconomy, self).__init__()
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
        #获取干细胞查询列表内容
        #month = response.css(".JoufArticleList input[id='issue_num'] ::attr(value)").extract()[0]
        print(response.text)
        post_nodes = response.css(".featureColumns #year-list h2 ")
        k = 1
        for post_node in post_nodes[1:]:
            k = k + 1
            year = post_node.css("::text").extract_first("")
            next_year_url = response.url + '&year=' + year
            print(next_year_url)
            if(year == '2013'):
                yield Request(url=next_year_url, headers=self.headers,
                              meta={"next_year": year, "index": k}, callback=self.parse_year)


    def parse_year(self, response):
        print("invoke parse year method")
        #获取干细胞查询列表内容
        #month = response.css(".JoufArticleList input[id='issue_num'] ::attr(value)").extract()[0]
        post_nodes = response.css("#year-list div[style='display: block;'] span ")
        year = response.meta.get("next_year", "")
        index = response.meta.get("index", 1)
        for post_node in post_nodes[0:]:
            month = post_node.css("::text").extract_first("")
            next_month_url = response.url + '&month=' + month
            yield Request(url=next_month_url, headers=self.headers,
                          meta={"next_year": year, "index": index, "next_month": month}, callback=self.parse_month)

    def parse_month(self, response):
        print("invoke parse month method")
        #获取干细胞查询列表内容
        post_nodes = response.css("#artical .JourArticleLi tr[class='listItem'] ")
        year = response.css("#year-list .open ::text").extract_first()
        meta_year = response.meta.get("next_year", "")
        print("meta_year ="+ meta_year)
        for post_node in post_nodes[0:]:
            detail_url = post_node.css("a::attr(href)").extract()[0]
            project_name = post_node.css("a::text").extract()[0]
            url = parse.urljoin(response.url, detail_url)
            yield Request(url, meta={"project_name": project_name, "year": year}, callback=self.parse_detail, dont_filter=True)
        #提取下一页的数据
        alist = response.css("#page_article a")
        month = response.css(".JoufArticleList input[id='issue_num'] ::attr(value)").extract()[0]

    def parse_detail(self, response):
        print(response.url)
        selector = Selector(text=response.text)
        # main_content = response.css(".content").extract_first()
        main_content = response.css(".left_con_top")
        title_cn = main_content.css(".title ::text").extract_first("")
        title_cn = title_cn.strip()
        title_en = main_content.css(".English ::text").extract_first("")
        title_en = title_en.strip()
        art_num = obj.inc_value('art_num')
        abstract = main_content.css("#see_alldiv ::text").extract()
        abstract_con = main_content.css(".abstract textarea ::text").extract()
        field_map = {}
        if len(abstract)>0 and len(abstract_con)>0:
            field_map[abstract[0]] = str(abstract_con[0])
        info = main_content.css(".info")
        li_nodes = info.css("li ")
        for li in li_nodes:
            li_one = li.css("div:nth-child(1) ::text").extract_first()
            li_one = li_one[0:-1]
            if '在线出版日期' in li_one or '页数' in li_one or '页码' in li_one:
                li_two = li.css("div:nth-child(2) ::text").extract_first()
                li_two = li_two.strip()
                field_map[li_one] = li_two
            elif '分类号' in li_one:
                li_two = li.css("div:nth-child(2) ::text").extract_first()
                li_two = li_two.strip()
                li_two = li_two.replace('\n', '').replace('\t', '')
                field_map[li_one] = li_two
            else:
                li_two = li.css("div:nth-child(2) a::text").extract()
                li_two = get_list_to_text(li_two)
                field_map[li_one] = li_two
        project_name = response.meta.get("project_name", "")
        year = response.meta.get("year", "")
        chemyItem = ChemyItem()
        chemyItem["art_num"] = art_num
        chemyItem["title_cn"] = title_cn
        chemyItem["title_en"] = title_en
        chemyItem["field_map"] = field_map
        chemyItem["year"] = year
        yield chemyItem
