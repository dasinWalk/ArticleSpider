# -*- coding: utf-8 -*-
import scrapy
import re
import requests
import math
import json
from scrapy.http import Request
from urllib import parse
from selenium import webdriver
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from ArticleSpider.utils.common import get_md5, get_list, get_atextname, get_next_page_node, get_total_number
from ArticleSpider.items import CommonItem, CommonItemLoader
from ArticleSpider.utils.mysql import Mysql
import datetime
import platform
from pyvirtualdisplay import Display


def remove_comment_tags(value):
    # 去掉tags中提取的评论
    if len(value) > 1:
        return value[1]
    else:
        return value[0]


class CommonSpider(scrapy.Spider):
    name = 'common'
    allowed_domains = ['www.pharmnet.com.cn']
    start_urls = []
    url_map = {}
    url_list = []
    node_map = {}
    node_list = []

    next_map = {}
    next_list = []

    field_map = {}
    db_map = {}


    headers = {
        "HOST": "www.pharmnet.com.cn",
        "Referer": "http://www.pharmnet.com.cn/search/template/yljg_index.htm",
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36"
    }

    def __init__(self, **kwargs):
        sysstr = platform.system()
        valuemap = json.loads(kwargs['value'])
        self.url_map = valuemap['urlMap']
        self.url_list = get_list(self.url_map)
        self.node_map = valuemap['nodeMap']
        self.node_list = get_list(self.node_map)

        self.next_map = valuemap['nextMap']
        self.next_list = get_list(self.next_map)

        self.field_map = valuemap['fieldMap']
        self.db_map = valuemap['dbMap']
        self.page_map = valuemap['pageMap']

        Mysql.create_table_by_name(Mysql(), table_name=self.db_map['tableName'])

        self.start_urls.append(self.url_list.pop(0))
        if sysstr == 'Windows':
            self.browser = webdriver.Chrome(executable_path="E:/pythonDriver/chromedriver.exe")
        else:
            # self.browser = webdriver.Chrome(executable_path="/root/software/pydriver/chromedriver")
            self.display = Display(visible=0, size=(800, 600))
            self.display.start()
            self.browser = webdriver.Chrome(executable_path="/usr/bin/chromedriver")
        super(CommonSpider, self).__init__()
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
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

        start_node = self.node_list.pop(0)
        post_nodes = response.xpath(start_node)
        for post_node in post_nodes:
            astart = post_node.css("a ::text").extract_first("")
            post_url = post_node.css("a::attr(href)").extract_first("")
            if astart == '山东':
                yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers, meta={"astart": astart},
                              callback=self.parse_middle)
        #是否获取了下一页的节点
        next_node = response.meta.get("next_node", "")
        if next_node == '':
            next_node = self.next_list[0]
        if next_node != '':
            islast = response.meta.get("islast", "")
            if islast != '0':
                post_nodes = response.xpath(next_node)
                islast = '0'
                node_len = len(post_nodes)
                if node_len > 1:
                    for post_node in post_nodes[1:]:
                        if post_nodes[node_len-1] == post_node:
                            islast = '1'
                        post_url = post_node.css("a::attr(href)").extract_first("")
                        yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers,
                                      meta={"next_node": next_node, "islast": islast}, callback=self.parse)

    def parse_middle(self, response):
        middle_count_json = response.meta.get("middle_count_json", "")
        middle_count_map = {}
        if middle_count_json == '':
            middle_count_map[get_atextname(len(middle_count_map))] = response.meta.get("astart", "")
        else:
            middle_count_map = json.loads(middle_count_json)
        if len(self.node_list) != len(middle_count_map):
            # 进入下一个节点(第二，第三.............)
            node_count = len(middle_count_map)
            next_node = self.node_list[node_count-1]
            post_nodes = response.xpath(next_node)
            for post_node in post_nodes:
                next_text = post_node.css("a ::text").extract_first("")
                middle_count_map[get_atextname(node_count)] = next_text
                post_url = post_node.css("a::attr(href)").extract_first("")
                middle_ct_json = json.dumps(middle_count_map)
                yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers,
                              meta={"middle_count_json": middle_ct_json}, callback=self.parse_middle)
            next_page_node = response.meta.get(get_next_page_node(node_count), "")
            if next_page_node == '':
                next_page_node = self.next_list[node_count]
            if next_page_node != '':
                islast = response.meta.get("islast", "")
                last_page = response.meta.get("last_page", 0)
                if islast != '0':
                    next_page_nodes = response.xpath(next_page_node).css("a::attr(href)").extract()
                    next_page_texts = response.xpath(next_page_node).css("a ::text").extract()
                    if len(next_page_nodes) > 1:
                        if '下一页' in next_page_texts:
                            post_url = next_page_nodes[0]
                            middle_ct_json = json.dumps(middle_count_map)
                            next_node_param = get_next_page_node(node_count)
                            islast = 1
                            yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers,
                                          meta={"middle_count_json": middle_ct_json, next_node_param: next_page_node,
                                                "islast": islast}, callback=self.parse_middle)
                    else:
                        page_key = str(node_count) + "_per_page"
                        detail_number = self.page_map[page_key]
                        total_key = str(node_count) + "_total_node"
                        total_page_node = self.page_map[total_key]
                        total_page = 0
                        if total_page_node != '':
                            total_count = response.xpath(total_page_node + "/text()").extract_first("0")
                            total_count = get_total_number(total_count)
                            total_page = math.ceil(total_count / detail_number)
                        else:
                            total_page_key = str(node_count) + "_total_page_node"
                            total_page_node = self.page_map[total_page_key]
                            total_page_extract = response.xpath(total_page_node + "/text()").extract_first("0")
                            total_page = get_total_number(total_page_extract)
                        islast = '0'
                        page_len = len(next_page_nodes)
                        start = 0
                        if islast == '1':
                            start = 1
                        for next_node in next_page_nodes[start:]:
                            post_url = next_node
                            middle_ct_json = json.dumps(middle_count_map)
                            next_node_param = get_next_page_node(node_count)
                            if next_page_nodes[page_len-1] == next_node:
                                islast = '1'
                                last_page = last_page + page_len
                            else:
                                islast = '0'
                            last_page_count = 0
                            if last_page == total_page:
                                if total_count > 0:
                                    last_page_count = total_count % detail_number
                            yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers,
                                          meta={"middle_count_json": middle_ct_json, next_node_param: next_page_node,
                                                "last_page_count": last_page_count, "last_page": last_page,
                                                "islast": islast}, callback=self.parse_middle)
        else:
            next_node = self.node_list[len(middle_count_map)-1]
            if next_node != '':
                post_nodes = response.xpath(next_node).css("a::attr(href)").extract()
                middle_count_json = json.dumps(middle_count_map)
                middle_ct_map = json.loads(middle_count_json)
                page_key = str(len(middle_ct_map)) + "_per_page"
                detail_number = self.page_map[page_key]
                last_page_count = response.meta.get("last_page_count", 0)
                if last_page_count > 0:
                    detail_number = last_page_count
                for post_node in post_nodes[0:detail_number]:
                    post_url = post_node
                    yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers,
                                  meta={"middle_count_json": middle_count_json}, callback=self.parse_detail)
                #最后一页 获取下一页
                next_page_node = response.meta.get("next_page_detail", "")
                if next_page_node == '':
                    next_page_node = self.next_list[len(middle_ct_map)]
                if next_page_node != '':
                    islast = response.meta.get("islast", "")
                    last_page = response.meta.get("last_page", 0)
                    if islast != '0':
                        next_page_nodes = response.xpath(next_page_node).css("a::attr(href)").extract()
                        next_page_texts = response.xpath(next_page_node).css("a ::text").extract()
                        if len(next_page_nodes) > 1:
                            if '下一页' in next_page_texts:
                                post_url = next_page_nodes[0]
                                middle_ct_json = json.dumps(middle_count_map)
                                next_node_param = get_next_page_node(node_count)
                                islast = 1
                                yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers,
                                              meta={"middle_count_json": middle_ct_json,
                                                    next_node_param: next_page_node,
                                                    "islast": islast}, callback=self.parse_middle)
                        else:
                            total_key = str(len(middle_ct_map)) + "_total_node"
                            total_page_node = self.page_map[total_key]
                            total_page = 0
                            if total_page_node != '':
                                total_count = response.xpath(total_page_node + "/text()").extract_first("0")
                                total_count = get_total_number(total_count)
                                total_page = math.ceil(total_count / detail_number)
                            else:
                                total_page_key = str(len(middle_ct_map)) + "_total_page_node"
                                total_page_node = self.page_map[total_page_key]
                                total_page_extract = response.xpath(total_page_node + "/text()").extract_first("0")
                                total_page = get_total_number(total_page_extract)
                            page_len = len(next_page_nodes)
                            start = 0
                            if islast == '1':
                                start = 1
                            for next_node in next_page_nodes[start:]:
                                post_url = next_node
                                middle_ct_json = json.dumps(middle_count_map)
                                if next_page_nodes[page_len - 1] == next_node:
                                    islast = '1'
                                    last_page = last_page + page_len
                                else:
                                    islast = '0'
                                last_page_count = 0
                                if last_page == total_page:
                                    if total_count > 0:
                                        last_page_count = total_count % detail_number
                                yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers,
                                              meta={"middle_count_json": middle_ct_json, "last_page_count": last_page_count,
                                                    "islast": islast, "last_page": last_page,
                                                    "next_page_detail": next_page_node}, callback=self.parse_middle)

    def parse_detail(self, response):
        # 提取文章的具体字段
        # title = response.xpath('//div[@class="entry-header"]/h1/text()').extract_first("")
        # create_date = response.xpath("//p[@class='entry-meta-hide-on-mobile']/text()").extract()[0].strip().replace("·","").strip()
        # praise_nums = response.xpath("//span[contains(@class, 'vote-post-up')]/h10/text()").extract()[0]
        # fav_nums = response.xpath("//span[contains(@class, 'bookmark-btn')]/text()").extract()[0]
        # match_re = re.match(".*?(\d+).*", fav_nums)
        # if match_re:
        #     fav_nums = match_re.group(1)
        #
        # comment_nums = response.xpath("//a[@href='#article-comment']/span/text()").extract()[0]
        # match_re = re.match(".*?(\d+).*", comment_nums)
        # if match_re:
        #     comment_nums = match_re.group(1)
        #
        # content = response.xpath("//div[@class='entry']").extract()[0]
        #
        # tag_list = response.xpath("//p[@class='entry-meta-hide-on-mobile']/a/text()").extract()
        # tag_list = [element for element in tag_list if not element.strip().endswith("评论")]
        # tags = ",".join(tag_list)

        # 通过css选择器提取字段
        # front_image_url = response.meta.get("front_image_url", "")  #文章封面图
        # title = response.css(".entry-header h1::text").extract()[0]
        # create_date = response.css("p.entry-meta-hide-on-mobile::text").extract()[0].strip().replace("·","").strip()
        # praise_nums = response.css(".vote-post-up h10::text").extract()[0]
        # fav_nums = response.css(".bookmark-btn::text").extract()[0]
        # match_re = re.match(".*?(\d+).*", fav_nums)
        # if match_re:
        #     fav_nums = int(match_re.group(1))
        # else:
        #     fav_nums = 0
        #
        # comment_nums = response.css("a[href='#article-comment'] span::text").extract()[0]
        # match_re = re.match(".*?(\d+).*", comment_nums)
        # if match_re:
        #     comment_nums = int(match_re.group(1))
        # else:
        #     comment_nums = 0
        #
        # content = response.css("div.entry").extract()[0]
        #
        # tag_list = response.css("p.entry-meta-hide-on-mobile a::text").extract()
        # tag_list = [element for element in tag_list if not element.strip().endswith("评论")]
        # tags = ",".join(tag_list)
        #
        # article_item["url_object_id"] = get_md5(response.url)
        # article_item["title"] = title
        # article_item["url"] = response.url
        # try:
        #     create_date = datetime.datetime.strptime(create_date, "%Y/%m/%d").date()
        # except Exception as e:
        #     create_date = datetime.datetime.now().date()
        # article_item["create_date"] = create_date
        # article_item["front_image_url"] = [front_image_url]
        # article_item["praise_nums"] = praise_nums
        # article_item["comment_nums"] = comment_nums
        # article_item["fav_nums"] = fav_nums
        # article_item["tags"] = tags
        # article_item["content"] = content
        middle_count_json = response.meta.get("middle_count_json", "")
        middle_ct_map = json.loads(middle_count_json)
        title = '-'
        content = '-'
        for mitem in self.field_map:
            field_node = self.field_map[mitem]
            fleld_value = response.xpath(field_node+"/text()").extract_first("")
            if mitem == 'title':
                title = fleld_value
            elif mitem == 'content':
                content = fleld_value
            else:
                middle_ct_map[mitem] = fleld_value
        if "title" in middle_ct_map:
            middle_ct_map.pop("title")
        if "content" in middle_ct_map:
            middle_ct_map.pop("content")
        item_loader = CommonItemLoader(item=CommonItem(), response=response)
        item_loader.add_value("url", response.url)
        item_loader.add_value("id", get_md5(response.url))
        item_loader.add_value("title", title)
        item_loader.add_value("content", content)
        item_loader.add_value("crawl_time", datetime.datetime.now())
        middle_ct_json = json.dumps(middle_ct_map)
        db_map_json = json.dumps(self.db_map)
        item_loader.add_value("fieldMap", middle_ct_json)
        item_loader.add_value("dbMap", db_map_json)
        article_item = item_loader.load_item()

        yield article_item
