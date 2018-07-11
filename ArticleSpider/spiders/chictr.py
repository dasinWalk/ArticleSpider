# -*- coding: utf-8 -*-
import scrapy
import re
import requests
from scrapy.http import Request
from urllib import parse
from ArticleSpider.utils.common import get_md5
import datetime
from scrapy.selector import Selector
from ArticleSpider.items import ChictrItem

class chictrSpider(scrapy.Spider):
    name = "chictr"
    allowed_domains = ['www.chictr.org.cn']
    start_urls = ['http://www.chictr.org.cn/searchproj.aspx?officialname=%E5%B9%B2%E7%BB%86%E8%83%9E&btngo=btn&page=1']

    def parse(self, response):
        #获取干细胞查询列表内容
        post_nodes = response.css(".table_list tr")
        for post_node in post_nodes[1:]:
            detail_url = post_node.css("a::attr(href)").extract()[1]
            project_name = post_node.css("a::text").extract()[1]
            url = parse.urljoin(response.url, detail_url)
            yield Request(url, meta={"project_name": project_name}, callback= self.parse_detail,  dont_filter=True)
        #提取下一页的数据
        alist = response.css(".pagearea a")
        for a_node in alist:
            next_content = a_node.css("::text").extract()
            if next_content[0] == "下一页":
                next_page = a_node.css("::attr(onclick)").extract_first("")
                match_obj = re.match('.*(\d+)', next_page)
                if match_obj:
                    next_page = match_obj.group(1)
                    next_url = "http://www.chictr.org.cn/searchproj.aspx?officialname=%E5%B9%B2%E7%BB%86%E8%83%9E&btngo=btn&page={0}".format(next_page)
                    yield Request(url=parse.urljoin(response.url, next_url), callback=self.parse)


    def parse_detail(self, response):
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36"}
        res = requests.get(response.url, headers=headers)
        selector = Selector(text=res.text)
        file_url = selector.css(".ProjetInfo_title a::attr(href)").extract()[0]
        file_url = parse.urljoin(response.url, file_url)
        project_name = response.meta.get("project_name", "")
        chictrItem = ChictrItem()
        chictrItem["projectName"] = project_name
        chictrItem['file_urls'] = [file_url]
        yield chictrItem