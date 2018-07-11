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


def remove_comment_tags(value):
    # 去掉tags中提取的评论
    if len(value) > 1:
        return value[1]
    else:
        return value[0]


class KjjysSpider(scrapy.Spider):
    name = 'kjjys'
    allowed_domains = ['www.nhfpc.gov.cn']
    start_urls = ['http://www.nhfpc.gov.cn/qjjys/new_index.shtml']

    headers = {
        "HOST": "www.nhfpc.gov.cn",
        "Referer": "http://www.nhfpc.gov.cn/qjjys/new_index.shtml",
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36"
    }

    def __init__(self):
        self.browser = webdriver.Chrome(executable_path="E:/pythonDriver/chromedriver.exe")
        super(KjjysSpider, self).__init__()
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
        #, 'http://www.nhfpc.gov.cn/qjjys/zcwj2/new_zcwj.shtml', 'http://www.nhfpc.gov.cn/qjjys/pgzdt/new_list.shtml'
        if response.url in ['http://www.nhfpc.gov.cn/qjjys/pqt/new_list.shtml']:
            #post_nodes = response.css(".zxxx_list a")
            post_nodes = response.css(".zxxx_list li")
            type_name = response.css(".index_title_h3.fl ::text").extract_first("")
            for post_node in post_nodes:
                post_url = post_node.css("a::attr(href)").extract_first("")
                publish_date = post_node.css("span ::text").extract()
                publish_date = remove_comment_tags(publish_date)
                print("innerurl ===")
                print(response.url)
                # compare_date = '2018-06-21'
                # compare_date = datetime.datetime.strptime(compare_date, "%Y-%m-%d").date()
                # publishDate = datetime.datetime.strptime(publish_date, "%Y-%m-%d").date()
                # if publishDate > compare_date:
                yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers, meta={"publish_date": publish_date, "type_name": type_name}, callback=self.parse_detail)
        else:
            post_nodes = response.css(".fr.index_more a")
            for post_node in post_nodes[0:3]:
                post_url = post_node.css("::attr(href)").extract_first("")
                print("out===url")
                print(post_url)
                yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers, callback=self.parse)

        # 提取下一页并交给scrapy进行下载
        # next_url = response.css(".next.page-numbers::attr(href)").extract_first("")
        if response.url == "http://www.nhfpc.gov.cn/qjjys/new_index.shtml1":
            yield Request(url=response.url, headers=self.headers, callback=self.parse)

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

        # 通过item loader加载item
        article_item = kjjysItem()

        type_name = response.meta.get("type_name", "")
        publish_date = response.meta.get("publish_date", "")  # 发布时间
        item_loader = kjjysItemLoader(item=kjjysItem(), response=response)

        image_url = response.css("#xw_box img::attr(src)").extract()
        new_image_url = ['http://wx3.sinaimg.cn/mw690/7cc829d3gy1fsrtjp2o93j20hs0audih.jpg']
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
        item_loader.add_value("source_name", '科技教育司')
        item_loader.add_value("type_name", type_name)
        item_loader.add_css("title", "div.tit ::text")
        item_loader.add_xpath("content", "//*[@id='xw_box']/p")
        item_loader.add_value("publish_time", publish_date)
        item_loader.add_value("crawl_time", datetime.datetime.now())
        article_item = item_loader.load_item()

        yield article_item
