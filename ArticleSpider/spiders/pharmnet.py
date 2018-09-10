# -*- coding: utf-8 -*-
import scrapy
import re
import requests
from scrapy.http import Request
from urllib import parse
from ArticleSpider.items import PharmnetItem, PharmnetItemLoader
from selenium import webdriver
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from ArticleSpider.utils.common import get_md5
import datetime
import platform
from pyvirtualdisplay import Display


def remove_comment_tags(value):
    # 去掉tags中提取的评论
    if len(value) > 1:
        return value[1]
    else:
        return value[0]


class PharmnetSpider(scrapy.Spider):
    name = 'pharmnet'
    allowed_domains = ['www.pharmnet.com.cn']
    start_urls = ['https://portal.gdc.cancer.gov/repository?facetTab=cases&filters=%7B%22op%22%3A%22and%22%2C%22content%22%3A%5B%7B%22op%22%3A%22in%22%2C%22content%22%3A%7B%22field%22%3A%22cases.primary_site%22%2C%22value%22%3A%5B%22Cervix%22%5D%7D%7D%2C%7B%22op%22%3A%22in%22%2C%22content%22%3A%7B%22field%22%3A%22cases.project.project_id%22%2C%22value%22%3A%5B%22TCGA-CESC%22%5D%7D%7D%2C%7B%22op%22%3A%22in%22%2C%22content%22%3A%7B%22field%22%3A%22files.experimental_strategy%22%2C%22value%22%3A%5B%22RNA-Seq%22%5D%7D%7D%5D%7D&searchTableTab=cases']

    headers = {
        "HOST": "www.pharmnet.com.cn",
        "Referer": "http://www.pharmnet.com.cn/search/template/yljg_index.htm",
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36"
    }

    def __init__(self, **kwargs):
        sysstr = platform.system()
        if sysstr == 'Windows':
            self.browser = webdriver.Chrome(executable_path="E:/pythonDriver/chromedriver.exe")
        else:
            # self.browser = webdriver.Chrome(executable_path="/root/software/pydriver/chromedriver")
            self.display = Display(visible=0, size=(800, 600))
            self.display.start()
            self.browser = webdriver.Chrome(executable_path="/usr/bin/chromedriver")
        super(PharmnetSpider, self).__init__()
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
        print(response.text)
        post_nodes = response.css(".bian221 .state li")
        for post_node in post_nodes:
            province = post_node.css("a ::text").extract_first("")
            if province in ['吉林', '广西', '江西', '上海']:
                post_url = post_node.css("a::attr(href)").extract_first("")
                yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers, meta={"province": province}, callback=self.parse_province)

    def parse_province(self, response):
        post_nodes = response.css(".city li")
        province = response.meta.get("province", "")
        for post_node in post_nodes:
            city = post_node.css("a ::text").extract_first("")
            cityname = re.findall(r'[^()]+', city)[0]
            hospitalct = re.findall(r'[^()]+', city)[1]
            post_url = post_node.css("a::attr(href)").extract_first("")
            yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers,
                          meta={"province": province, "cityname": cityname, "hospitalct": hospitalct},
                          callback=self.parse_city)

    def parse_city(self, response):
        province = response.meta.get("province", "")
        cityname = response.meta.get("cityname", "")
        hospitalct = response.meta.get("hospitalct", "")
        hospitalct = int(hospitalct)
        page = int(response.meta.get("page", "1"))
        pages = (hospitalct / 15) + 1

        post_nodes = response.css(".bian221 strong")
        for post_node in post_nodes:
            post_url = post_node.css("a::attr(href)").extract_first("")
            yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers,
                          meta={"province": province, "cityname": cityname, "hospitalct": hospitalct},
                          callback=self.parse_detail)
        if page <= pages:
            p = page + 1
            next_url = ''
            if 'p=' in response.url:
                next_url = response.url.replace('p='+str(page), 'p='+str(p))
            else:
                next_url = response.url + '&p=' + str(p)
            print(next_url)
            yield Request(url=parse.urljoin(response.url, next_url), headers=self.headers,
                          meta={"province": province, "cityname": cityname, "hospitalct": hospitalct, "page": p},
                          callback=self.parse_city)

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
        province = response.meta.get("province", "")
        cityname = response.meta.get("cityname", "")
        contentNodes = response.css("#fontsize tr:nth-child(2)")
        fieldNodes = contentNodes.css("tbody tr")
        # 通过item loader加载item
        #医院名称
        hospital_name = fieldNodes[0].css("td:nth-child(2) ::text").extract_first("")
        #等级
        grade = fieldNodes[1].css("td:nth-child(2) ::text").extract_first("")
        #类型
        type = fieldNodes[2].css("td:nth-child(2) ::text").extract_first("")
        #是否医保定点
        ifInsurance = fieldNodes[3].css("td:nth-child(2) ::text").extract_first("")
        # 病床数
        beds_num = fieldNodes[4].css("td:nth-child(2) ::text").extract_first("")
        #门诊量
        outpatient = fieldNodes[5].css("td:nth-child(2) ::text").extract_first("")
        #地址
        address = fieldNodes[6].css("td:nth-child(2) ::text").extract_first("")
        #邮编
        zipcode = fieldNodes[7].css("td:nth-child(2) ::text").extract_first("")
        #联系电话
        telephone = fieldNodes[8].css("td:nth-child(2) ::text").extract_first("")
        #网址
        net_work = fieldNodes[9].css("td:nth-child(2) ::text").extract_first("")
        #乘车路线
        bus_line = fieldNodes[10].css("td:nth-child(2) ::text").extract_first("")
        #主要设备
        equipment = fieldNodes[11].css("td:nth-child(2) ::text").extract_first("")
        #特色专科
        specialties = fieldNodes[12].css("td:nth-child(2) ::text").extract_first("")
        #医院介绍
        hospital_introduce = fieldNodes[13].css("td:nth-child(2) ::text").extract_first("")

        item_loader = PharmnetItemLoader(item=PharmnetItem(), response=response)
        item_loader.add_value("url", response.url)
        item_loader.add_value("url_object_id", get_md5(response.url))
        item_loader.add_value("province", province)
        item_loader.add_value("city_name", cityname)
        item_loader.add_value("hospital_name", hospital_name)
        item_loader.add_value("grade", grade)
        item_loader.add_value("type", type)
        item_loader.add_value("ifInsurance", ifInsurance)
        item_loader.add_value("beds_num", beds_num)
        item_loader.add_value("outpatient", outpatient)
        item_loader.add_value("address", address)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("telephone", telephone)
        item_loader.add_value("net_work", net_work)
        item_loader.add_value("bus_line", bus_line)
        item_loader.add_value("equipment", equipment)
        item_loader.add_value("specialties", specialties)
        item_loader.add_value("hospital_introduce", hospital_introduce)
        item_loader.add_value("crawl_time", datetime.datetime.now())
        article_item = item_loader.load_item()

        yield article_item
