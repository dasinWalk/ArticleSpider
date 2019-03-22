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
from ArticleSpider.utils.common import get_md5, get_list, get_atextname, get_next_page_node,\
    get_total_number, get_real_date, get_now_date
from ArticleSpider.items import CommonItem, CommonItemLoader
from ArticleSpider.utils.mysql import Mysql
import datetime
import platform
from pyvirtualdisplay import Display
from scrapy_redis.spiders import RedisSpider
import logging
from ArticleSpider.utils.nativemysql import NativeMysql
from ArticleSpider.utils.RedisUtils import RedisHelper
from retrying import retry

obj = RedisHelper()


def remove_comment_tags(value):
    # 去掉tags中提取的评论
    if len(value) > 1:
        return value[1]
    else:
        return value[0]


def update_crawl_status(crawl_id, task_id, failed_num, success_num):
    if failed_num is None:
        failed_num = 0
    if success_num is None:
        success_num = 0
    total_page = failed_num + success_num
    logging.info("start crawl data ===failed_num" + task_id + str(failed_num))
    logging.info("start crawl data ===success_num" + task_id + str(success_num))
    #redis中统计爬虫任务爬虫记录数
    obj.hash_inrc_key("'"+task_id+"'", 'failed_num', failed_num)
    obj.hash_inrc_key("'"+task_id+"'", 'success_num', success_num)
    try:
        sql = """
                  update scrapy_task set status = '1',history = '1', crawl_num = %s, lost_num = %s, 
                  total_page = %s where id = %s
              """
        mysql2 = NativeMysql()
        params = (success_num, failed_num, total_page, crawl_id)
        mysql2.update(sql, param=params)
        #查询该任务的所有爬虫信息
        query_sql = """
                        select status from scrapy_task WHERE history = '0' and task_id = %s  for update
                    """
        query_param = (task_id, )
        cursor = mysql2.getAll(query_sql, param=query_param)
        if not cursor:
            # 更新crawl_record， crawl_task
            uptask_sql = """
                             update crawl_task set task_status = 3 where id = %s
                           """
            record_sql = """
                         update crawl_record set crawl_num = %s, in_db_num = %s, end_time = %s where task_id = %s
                     """
            task_total_success = obj.get_hash_value("'"+task_id+"'", 'success_num')
            if task_total_success is None:
                task_total_success = 0
            task_total_fail = obj.get_hash_value("'" + task_id + "'", 'failed_num')
            if task_total_fail is None:
                task_total_fail = 0
            total_page = int(task_total_success) + int(task_total_fail)
            update_param = (total_page, task_total_success, get_now_date(), task_id)
            mysql2.update(uptask_sql, param=query_param)
            mysql2.update(record_sql, param=update_param)
            obj.del_key(task_id)
            obj.del_key(task_id + 'dog')
            obj.remove_hash("'" + task_id + "'", {'failed_num', 'success_num'})
    except Exception as e:
        print(e)


class CommonSpider(RedisSpider):#scrapy.Spider
    name = 'common'
    crawl_id = None
    task_id = None
    allowed_domains = []
    start_urls = []
    url_map = {}
    url_list = []
    node_map = {}
    node_list = []

    next_map = {}
    next_list = []

    field_map = {}
    field_type = {}
    db_map = {}

    headers = {
        "HOST": "www.pharmnet.com.cn",
        "Referer": "http://www.pharmnet.com.cn/search/template/yljg_index.htm",
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36"
    }

    def __init__(self, param=None, *args, **kwargs):#param=None,
        sysstr = platform.system()
        if param is not None:
            param_json = param
        else:
            param_json = kwargs['value']
        logging.info("===" + param_json)
        valuemap = json.loads(param_json)
        if 'crawl_id' in valuemap:
            self.crawl_id = valuemap['crawl_id']
        self.url_map = valuemap['urlMap']
        self.url_list = get_list(self.url_map)
        self.node_map = valuemap['nodeMap']
        self.node_list = get_list(self.node_map)

        self.next_map = valuemap['nextMap']
        self.next_list = get_list(self.next_map)

        self.field_map = valuemap['fieldMap']
        self.field_type = valuemap['fieldType']
        self.db_map = valuemap['dbMap']
        self.page_map = valuemap['pageMap']
        #创建表 初始化表字段
        task_id = self.db_map['task_id']
        #删除redis中缓存的任务信息 start
        obj.del_key(task_id)
        obj.del_key(task_id + 'dog')
        obj.del_key(task_id + 'node_list')
        if obj.if_exist_key(task_id, 'success_num'):
            obj.remove_hash("'" + task_id + "'", {'success_num'})
        if obj.if_exist_key(task_id, 'failed_num'):
            obj.remove_hash("'" + task_id + "'", {'failed_num'})
        obj.set_value(task_id)
        # 添加看门狗，第一个item时初始化字段用
        obj.set_value(task_id + 'dog')
        # 删除redis中缓存的任务信息 end
        self.task_id = task_id
        task_value = obj.inc_value(task_id)
        if task_value == 1:
            Mysql.create_table_by_name(Mysql(self.db_map), db_map=self.db_map, field_map=self.field_map)
        # self.allowed_domains.append(self.url_list[1])
        if sysstr == 'Windows':
            self.browser = webdriver.Chrome(executable_path="E:/pythonDriver/chromedriver.exe")
        else:
            # self.browser = webdriver.Chrome(executable_path="/root/software/pydriver/chromedriver")
            self.display = Display(visible=0, size=(800, 600))
            self.display.start()
            self.browser = webdriver.Chrome(executable_path="/usr/bin/chromedriver")
        super(CommonSpider, self).__init__()
        self.browser.implicitly_wait(20)
        try:
            dispatcher.connect(self.spider_closed, signals.spider_closed)
        except Exception as e:
            print("获取redis urls 异常")
            print(e)

    def spider_closed(self, spider):
        #当爬虫退出时关闭chrom
        logging.info("spider closed")
        sysstr = platform.system()
        success_num = self.crawler.stats.get_value('success_url')
        failed_num = self.crawler.stats.get_value('failed_url')
        update_crawl_status(self.crawl_id, self.db_map['task_id'], failed_num, success_num)
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
        # 该函数为多个爬虫竞争 进去的函数
        logging.info("start crawl data ===0" + response.url)
        p_node_list = obj.get_value(self.task_id + 'node_list')
        if p_node_list is None:
            p_node_list = self.node_list
        else:
            p_node_list = json.loads(p_node_list)
        start_node = p_node_list.pop(0) #将第一步节点xpath 取出
        # 将点击xpath路径信息 存入redis
        obj.set_list(self.task_id + 'node_list', json.dumps(p_node_list))
        post_nodes = response.xpath(start_node)
        last_page_count = response.meta.get("last_page_count", 0)
        try:
            for post_node in post_nodes:
                astart = post_node.css("a ::text").extract_first("")
                post_url = post_node.css("a::attr(href)").extract_first("")
                yield Request(url=parse.urljoin(response.url, post_url), meta={"astart": astart},
                              callback=self.parse_middle)
        except Exception as e:
            print(e)
        #是否获取了下一页的节点
        next_node = response.meta.get("next_node", "")
        if next_node == '' and len(self.next_list) > 0:
                next_node = self.next_list[0]
        if next_node != '':
            islast = response.meta.get("islast", "")
            last_page = response.meta.get("last_page", 0)
            if islast != '0':
                next_page_nodes = response.xpath(next_node).css("a::attr(href)").extract()
                next_page_texts = response.xpath(next_node).css("a ::text").extract()
                if '首页' in next_page_texts:
                    if '下一页' in next_page_texts:
                        next_index = next_page_texts.index('下一页')
                        post_url = next_page_nodes[next_index]
                        islast = 1
                        yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers,
                                      meta={"next_node": next_node, "islast": islast}, callback=self.parse)
                else:
                    last_page = response.meta.get("last_page", 0)
                    detail_number = self.page_map["0_per_page"]
                    total_page_node = self.page_map["0_total_node"]
                    total_page = 0
                    if total_page_node != '':
                        total_count = response.xpath(total_page_node + "/text()").extract_first("0")
                        total_count = get_total_number(total_count)
                        total_page = math.ceil(total_count / detail_number)
                    else:
                        total_page_key = "0_total_page_node"
                        total_page_node = self.page_map[total_page_key]
                        total_page_extract = response.xpath(total_page_node + "/text()").extract_first("0")
                        total_page = get_total_number(total_page_extract)
                    islast = '0'
                    page_len = len(next_page_nodes)
                    start = 0
                    if islast == '1':
                        start = 1
                    for next_url in next_page_nodes[start:]:
                        post_url = next_url
                        if next_page_nodes[page_len - 1] == next_url:
                            islast = '1'
                            last_page = last_page + page_len
                        else:
                            islast = '0'
                        last_page_count = 0
                        if last_page == total_page:
                            if total_count > 0:
                                last_page_count = total_count % detail_number
                        yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers,
                                      meta={"next_node": next_node, "last_page_count": last_page_count,
                                            "last_page": last_page, "islast": islast}, callback=self.parse)

    def parse_middle(self, response):
        middle_count_json = response.meta.get("middle_count_json", "")
        middle_count_map = {}
        if middle_count_json == '':
            # atext = "第一步点击热链接的内容 如 国务院文件" 键值对
            middle_count_map[get_atextname(len(middle_count_map))] = response.meta.get("astart", "")
        else:
            middle_count_map = json.loads(middle_count_json)
        p_node_list_key = self.task_id + 'node_list'
        logging.info("start crawl data 3===p_node_list_key" + p_node_list_key)
        # 从redis 中取出扒取步骤对应的xpath 信息
        p_node_list = obj.get_value(p_node_list_key)
        if p_node_list is None:
            p_node_list = self.node_list
        else:
            p_node_list = json.loads(p_node_list)
        logging.info("start crawl data 3===p_node_list" + json.dumps(p_node_list))
        logging.info("start crawl data 3===middle_count_map" + json.dumps(middle_count_map))
        if len(p_node_list) != len(middle_count_map):
            # 进入下一个节点(第二，第三.............)
            node_count = len(middle_count_map)
            next_node = p_node_list[node_count-1]
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
                    if '首页' in next_page_texts:
                        if '下一页' in next_page_texts:
                            next_index = next_page_texts.index('下一页')
                            post_url = next_page_nodes[next_index]
                            middle_ct_json = json.dumps(middle_count_map)
                            next_node_param = get_next_page_node(node_count)
                            islast = 1
                            yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers,
                                          meta={"middle_count_json": middle_ct_json, next_node_param: next_page_node,
                                                "islast": islast}, callback=self.parse_middle)
                    else:
                        page_key = str(node_count) + "_per_page"
                        if page_key in self.page_map:
                            detail_number = self.page_map[page_key]
                        else:
                            detail_number = len(next_page_nodes)
                        total_key = str(node_count) + "_total_node"
                        total_page_node = ''
                        if total_key in self.page_map:
                            total_page_node = self.page_map[total_key]
                        total_page = 0
                        if total_page_node != '':
                            total_count = response.xpath(total_page_node + "/text()").extract_first("0")
                            total_count = get_total_number(total_count)
                            total_page = math.ceil(total_count / detail_number)
                        else:
                            logging.info("get-total-page" + str(node_count))
                            total_page_key = str(node_count) + "_total_page"
                            total_page = self.page_map[total_page_key]
                            # total_page_key = str(node_count) + "_total_page_node"
                            # total_page_node = self.page_map[total_page_key]
                            # total_page_extract = response.xpath(total_page_node + "/text()").extract_first("0")
                            # total_page = get_total_number(total_page_extract)
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
            #next_node = self.node_list[len(middle_count_map)-1]
            next_node = p_node_list[len(middle_count_map) - 1]
            logging.info("start crawl data ===4 status" + str(response.status))
            logging.info("start crawl data ===4next_node" + next_node)
            if next_node != '':
                post_nodes = response.xpath(next_node).css("a::attr(href)").extract()
                middle_count_json = json.dumps(middle_count_map)
                middle_ct_map = json.loads(middle_count_json)
                node_count = len(middle_ct_map)
                page_key = str(node_count) + "_per_page"
                if page_key in self.page_map:
                    detail_number = self.page_map[page_key]
                else:
                    detail_number = len(post_nodes)
                last_page_count = response.meta.get("last_page_count", 0)
                if last_page_count > 0:
                    detail_number = last_page_count
                for post_node in post_nodes[0:detail_number]:
                    post_url = post_node
                    logging.info("detail" + post_url)
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
                        if '首页' in next_page_texts:
                            if '下一页' in next_page_texts:
                                next_index = next_page_texts.index('下一页')
                                post_url = next_page_nodes[next_index]
                                middle_ct_json = json.dumps(middle_count_map)
                                islast = 1
                                yield Request(url=parse.urljoin(response.url, post_url), headers=self.headers,
                                              meta={"middle_count_json": middle_ct_json,
                                                    "next_page_detail": next_page_node,
                                                    "islast": islast}, callback=self.parse_middle)
                        else:
                            total_key = str(len(middle_ct_map)) + "_total_node"
                            total_page_node = ''
                            if total_key in self.page_map:
                                total_page_node = self.page_map[total_key]
                            total_page = 0
                            if total_page_node != '':
                                total_count = response.xpath(total_page_node + "/text()").extract_first("0")
                                total_count = get_total_number(total_count)
                                total_page = math.ceil(total_count / detail_number)
                            else:
                                logging.info("get-total-page1" + str(len(middle_ct_map)))
                                total_page_key = str(len(middle_ct_map)) + "_total_page"
                                total_page = self.page_map[total_page_key]
                                # total_page_key = str(len(middle_ct_map)) + "_total_page_node"
                                # total_page_node = self.page_map[total_page_key]
                                # total_page_extract = response.xpath(total_page_node + "/text()").extract_first("0")
                                # total_page = get_total_number(total_page_extract)
                            logging.info("start crawl data 4===middle_count_map" + json.dumps(next_page_nodes))
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
                                              meta={"middle_count_json": middle_ct_json,
                                                    "last_page_count": last_page_count,
                                                    "islast": islast, "last_page": last_page,
                                                    "next_page_detail": next_page_node}, callback=self.parse_middle)

    @retry
    def parse_detail(self, response):
        logging.info("start crawl data ===5")
        if response.status == 404:
            self.fail_urls.append(response.url)
            self.crawler.stats.inc_value("failed_url")

        def is_empty(fv_map):
            is_em = True
            for fd in self.field_map:
                if fd in fv_map and fv_map[fd] != '':
                    is_em = False
                    break
            return is_em
        middle_count_json = response.meta.get("middle_count_json", "")
        middle_ct_map = json.loads(middle_count_json)
        title = '-'
        content = '-'
        #middle_ct_map 记录动态添加的字段和字段对应的值 如果要扒取的字段有title和content则 不处理
        for mitem in self.field_map:
            field_node = self.field_map[mitem]
            field_value = response.xpath(field_node+"/text()").extract_first("").strip()
            if field_value == '':
                data = response.xpath(field_node)
                field_value = data.xpath('string(.)').extract_first("").strip()
            if mitem == 'title':
                title = field_value.strip()
            elif mitem == 'content':
                if field_value == '':
                    field_value = response.xpath(field_node).extract_first("").strip()
                    content = field_value
            else:
                if mitem in self.field_type and self.field_type[mitem] == 'date':
                    field_value = get_real_date(field_value)
                middle_ct_map[mitem] = field_value
        if ("title" in middle_ct_map and title == '') | ("content" in middle_ct_map
                                                         and content == '') | (is_empty(middle_ct_map)):
            self.crawler.stats.inc_value("failed_url")
        else:
            self.crawler.stats.inc_value("success_url")

        if "title" in middle_ct_map:
            middle_ct_map.pop("title")
        if "content" in middle_ct_map:
            middle_ct_map.pop("content")
        if title == '':
            title = '-'
        if content == '':
            content = '-'

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