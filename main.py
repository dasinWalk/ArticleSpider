import os
import sys
import re
import json
import math
from scrapy.cmdline import execute
# from threading import Timer
# from ArticleSpider.utils.task import start_worker
# from scrapy.crawler import CrawlerRunner
# from scrapy.utils.log import configure_logging
# from ArticleSpider.spiders.commonSpider import CommonSpider
# from twisted.internet import reactor, defer
# from scrapy.utils.project import get_project_settings
# import multiprocessing
# from fake_useragent import UserAgent

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# execute(["scrapy","crawl","jobbole"])
# execute(["scrapy", "crawl", "chictr"])
# execute(["scrapy", "crawl", "kjjys"])
# execute(["scrapy", "crawl", "kjxwzx"])
# execute(["scrapy", "crawl", "kjxwzx_gwy"])
# execute(["scrapy", "crawl", "kxjsb_yw"])
# execute(["scrapy", "crawl", "gjzrkx"])
# execute(["scrapy", "crawl", "zgkxb"])
# execute(["scrapy", "crawl", "gjypjg"])
# execute(["scrapy", "crawl", "science"])
# execute(["scrapy", "crawl", "pharmnet"])
# execute(["scrapy", "crawl", "chemy"])

#中国医药网
# url_map = {}
# next_map = {}
# field_map = {}
# db_map = {}
# node_map = {}
# page_map = {}
# field_type = {}
# url_map['start_url'] = 'http://www.pharmnet.com.cn/search/template/yljg_index.htm'
# url_map['domain'] = 'www.pharmnet.com.cn'
# url_map['third_url'] = 'http://www.pharmnet.com.cn/search/index.cgi?c1=19&c2=%C9%BD%B6%AB&city=%C7%E0%B5%BA&cate1=%D2%BD%D4%BA%2F%C9%BD%B6%AB'
#
# node_map['start_node'] = '/html/body/table[5]/tbody/tr/td[3]/table/tbody/tr[3]/td/table/tbody/tr/td/table[3]/tbody/tr[2]/td/div/ul/li'
# node_map['second_node'] = '/html/body/table[5]/tbody/tr/td[3]/table/tbody/tr[2]/td/table/tbody/tr/td/table[1]'
# node_map['third_node'] = '/html/body/table[5]/tbody/tr/td[3]/table/tbody/tr[2]/td/table/tbody/tr/td/table[position()>1]'
#
# next_map['a_url_next'] = ''
# next_map['b_url_next'] = ''
# next_map['c_url_next'] = '/html/body/table[5]/tbody/tr/td[3]/table/tbody/tr[2]/td/table/tbody/tr/td/table[33]/tbody/tr/td/span'
# #1:mysql 2:oracle 3:mongo 4 hdfs
# db_map['type'] = '1'
# db_map['tableName'] = 'web_site_data1'
# db_map['task_id'] = '001002'
#
# page_map['2_per_page'] = 15
# page_map['2_total_page'] = 30
# page_map['2_total_count'] = 450
# page_map['2_total_node'] = '/html/body/table[5]/tbody/tr/td[3]/table/tbody/tr[2]/td/table/tbody/tr/td/table[33]/tbody/tr/td/span/font[1]'
#
# field_map['hospital_name'] = '//*[@id="fontsize"]/tbody/tr[2]/td/table/tbody/tr[1]/td[2]'
# field_map['grade'] = '//*[@id="fontsize"]/tbody/tr[2]/td/table/tbody/tr[2]/td[2]'
# field_map['type'] = '//*[@id="fontsize"]/tbody/tr[2]/td/table/tbody/tr[3]/td[2]'
# field_map['bed_nums'] = '//*[@id="fontsize"]/tbody/tr[2]/td/table/tbody/tr[5]/td[2]'

#只定义起始的url，后续的url可以根据配置节点获取
# url_map['start_url'] = 'http://www.nhfpc.gov.cn/qjjys/new_index.shtml'
# url_map['domain'] = 'www.nhfpc.gov.cn'
#
# node_map['start_node'] = '/html/body/div[2]/div[1]/div[1]/div/span'
# node_map['second_node'] = '/html/body/div[2]/div[2]/ul'
#
# next_map['a_url_next'] = ''
# #直接指定下一页的节点，翻页的话节点位置会发生变化
# next_map['b_url_next'] = '//*[@id="page_div"]'
# #1:mysql 2:oracle 3:mongo 4 hdfs
# db_map['type'] = '1'
# db_map['tableName'] = 'web_site_data2'
# db_map['task_id'] = '001002'
# db_map['host'] = '127.0.0.1'
# db_map['port'] = 3306
# db_map['dbName'] = 'rdf'
# db_map['userName'] = 'root'
# db_map['password'] = 'skq123123'
#
# page_map['1_per_page'] = 24
# page_map['1_total_page'] = 30
# page_map['1_total_count'] = 450
# page_map['1_total_node'] = ''
# page_map['1_total_page_node'] = '//*[@id="page_div"]/div[10]'
#
# field_map['publish_time'] = '/html/body/div[2]/div[2]/div[2]/span'
# field_map['content'] = '//*[@id="xw_box"]'
#
# field_type['publish_time'] = 'date'
# field_type['content'] = 'text'

url_map = {}
next_map = {}
field_map = {}
db_map = {}
node_map = {}
page_map = {}
field_type = {}
url_map['start_url'] = 'http://www.nhc.gov.cn/'
url_map['domain'] = 'www.nhc.gov.cn'

# 第一步点击的节点xpath 路径 http://www.nhc.gov.cn/ 国务院文件 现在前端获取的xpath路径有问题 需要修改
node_map['start_node'] = "//html/body/div[@class='index_bg']/div[@class='w1180']/div[@class='inConbot']/div[@class='fl slideTxtBox dttzTab']/div[@class='bd']/div[1]/ul[@class='menu']/li[2]/a"
# 点击进去 定位扒取列表xpath
node_map['second_node'] = "//html/body/div[@class='w1024 mb50']/div[@class='list']/ul[@class='zxxx_list']"
# node_map['third_node'] = "//html/body/div[@class='w1024 mb50']/div[@class='list']/ul[@class='zxxx_list']/li[1]/a" 详情列表热链接 xpath不需要

# 定义翻页信息
next_map['a_url_next'] = ''
# 直接指定下一页的节点，翻页的话节点位置会发生变化
next_map['b_url_next'] = "//html/body/div[@class='w1024 mb50']/div[@class='list']/div[@class='pagediv']"
# 1:mysql 2:oracle 3:mongo 4 hdfs
db_map['type'] = '1'
db_map['tableName'] = 'web_site_data12'
db_map['task_id'] = '40287e81699e055e01699e1bc8f80001'
db_map['host'] = '10.1.85.36'
db_map['port'] = 3306
db_map['dbName'] = 'test'
db_map['userName'] = 'root'
db_map['password'] = 'skq123123'

page_map['0_per_page'] = ''
page_map['0_total_page'] = ''
page_map['0_total_node'] = ''
page_map['0_total_page_node'] = ''

page_map['1_per_page'] = 24
page_map['1_total_page'] = 19
page_map['1_total_node'] = ''
# 翻页的总条数信息 可选，如果没有的话 模拟点击下一页 直到没有下一页 则扒取完毕
page_map['1_total_page_node'] = "//html/body/div[@class='w1024 mb50']/div[@class='list']/div[@class='pagediv']"

# 扒取详情页字段xpath路径信息
field_map['ziduan1'] = "//html/body/div[@class='w1024 mb50']/div[@class='list']/div[@class='tit']"
field_map['ziduan2'] = "//html/body/div[@class='w1024 mb50']/div[@class='list']/div[@class='source']/span[1]"

# 后续前端 如果能够判断出字段类型 可以传递对应的类型值 text为文字内容 date为日期类型
# field_type['ziduan1'] = 'text'
# field_type['ziduan2'] = 'date'

valueMap = {}
valueMap['crawl_id'] = 'f37d161cf3aa11e8b03cbca8a6e403d7'
valueMap['urlMap'] = url_map
valueMap['nextMap'] = next_map
valueMap['fieldMap'] = field_map
valueMap['dbMap'] = db_map
valueMap['nodeMap'] = node_map
valueMap['pageMap'] = page_map
valueMap['fieldType'] = field_type
jsonMap = json.dumps(valueMap)
# myMap = {}
# myMap["name"] = 'sara'
# myMap["age"] = 7
# myMap["class"] = 'first'

execute(["scrapy", "crawl", "common", '-avalue='+jsonMap])
# if __name__ == "__main__":
#     print("main")
#     pool = multiprocessing.Pool(processes=3)
#     pool.apply_async(start_worker, (jsonMap, '2018-09-16 21:10:59', '2018-09-16 21:11:30', '1', 1))
#     pool.close()
#     pool.join()
    #Timer(3, start_worker, (jsonMap, '2018-09-14 18:16:59', '2018-09-13 16:17:30', '1', 1)).start()
    #Timer(3, start_worker, (jsonMap, '2018-09-14 18:16:59', '2018-09-13 16:17:30', '1', 1)).start()
    # start_worker(jsonMap, '2018-09-13 16:16:59', '2018-09-13 16:17:30', '1', 1)
# content='附件1:作交流协议书撰写说明及范本">'
# PATTERN = '.*?附件.*?:.*'
# match_obj = re.match(PATTERN, content)
# if match_obj:
#     print(True)
# url = 'hao123.com'
# articleOrign = '<p><a href="{0}" target="_blank"><span style="color: #0070c0; text-decoration: underline;">' \
#                    '原文链接</span></a></p>'.format(url)
# ptn = re.compile('原文链接[^<]*?<a[^>]*?href="([^"]*)')
# print(articleOrign)


# mytest = ['1', '2', '3']
#
# for mt in mytest[0:3]:
#     print(mt)
#
# content = '青岛(200)'
# PATTERN = '[(](.*?)[)]'
#
# print(re.findall(r'[^()]+', content)[1])
#
# print(re.findall(r'[^()]+', content)[0])