import os
import sys
import re
import json
import math
from scrapy.cmdline import execute

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# execute(["scrapy","crawl","jobbole"])
#execute(["scrapy", "crawl", "chictr"])
# execute(["scrapy", "crawl", "kjjys"])
# execute(["scrapy", "crawl", "kjxwzx"])
# execute(["scrapy", "crawl", "kjxwzx_gwy"])
#execute(["scrapy", "crawl", "kxjsb_yw"])
# execute(["scrapy", "crawl", "gjzrkx"])
# execute(["scrapy", "crawl", "zgkxb"])
# execute(["scrapy", "crawl", "gjypjg"])
# execute(["scrapy", "crawl", "science"])
#execute(["scrapy", "crawl", "pharmnet"])
url_map = {}
next_map = {}
field_map = {}
db_map = {}
node_map = {}
page_map = {}
#中国医药网
# url_map['start_url'] = 'http://www.pharmnet.com.cn/search/template/yljg_index.htm'
# url_map['second_url'] = 'http://www.pharmnet.com.cn/search/index.cgi?c1=19&c2=%C9%BD%B6%AB&p=1'
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

url_map['start_url'] = 'http://www.nhfpc.gov.cn/qjjys/new_index.shtml'
url_map['second_url'] = 'http://www.nhfpc.gov.cn/qjjys/pqt/new_list.shtml'

node_map['start_node'] = '/html/body/div[2]/div[1]/div[1]/div/span'
node_map['second_node'] = '/html/body/div[2]/div[2]/ul'

next_map['a_url_next'] = ''
next_map['b_url_next'] = '//*[@id="page_div"]/div[8]/span'
#1:mysql 2:oracle 3:mongo 4 hdfs
db_map['type'] = '1'
db_map['tableName'] = 'web_site_data2'
db_map['task_id'] = '001002'

page_map['2_per_page'] = 24
page_map['2_total_page'] = 30
page_map['2_total_count'] = 450
page_map['2_total_node'] = ''
page_map['2_total_page_node'] = '//*[@id="page_div"]/div[10]'

field_map['publish_time'] = '/html/body/div[2]/div[2]/div[2]/span'
field_map['content'] = '//*[@id="xw_box"]'

valueMap = {}
valueMap['urlMap'] = url_map
valueMap['nextMap'] = next_map
valueMap['fieldMap'] = field_map
valueMap['dbMap'] = db_map
valueMap['nodeMap'] = node_map
valueMap['pageMap'] = page_map

for item in field_map:
    print(item)
jsonMap = json.dumps(valueMap)
print(jsonMap)

myMap = {}
myMap["name"] = 'sara'
myMap["age"] = 7
myMap["class"] = 'first'

print(tuple(myMap.values()))
execute(["scrapy", "crawl", "common", '-avalue='+jsonMap])
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