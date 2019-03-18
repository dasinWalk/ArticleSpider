

import hashlib
import re
import json
import uuid
import datetime

en_array = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p',
            'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']


def get_list_to_text(list):
    result = ''
    if list is not None and len(list) > 0:
        for lt in list:
            result += lt.strip() + ','
    return result[0:-1]

def get_now_date():
    create_date = datetime.datetime.now().date()
    return create_date


def create_table_id():
    uid = str(uuid.uuid1())
    tid = uid.replace('-', '')
    return tid


def get_md5(url):
    if isinstance(url, str):
        url = url.encode("utf-8")
    m = hashlib.md5()
    m.update(url)
    return m.hexdigest()


def get_list(value_map):
    value_list = []
    for i in value_map:
        value_list.append(value_map[i])
    return value_list


def get_atextname(value):
    begin = en_array[value]
    return begin + 'text'


def get_next_page_node(value):
    begin = en_array[value]
    return begin + 'page_node'


def get_query_columns(column_list):
    result = ""
    if isinstance(column_list, list):
        for column in column_list:
            if column != column_list[len(column_list)-1]:
                result = result + "'" + column + "',"
            else:
                result = result + "'" + column + "'"
    return result


def get_total_number(value):
    result = 0
    if value != '0':
        regex_str = ".*?(\d+).*"
        match_obj = re.match(regex_str, value)
        if match_obj:
            result = int(match_obj.group(1))
    return result


def get_real_date(value):
    result = ''
    if value != '':
        value = value.replace("\n", "")
        value = value.replace(" ", "")
        regex_str = ".*?(\d{4}.\d+.\d+.).*"
        match_obj = re.match(regex_str, value)
        if match_obj:
            result = match_obj.group(1)
    if result == '':
        result = value
    return result


#去除字符串特殊字符
def get_filter_str(value):
    result = value.strip()
    result = result.replace(' ', '')
    result = result.replace(':', '')
    return result

def get_spider_param():
    url_map = {}
    next_map = {}
    field_map = {}
    db_map = {}
    node_map = {}
    page_map = {}
    field_type = {}

    url_map['start_url'] = 'http://www.nhc.gov.cn/'
    url_map['domain'] = 'www.nhc.gov.cn'

    node_map['start_node'] = "//html/body/div[@class='w1100 bgfff']/div[@class='zwgkleft fl']/ul[@class='zwgklist']/li[1]/h3[@class='tt']/a"
    node_map['second_node'] = "//html/body/div[@class='w1100 bgfff']/div[@class='zwgkleft fl']/ul[@class='zwgklist'"

    next_map['a_url_next'] = ''
    # 直接指定下一页的节点，翻页的话节点位置会发生变化
    next_map['b_url_next'] = '//*[@id="page_div"]/div[10]'
    # 1:mysql 2:oracle 3:mongo 4 hdfs
    db_map['type'] = '1'
    db_map['tableName'] = 'web_site_data3'
    db_map['task_id'] = '001002'
    db_map['host'] = '10.1.85.36'
    db_map['port'] = 3306
    db_map['dbName'] = 'dch'
    db_map['userName'] = 'root'
    db_map['password'] = 'skq123123'

    page_map['1_per_page'] = 8
    page_map['1_total_page'] = 125
    page_map['1_total_count'] = 1000
    page_map['1_total_node'] = ''
    page_map['1_total_page_node'] = ''

    field_map['title'] = "//html/body/div[@class='w1024 mb50']/div[@class='list']/div[@class='tit']"
    field_map['date'] = "//html/body/div[@class='w1024 mb50']/div[@class='list']/div[@class='source']/span"

    field_type['title'] = 'text'
    field_type['date'] = 'date'
    # url_map['start_url'] = 'http://www.nhfpc.gov.cn/qjjys/new_index.shtml'
    # url_map['domain'] = 'www.nhfpc.gov.cn'
    #
    # node_map['start_node'] = '/html/body/div[2]/div[1]/div[1]/div/span'
    # node_map['second_node'] = '/html/body/div[2]/div[2]/ul'
    #
    # next_map['a_url_next'] = ''
    # # 直接指定下一页的节点，翻页的话节点位置会发生变化
    # next_map['b_url_next'] = '//*[@id="page_div"]'
    # # 1:mysql 2:oracle 3:mongo 4 hdfs
    # db_map['type'] = '1'
    # db_map['tableName'] = 'web_site_data2'
    # db_map['task_id'] = '001002'
    # db_map['host'] = '10.1.85.36'
    # db_map['port'] = 3306
    # db_map['dbName'] = 'dch'
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
    value_map = {}
    value_map['urlMap'] = url_map
    value_map['nextMap'] = next_map
    value_map['fieldMap'] = field_map
    value_map['dbMap'] = db_map
    value_map['nodeMap'] = node_map
    value_map['pageMap'] = page_map
    value_map['fieldType'] = field_type
    json_map = json.dumps(value_map)
    return json_map


import random
from retrying import retry


@retry
def do_something_unreliable():
    if random.randint(0, 10) > 1:
        raise IOError("Broken sauce, everything is hosed!!!111one")
    else:
        return "Awesome sauce!"


if __name__ == "__main__":
    #print(get_md5("http://jobbole.com".encode("utf-8")))
    #print(get_total_number("共 22 页  跳转到"))
    # value = '96.120.0'
    # regex_str = ".*?(\d+[.]\d+)[.].*"
    # match_obj = re.match(regex_str, value)
    # if match_obj:
    #     result = match_obj.group(1)
    #     print(result)
    a = 1
    print(str(a))
    tvalue = '<html xmlns="http://www.w3.org/1999/xhtml"><head></head><body></body></html>'
    regex_tv = ".*?<body>(.*)?</body>.*"
    match_obj = re.match(regex_tv, tvalue)
    if match_obj:
        result = match_obj.group(1)
        print(tvalue.__len__())
    print(do_something_unreliable())
    value = "dch_1540794792460:20181029"
    regex_str = "^dch_1540794792460.*0291.*$"
    match_obj = re.match(regex_str, value)
    if match_obj:
        result = match_obj.group(1)
        print(result)
    print(get_real_date("发布时间：2013-11-12"))

