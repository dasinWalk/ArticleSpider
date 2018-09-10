

import hashlib
import re

en_array = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p',
            'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']


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


if __name__ == "__main__":
    #print(get_md5("http://jobbole.com".encode("utf-8")))
    print(get_total_number("共 22 页  跳转到"))

