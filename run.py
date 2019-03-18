from multiprocessing import Lock, Pool
import time
import os
from ArticleSpider.utils.nativemysql import NativeMysql
import platform
import requests
from ArticleSpider.utils.common import get_spider_param
from ArticleSpider.utils.common import create_table_id, get_now_date
import uuid
import json
import logging
import pika


credentials = pika.PlainCredentials('admin', '123456')
connection = pika.BlockingConnection(pika.ConnectionParameters('10.1.85.35', '5672', '/', credentials))
channel = connection.channel();
channel.queue_declare(queue='dch', durable=True)

def work(message):
    #查询数据库中配置的scrapyd服务 进行爬虫任务启动
    try:
        result = get_scrapy_list()
        sysstr = platform.system()
        #json_map = get_spider_param()
        msg_map = json.loads(message)
        value_map = msg_map['valueMap']
        for item in result:
            crawl_id = add_crawl_job(value_map, item[0])
            value_map['crawl_id'] = crawl_id
            json_map = json.dumps(value_map)
            data = {
                'project': 'ArticleSpider',
                'spider': 'common',
                'param': json_map
            }
            response = requests.post(item[0], data=data)
            logging.info("start scrapy spider ok")
        time.sleep(3)
        start_url = value_map['urlMap']['start_url']
        task_id = value_map['dbMap']['task_id']
        add_crawl_record(task_id)
        obj.do_command('common:start_urls', start_url)
        obj.del_key(task_id)
        obj.del_key(task_id + 'dog')
        obj.del_key(task_id + 'node_list')
        if obj.if_exist_key(task_id, 'success_num'):
            obj.remove_hash("'" + task_id + "'", {'success_num'})
        if obj.if_exist_key(task_id, 'failed_num'):
            obj.remove_hash("'" + task_id + "'", {'failed_num'})
        obj.set_value(task_id)
        #添加看门狗，第一个item时初始化字段用
        obj.set_value(task_id + 'dog')
    except Exception as e:
        error = 'failed! ERROR (%s): %s' % (e.args[0], e.args[1])
        print(error)
        reset_crawl_task(task_id)

    print('End process', message)


# 爬虫任务开启失败更改任务状态
def reset_crawl_task(task_id):
    mysql2 = NativeMysql()
    sql = """
              update crawl_task set task_status = 3 where id = %s
            """
    params = (task_id, )
    mysql2.update(sql, param=params)


# 查询可用的爬虫服务系统
def get_scrapy_list():
    mysql2 = NativeMysql()
    sql = """
          SELECT url FROM scrapy_service where  1=1 
        """
    result = mysql2.getAll(sql)
    return result


#开启爬虫任务 记录任务信息
def add_crawl_job(value_map, service):
    db_map = value_map['dbMap']
    mysql2 = NativeMysql()
    tid = create_table_id()
    sql = """
          insert into scrapy_task (id,task_id,status,service_name,crawl_num,lost_num,total_page,history) 
          values (%s, %s, %s, %s, %s, %s, %s, %s)
        """
    params = (tid, db_map["task_id"], 0, service, 0, 0, 0, 0)
    mysql2._execute_commit(sql, arg=params)
    return tid


# 添加爬虫记录信息
def add_crawl_record(task_id):
    mysql2 = NativeMysql()
    tid = create_table_id()
    create_time = get_now_date()
    sql = """
          insert into crawl_record (id,task_id,create_time,start_time,crawl_num,in_db_num) 
          values (%s, %s, %s, %s, %s, %s)
        """
    params = (tid, task_id, create_time, create_time, 0, 0)
    mysql2._execute_commit(sql, arg=params)
    return tid


def callback(ch, method, properties, body):
    msg = str(body, 'utf-8')
    ch.basic_ack(delivery_tag=method.delivery_tag)
    if msg != '':
        print(msg)
        pool = Pool(processes=1)
        pool.apply_async(work, (msg,))
        pool.close()
        pool.join()


if __name__ == '__main__':
    k = 0
    channel.basic_consume(callback, queue='dch', no_ack=False)
    channel.start_consuming()

