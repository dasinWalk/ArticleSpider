# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.pipelines.images import ImagesPipeline
import codecs
import json
from scrapy.exporters import JsonItemExporter
import MySQLdb
import MySQLdb.cursors
import cx_Oracle
import logging
from ArticleSpider.utils import Config
from twisted.enterprise import adbapi
from scrapy.pipelines.files import FilesPipeline
from scrapy import Request
from scrapy.exceptions import DropItem
from urllib.parse import urlparse
from os.path import basename, dirname, join
from pymongo import MongoClient
import time
try:
    from urllib.parse import quote_plus
except ImportError:
    from urllib import quote_plus
from ArticleSpider.utils.RedisUtils import RedisHelper
from ArticleSpider.utils.common import get_filter_str
import os
obj = RedisHelper()


class ArticlespiderPipeline(object):
    def process_item(self, item, spider):
        return item


class TextWithEncodingPipeline(object):
    def process_item(self, item, spider):
        year = item["year"]
        pwd = os.getcwd()
        path_year = pwd + '/' + year
        if not os.path.exists(year):
            os.mkdir(path_year)
        os.chdir(path_year)
        file_name = get_filter_str(str(item["title_cn"])) + ".txt"
        self.file = codecs.open(file_name, 'w', encoding='utf-8')
        lines = item["title_cn"] + "\n"
        lines = lines + item["title_en"] + "\n"
        field_map = item["field_map"]
        for fk in field_map:
            lines = lines + fk + "：" + field_map[fk] + "\n"
        self.file.write(lines)
        self.file.close()
        os.chdir(pwd)
        return item


class JsonWithEncodingPipeline(object):
    def __init__(self):
        self.file = codecs.open('article.json', 'w', encoding='utf-8')

    def process_item(self, item, spider):
        lines = json.dumps(dict(item), ensure_ascii=False) + "\n"
        self.file.write(lines)
        return item

    def spider_closed(self, spider):
        self.file.close()


class MysqlPipeline(object):
    #使用mysql的同步机制保存数据, 使用scrapy自带的item外 可以使用djiango item插件来保存数据
    def __init__(self):
        self.conn = MySQLdb.connect('127.0.0.1', 'root', 'skq123123', 'rdf', charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        insert_sql = """
               insert into article(url_object_id, title, create_date, url, front_image_url, front_image_path, comment_nums, fav_nums, praise_nums, tags, content)
               values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        self.cursor.execute(insert_sql, (item["url_object_id"], item["title"], item["create_date"], item["url"],
                                         item["front_image_url"], item["front_image_path"], item["comment_nums"], item["fav_nums"],
                                         item["praise_nums"], item["tags"], item["content"]))
        self.conn.commit()


class MysqlTwistedPipline(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        dbparms = dict(
            host=settings["MYSQL_HOST"],
            db=settings["MYSQL_DBNAME"],
            user=settings["MYSQL_USER"],
            passwd=settings["MYSQL_PASSWORD"],
            charset='utf8',
            cursorclass=MySQLdb.cursors.DictCursor,
            use_unicode=True,
        )
        dbpool = adbapi.ConnectionPool("MySQLdb", **dbparms)

        return cls(dbpool)

    def process_item(self, item, spider):
        #使用twisted将mysql插入变成异步执行
        query = self.dbpool.runInteraction(self.do_insert, item)
        query.addErrback(self.handle_error, item, spider) #处理异常

    def handle_error(self, failure, item, spider):
        #处理异步插入的异常
        print(failure)

    def do_insert(self, cursor, item):
        #执行具体的插入
        #根据不同的item 构建不同的sql语句并插入到mysql中
        insert_sql, params = item.get_insert_sql()
        cursor.execute(insert_sql, params)


class JsonExporterPipeline(object):
    #调用scrapy提供的json exporter导出json文件
    def __init__(self):
        self.file = open('articleexporter.json', 'wb')
        self.exporter = JsonItemExporter(self.file, encoding="utf-8",ensure_ascii=False)
        self.exporter.start_exporting()

    def close_spider(self,spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item


class ArticleImagePipline(ImagesPipeline):
    def item_completed(self, results, item, info):
        if "front_image_url" in item:
            for ok, value in results:
                image_file_path = value["path"]
                item["front_image_path"] = image_file_path

        return item


class KjjysImagePipline(ImagesPipeline):

    def item_completed(self, results, item, info):
        if "front_image_url" in item:
            for ok, value in results:
                image_file_path = value["path"]
                if "front_image_path" in item:
                    imagePath = item["front_image_path"]
                    if imagePath == '--':
                        item["front_image_path"] = image_file_path
                    else:
                        item["front_image_path"] = imagePath + ',' + image_file_path
                else:
                    item["front_image_path"] = image_file_path
        else:
            item["front_image_url"] = ''

        return item


class MyFilePipeline(FilesPipeline):
    # def get_media_requests(self, item, info):
    #     file_url = item["file_urls"][0]
    #     yield Request(file_url)
    #
    # def item_completed(self, results, item, info):
    #     file_paths = [x['path'] for ok, x in results if ok]
    #     if not file_paths:
    #         raise DropItem("Item contains no files")
    #     item['file_paths'] = file_paths
    #     return item
    def get_media_requests(self, item, info):
        return [Request(x, meta={"projectName": item["projectName"]}) for x in item.get(self.files_urls_field, [])]

    def file_path(self, request, response=None, info=None):
        path = urlparse(request.url).path
        path = request.meta.get("projectName", "")
        path = path.replace("/", "_")
        path = path.replace(" ", "")
        path = path + ".xml"
        return join(basename(dirname(path)), basename(path))


class MultipleTwistedPipline(object):
    watch_dog = ''
    success = ''

    def __init__(self, db_pool):
        self.db_pool = db_pool
        #self.oracleparms = oracleparms

    # @classmethod
    # def from_settings(cls, settings):
    #     dbparms = dict(
    #         host=settings["MYSQL_HOST"],
    #         db=settings["MYSQL_DBNAME"],
    #         user=settings["MYSQL_USER"],
    #         passwd=settings["MYSQL_PASSWORD"],
    #         charset='utf8',
    #         cursorclass=MySQLdb.cursors.DictCursor,
    #         use_unicode=True,
    #     )
    #     oracleparms = dict(
    #         host=settings["ORACLE_HOST"],
    #         db=settings["ORACLE_DBNAME"],
    #         user=settings["ORACLE_USER"],
    #         passwd=settings["ORACLE_PASSWORD"],
    #         charset='utf8',
    #         cursorclass=MySQLdb.cursors.DictCursor,
    #         use_unicode=True,
    #     )
    #     dbpool = adbapi.ConnectionPool("MySQLdb", **dbparms)
    #     return cls(dbpool, oracleparms)

    @classmethod
    def from_crawler(cls, crawler):
        db_pool = None
        try:
            db_map = crawler.spider.db_map
            db_type = db_map['type']
            ##1:mysql 2:oracle 3:mongodb 4:hdfs
            if db_type == '1':
                db_params = dict(
                    host=db_map['host'],
                    db=db_map['dbName'],
                    port=db_map['port'],
                    user=db_map['userName'],
                    passwd=db_map['password'],
                    charset='utf8',
                    cursorclass=MySQLdb.cursors.DictCursor,
                    use_unicode=True,
                )
                db_pool = adbapi.ConnectionPool("MySQLdb", **db_params)
            elif db_type == '2':
                dsn = db_map['host'] + "/" + db_map['dbName']
                db_pool = adbapi.ConnectionPool("cx_oracle", user=db_map['userName'],
                                                password=db_map['password'], dsn=dsn)
            elif db_type == '3':
                host = db_map['host']
                user = db_map['userName']
                passwd = db_map['password']
                port = db_map['port']
                mongodb_name = db_map['dbName']
                table_name = db_map['tableName']
                mongodb_uri = 'mongodb://' + host + ':' + port + '/'  # 没有账号密码验证
                if user != '':
                    mongodb_uri = "mongodb://%s:%s@%s" % (quote_plus(user), quote_plus(passwd), host)
                client = MongoClient(mongodb_uri)  # 创建了与mongodb的连接
                db = client[mongodb_name]
                db_pool = db[table_name]  # 获取数据库中表的游标
        except AttributeError as e:
            print('AttributeError', e)
        if db_pool is None:
            db_params = dict(
                host=Config.MYSQL_HOST,
                db=Config.MYSQL_DBNAME,
                port=Config.MYSQL_DBPORT,
                user=Config.MYSQL_USER,
                passwd=Config.MYSQL_PASSWORD,
                charset='utf8',
                cursorclass=MySQLdb.cursors.DictCursor,
                use_unicode=True,
                )
            db_pool = adbapi.ConnectionPool("MySQLdb", **db_params)
        return cls(db_pool)

    def process_item(self, item, spider):
        #使用twisted将mysql插入变成异步执行
        if type(self.db_pool) == adbapi.ConnectionPool:
            query = self.db_pool.runInteraction(self.do_insert, item)
            query.addErrback(self.handle_error, item, spider) #处理异常
        else:
            self.db_pool.insert(dict(item))

    # 处理异步插入的异常
    def handle_error(self, failure, item, spider):
        print(failure)
        logging.info(failure)

    def do_insert(self, cursor, item):
        try:
            while self.watch_dog == '':
                watch_dog = item.get_task_id()
                rt = obj.set_lock(self.watch_dog, 'value')
                if rt:
                    if self.watch_dog == '':
                        alter_sql, db_type, task_id = item.get_init_sql()
                        if alter_sql != '':
                            logging.info("添加新的字段成功")
                            cursor.execute(alter_sql)
                        self.watch_dog = watch_dog

            insert_sql, params = item.get_insert_sql()
            cursor.execute(insert_sql, params)
        except Exception as e:
            logging.info("插入数据异常")
            logging.info(e)
