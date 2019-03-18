# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
import datetime
from scrapy.loader.processors import MapCompose, TakeFirst, Join
from scrapy.loader import ItemLoader
import re
import json
from w3lib.html import remove_tags
from ArticleSpider.settings import SQL_DATETIME_FORMAT, SQL_DATE_FORMAT
from ArticleSpider.utils.common import get_query_columns
from ArticleSpider.utils.mysql import Mysql


class ArticlespiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


def add_jobbole(value):
    return value + "-jobbole"


def date_convert(value):
    try:
        create_date = datetime.datetime.strptime(value, "%Y/%m/%d").date()
    except Exception as e:
        create_date = datetime.datetime.now().date()
    return create_date


def get_nums(value):
    match_re = re.match(".*(\d+).*", value)
    if match_re:
        nums = int(match_re.group(1))
    else:
        nums = 0
    return nums


def remove_comment_tags(value):
    #去掉tags中提取的评论
    if "评论" in value:
        return ""
    else:
        return value


def return_value(value):
    return value


def get_image_urls(value):
    if "," in value:
        return ",".join(value)
    return value


def join_value(value):
    result = "".join(value)
    result = result.strip()
    return result


class ArticleItemLoader(ItemLoader):
    #自定义itemloader输出
    default_output_processor = TakeFirst()


class ChictrItem(scrapy.Item):
    projectName = scrapy.Field()
    file_urls = scrapy.Field()
    xmlPath = scrapy.Field()


class JobBoleArticleItem(scrapy.Item):
    title = scrapy.Field()
    create_date = scrapy.Field(
        input_processor=MapCompose(date_convert),
    )
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    front_image_url = scrapy.Field(
        output_processor=MapCompose(return_value)
    )
    front_image_path = scrapy.Field()
    praise_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    comment_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    fav_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    tags = scrapy.Field(
        input_processor=MapCompose(remove_comment_tags),
        output_processor=Join(",")
    )
    content = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            insert into article(title, url, create_date, fav_nums)
            VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE content=VALUES(fav_nums)
        """
        params = (self["title"], self["url"], self["create_date"], self["fav_nums"])

        return insert_sql, params

    # def save_to_es(self):
    #     article = ArticleType()
    #     article.title = self['title']
    #     article.create_date = self["create_date"]
    #     article.content = remove_tags(self["content"])
    #     article.front_image_url = self["front_image_url"]
    #     if "front_image_path" in self:
    #         article.front_image_path = self["front_image_path"]
    #     article.praise_nums = self["praise_nums"]
    #     article.fav_nums = self["fav_nums"]
    #     article.comment_nums = self["comment_nums"]
    #     article.url = self["url"]
    #     article.tags = self["tags"]
    #     article.meta.id = self["url_object_id"]
    #
    #     article.suggest = gen_suggests(ArticleType._doc_type.index, ((article.title, 10), (article.tags, 7)))
    #
    #     article.save()
    #
    #     redis_cli.incr("jobbole_count")
    #
    #     return


class ZhihuQuestionItem(scrapy.Item):
    # 知乎的问题 item
    zhihu_id = scrapy.Field()
    topics = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    answer_num = scrapy.Field()
    comments_num = scrapy.Field()
    watch_user_num = scrapy.Field()
    click_num = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        # 插入知乎question表的sql语句
        insert_sql = """
            insert into zhihu_question(zhihu_id, topics, url, title, content, answer_num, comments_num,
              watch_user_num, click_num, crawl_time
              )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE content=VALUES(content), answer_num=VALUES(answer_num), comments_num=VALUES(comments_num),
              watch_user_num=VALUES(watch_user_num), click_num=VALUES(click_num)
        """
        zhihu_id = self["zhihu_id"][0]
        topics = ",".join(self["topics"])
        url = self["url"][0]
        title = "".join(self["title"])
        content = "".join(self["content"])
        answer_num = extract_num("".join(self["answer_num"]))
        comments_num = extract_num("".join(self["comments_num"]))

        if len(self["watch_user_num"]) == 2:
            watch_user_num = int(self["watch_user_num"][0])
            click_num = int(self["watch_user_num"][1])
        else:
            watch_user_num = int(self["watch_user_num"][0])
            click_num = 0

        crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)

        params = (zhihu_id, topics, url, title, content, answer_num, comments_num,
                  watch_user_num, click_num, crawl_time)

        return insert_sql, params

class ZhihuAnswerItem(scrapy.Item):
    # 知乎的问题回答item
    zhihu_id = scrapy.Field()
    url = scrapy.Field()
    question_id = scrapy.Field()
    author_id = scrapy.Field()
    content = scrapy.Field()
    parise_num = scrapy.Field()
    comments_num = scrapy.Field()
    create_time = scrapy.Field()
    update_time = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        # 插入知乎question表的sql语句
        insert_sql = """
            insert into zhihu_answer(zhihu_id, url, question_id, author_id, content, parise_num, comments_num,
              create_time, update_time, crawl_time
              ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
              ON DUPLICATE KEY UPDATE content=VALUES(content), comments_num=VALUES(comments_num), parise_num=VALUES(parise_num),
              update_time=VALUES(update_time)
        """

        create_time = datetime.datetime.fromtimestamp(self["create_time"]).strftime(SQL_DATETIME_FORMAT)
        update_time = datetime.datetime.fromtimestamp(self["update_time"]).strftime(SQL_DATETIME_FORMAT)
        params = (
            self["zhihu_id"], self["url"], self["question_id"],
            self["author_id"], self["content"], self["parise_num"],
            self["comments_num"], create_time, update_time,
            self["crawl_time"].strftime(SQL_DATETIME_FORMAT),
        )

        return insert_sql, params

def remove_splash(value):
    # 去掉工作城市的斜线
    return value.replace("/", "")

def handle_jobaddr(value):
    addr_list = value.split("\n")
    addr_list = [item.strip() for item in addr_list if item.strip() != "查看地图"]
    return "".join(addr_list)


class LagouJobItemLoader(ItemLoader):
    # 自定义itemloader
    default_output_processor = TakeFirst()


class LagouJobItem(scrapy.Item):
    # 拉勾网职位信息
    title = scrapy.Field()
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    salary = scrapy.Field()
    job_city = scrapy.Field(
        input_processor=MapCompose(remove_splash),
    )
    work_years = scrapy.Field(
        input_processor=MapCompose(remove_splash),
    )
    degree_need = scrapy.Field(
        input_processor=MapCompose(remove_splash),
    )
    job_type = scrapy.Field()
    publish_time = scrapy.Field()
    job_advantage = scrapy.Field()
    job_desc = scrapy.Field()
    job_addr = scrapy.Field(
        input_processor=MapCompose(remove_tags, handle_jobaddr),
    )
    company_name = scrapy.Field()
    company_url = scrapy.Field()
    tags = scrapy.Field(
        input_processor=Join(",")
    )
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            insert into lagou_job(title, url, url_object_id, salary, job_city, work_years, degree_need,
            job_type, publish_time, job_advantage, job_desc, job_addr, company_name, company_url,
            tags, crawl_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE salary=VALUES(salary), job_desc=VALUES(job_desc)
        """
        params = (
            self["title"], self["url"], self["url_object_id"], self["salary"], self["job_city"],
            self["work_years"], self["degree_need"], self["job_type"],
            self["publish_time"], self["job_advantage"], self["job_desc"],
            self["job_addr"], self["company_name"], self["company_url"],
            self["job_addr"], self["crawl_time"].strftime(SQL_DATETIME_FORMAT),
        )

        return insert_sql, params


class kjjysItemLoader(ItemLoader):
    # 自定义itemloader
    default_output_processor = TakeFirst()


class kjjysItem(scrapy.Item):
    # 科技教育司信息
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    front_image_url = scrapy.Field(
        output_processor=MapCompose(return_value, get_image_urls)
    )
    front_image_path = scrapy.Field(
        input_processor=MapCompose(remove_splash),
    )
    source_net = scrapy.Field()
    source_name = scrapy.Field()
    type_name = scrapy.Field()
    title = scrapy.Field(
        output_processor=MapCompose(join_value)
    )
    content = scrapy.Field()
    publish_time = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            insert into technology(url, url_object_id, front_image_url, front_image_path, source_net, source_name,
            type_name, title, content, publish_time, crawl_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE content=VALUES(content), publish_time=VALUES(publish_time)
        """
        params = (
            self["url"], self["url_object_id"], ",".join(self["front_image_url"]), self["front_image_path"],
            self["source_net"], self["source_name"], self["type_name"],
            ",".join(self["title"]), self["content"], self["publish_time"], self["crawl_time"].strftime(SQL_DATETIME_FORMAT)
        )

        return insert_sql, params


class PharmnetItemLoader(ItemLoader):
    # 自定义itemloader
    default_output_processor = TakeFirst()


class PharmnetItem(scrapy.Item):
    # 科技教育司信息
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    province = scrapy.Field()
    city_name = scrapy.Field()
    hospital_name = scrapy.Field()
    grade = scrapy.Field()
    type = scrapy.Field()
    ifInsurance = scrapy.Field()
    beds_num = scrapy.Field()
    outpatient = scrapy.Field()
    address = scrapy.Field()
    zipcode = scrapy.Field()
    telephone = scrapy.Field()
    net_work = scrapy.Field()
    bus_line = scrapy.Field()
    equipment = scrapy.Field()
    specialties = scrapy.Field()
    hospital_introduce = scrapy.Field()
    crawl_time = scrapy.Field()


class CommonItemLoader(ItemLoader):
    # 自定义itemloader
    default_output_processor = TakeFirst()


class CommonItem(scrapy.Item):
    # 科技教育司信息
    id = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    crawl_time = scrapy.Field()
    fieldMap = scrapy.Field()
    dbMap = scrapy.Field()

    def get_task_id(self):
        db_map_json = self["dbMap"]
        db_map = json.loads(db_map_json)
        task_id = db_map["task_id"]
        return task_id + 'dog'

    def get_init_sql(self):
        field_map_json = self["fieldMap"]
        db_map_json = self["dbMap"]
        field_map = json.loads(field_map_json)
        db_map = json.loads(db_map_json)
        table_name = db_map["tableName"]
        table_schema = db_map["dbName"]
        db_type = db_map["type"]
        task_id = db_map["task_id"]
        alter_sql = ""
        alter_list = []
        column_list = []
        for mt in field_map:
            column_list.append(mt)
        records = Mysql.judge_column_exist(Mysql(db_map),table_schema=table_schema, table_name=table_name, column_list=column_list)
        for rec in field_map:
            if rec not in records:
                alter_list.append(rec)
        alen = len(alter_list)
        print(alter_list)
        if alen > 0:
            alter_sql = "alter table " + table_name + " add ("
            for item in alter_list:
                if item != alter_list[alen-1]:
                    alter_sql = alter_sql + item + " text,"
                else:
                    alter_sql = alter_sql + item + " text)"
        return alter_sql, db_type, task_id

    def get_insert_sql(self):
        field_map_json = self["fieldMap"]
        db_map_json = self["dbMap"]
        field_map = json.loads(field_map_json)
        db_map = json.loads(db_map_json)
        table_name = db_map["tableName"]
        db_type = db_map["type"]
        insert_sql = "insert into " + table_name + "(id, url, title, content, crawl_time,"
        flen = len(field_map)
        k = 0
        if flen > 0:
            for item in field_map:
                k = k + 1
                if k == flen:
                    insert_sql = insert_sql + item + ") VALUES ("
                else:
                    insert_sql = insert_sql + item + ","
        total = flen + 5
        for i in range(total):
            if i != total-1:
                insert_sql = insert_sql + "%s,"
            else:
                insert_sql = insert_sql + "%s) ON DUPLICATE KEY UPDATE content=VALUES(content)"
        param_map = {}
        try:
            param_map["id"] = self["id"]
            param_map["url"] = self["url"]
            param_map["title"] = self["title"]
            param_map["content"] = self["content"]
            param_map["crawl_time"] = self["crawl_time"].strftime(SQL_DATETIME_FORMAT)
        except Exception as e:
            print("获取数据异常")
            print(e)
        for mt in field_map:
            param_map[mt] = field_map[mt]
        params = tuple(param_map.values())
        return insert_sql, params


class ChemyItem(scrapy.Item):
    # 中国卫生经济周刊
    art_num = scrapy.Field()
    title_cn = scrapy.Field()
    title_en = scrapy.Field()
    field_map = scrapy.Field()
    year = scrapy.Field()
