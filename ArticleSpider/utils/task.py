import time
import sched
import threading
from threading import Timer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from ArticleSpider.spiders.commonSpider import CommonSpider
from twisted.internet import reactor, defer
from scrapy.utils.project import get_project_settings

configure_logging()
watch_dog = []
mutex = threading.Lock()


@defer.inlineCallbacks
def crawl(json_map):
    print("begin-----------1")
    runner = CrawlerRunner(get_project_settings())
    yield runner.crawl(CommonSpider, value=json_map)
    reactor.stop()
    print("end-----------2")


# class UrlCrawlerScript(Process):
#     def __init__(self, spider, job):
#         Process.__init__(self)
#         self.crawler = CrawlerRunner(get_project_settings())
#         self.crawler.crawl(spider, value=job)
#
#     def run(self):
#         d = self.crawler.join()
#         d.addBoth(lambda _: reactor.stop())
#         reactor.run(0)


def get_format_date(in_time):
    format_date = ''
    if in_time is not None and in_time > 0:
        time_array = time.localtime(in_time)
        format_date = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
    if format_date == '':
        format_date = in_time
    return format_date


def get_time(fm_time):
    if fm_time is None:
        return fm_time
    time_array = time.strptime(fm_time, "%Y-%m-%d %H:%M:%S")
    time_stamp = time.mktime(time_array)
    return time_stamp


def worker(json_map, start_time, end_time):
    print("------------------执行爬虫任务------------------")
    crawl(json_map)
    print(threading.currentThread().name)
    if len(watch_dog) < 1:
        print("111111111")
        reactor.run()
        watch_dog.append(1)
    mutex.release()
    #crawler = UrlCrawlerScript(CommonSpider, json_map)
    #crawler.start()
    print(u"任务执行的时刻", get_format_date(time.time()), "传达的消息是", json_map, '任务建立时刻', get_format_date(start_time))
    if end_time is not None and end_time > start_time and end_time > time.time():
        sch = sched.scheduler(time.time, time.sleep)
        sch.enter(1, 1, worker, (json_map, time.time(), end_time))
        sch.run()


def start_worker(json_map, start_time, end_time, task_type, priority):
    # 如果为开始时间和结束时间
    sch = sched.scheduler(time.time, time.sleep)
    if task_type == '1':
        time_stamp = get_time(start_time)
        end_stamp = get_time(end_time)
        time_now = time.time()
        if time_stamp < time_now:
            sch.enter(1, priority, worker, (json_map, time_now, end_stamp))
            sch.run()
        else:
            delay = int(time_stamp - time_now)
            sch.enter(delay, priority, worker, (json_map, time_now, end_stamp))
            sch.run()


if __name__ == "__main__":
    Timer(3, start_worker, ('任务1', '2018-09-13 16:16:59', '2018-09-13 16:17:30', '1', 1)).start()
    print("--------------------------华丽的分割线-----------------------------")
    Timer(3, start_worker, ('任务2', '2018-09-13 16:16:59', '2018-09-13 16:17:30', '1', 1)).start()
    tss1 = '2018-09-13 15:12:59'
    timeArray = time.strptime(tss1, "%Y-%m-%d %H:%M:%S")
    print(time.mktime(timeArray))
# print(u'程序启动时刻：', get_format_date(time.time()))
# s = sched.scheduler(time.time, time.sleep)
# s.enter(1, 1, worker, ('hello', time.time(), None))
# s.enter(3, 1, worker, ('world', time.time(), None))
# s.run()  # 这一个 s.run() 启动上面的两个任务
# print(u'睡眠２秒前时刻：', get_format_date(time.time()))
# time.sleep(2)
# print(u'睡眠２秒结束时刻：', get_format_date(time.time()))


