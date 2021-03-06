
import requests
from scrapy.selector import Selector
from ArticleSpider.utils.nativemysql import NativeMysql


def crawl_ips():
    #爬取西刺的免费代理ip
    mysql1 = NativeMysql()
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36"}
    for i in range(3444):
        result = requests.get("http://www.xicidaili.com/nn/{0}".format(i), headers=headers)
        selector = Selector(text=result.text)
        all_trs = selector.css("#ip_list tr")
        ip_list = []
        for tr in all_trs[1:]:
            speed_str = tr.css(".bar::attr(title)").extract()[0]
            if '秒' in speed_str:
                speed = float(speed_str.split("秒")[0])
                all_texts = tr.css("td::text").extract()
                ip = all_texts[0]
                port = all_texts[1]
                proxy_type = all_texts[5]
                if proxy_type not in ['HTTP', 'HTTPS']:
                    proxy_type = all_texts[4]
                ip_list.append((ip, port, proxy_type, speed))
        for ip_info in ip_list:
            sql = "insert into proxy_ip(ip, port, proxy_type, speed) VALUES ('{0}', '{1}', '{2}', {3})".format(
                ip_info[0], ip_info[1], ip_info[2], ip_info[3]
            )
            mysql1.insertOne(sql)


class GetIp(object):
    mysql2 = NativeMysql()

    def delete_ip(self, ip):
        # 从数据库中删除无效的ip
        delete_sql = """
            delete from proxy_ip where ip='{0}'
        """.format(ip)
        self.mysql2.delete(delete_sql)
        return True

    def judge_ip(self, ip, port):
        # 判断ip是否可用
        http_url = "http://www.baidu.com"
        proxy_url = "http://{0}:{1}".format(ip, port)
        try:
            proxy_dict = {
                "http": proxy_url,
            }
            response = requests.get(http_url, proxies=proxy_dict, timeout=4)
        except Exception as e:
            print("invalid ip and port")
            self.delete_ip(ip)
            return False
        else:
            code = response.status_code
            if code >= 200 and code < 300:
                print("effective ip")
                return True
            else:
                print("invalid ip and port")
                self.delete_ip(ip)
                return False

    def get_random_ip(self):
        # 从数据库中随机获取一个可用的ip
        random_sql = """
              SELECT ip, port FROM proxy_ip where  proxy_type = 'HTTPS'
            ORDER BY RAND()
            LIMIT 1
            """
        result = self.mysql2.getOne(random_sql)
        ip = result[0]
        port = result[1]
        judge_re = self.judge_ip(ip, port)
        if judge_re:
            return "http://{0}:{1}".format(ip, port)
        else:
            return self.get_random_ip()


if __name__ == "__main__":
    crawl_ips()
