import redis
from ArticleSpider.settings import REDIS_HOST, REDIS_PASSWORD, REDIS_PORT


class RedisHelper:
    def __init__(self):
        self.__conn = redis.Redis(host=REDIS_HOST, password=REDIS_PASSWORD, port=REDIS_PORT)
        self.chan_sub = 'dch'
        self.chan_pub = 'dch'

#发送消息
    def public(self, msg):
        self.__conn.publish(self.chan_pub, msg)
        return True

#订阅
    def subscribe(self):
        #打开收音机
        pub = self.__conn.pubsub()
        #调频道
        pub.subscribe(self.chan_sub)
        #准备接收
        pub.parse_response()
        return pub

    def do_command(self, spider_name, command):
        return self.__conn.lpush(spider_name, command)

    def set_value(self, key):
        return self.__conn.set(key, 0)

    def inc_value(self, key):
        return self.__conn.incr(key, 1)

    def del_key(self, key):
        return self.__conn.delete(key)

    def get_value(self, key):
        return self.__conn.get(key)

    def set_list(self,key,value):
        self.__conn.set(key, value)

    def set_lock(self, name, value):
        rt = self.__conn.setnx(name, value)
        self.__conn.expire(name, 3)
        return rt

    def set_hash_value(self,name, key, value):
        self.__conn.hset(name, key, value)

    def hash_inrc_key(self,name, key, incr_value):
        self.__conn.hincrby(name, key, amount=incr_value)

    def get_hash_value(self,name, key):
        return self.__conn.hget(name, key)

    def remove_hash(self,name, keys):
        return self.__conn.hdel(name, keys)

    def if_exist_key(self,name, key):
        return self.__conn.hexists(name=name, key=key)


if __name__ == "__main__":
    obj = RedisHelper()
    import time
    for i in range(10):
        obj.public('how are you{0}?'.format(i))
        obj.public('i am fine{0}?'.format(i))
        time.sleep(3)

