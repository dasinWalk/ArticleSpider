
import requests
import time
try:
    import cookielib
except:
    import http.cookiejar as cookielib
import re
import hmac
from hashlib import sha1
import json
import base64
from PIL import Image

session = requests.session()
session.cookies = cookielib.LWPCookieJar(filename="cookies.txt")
try:
    session.cookies.load(ignore_discard=True)
except:
    print ("cookie未能加载")

agent = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36"
header = {
    "HOST": "www.zhihu.com",
    "Referer": "https://www.zhihu.com",
    "User-Agent": agent,
    'Connection': 'keep-alive'
}

def is_login():
    #通过个人中心页面返回状态码来判断是否为登录状态
    inbox_url = "https://www.zhihu.com/question/56250357/answer/148534773"
    response = session.get(inbox_url, headers=header, allow_redirects=False)
    if response.status_code != 200:
        return False
    else:
        return True


def get_xsrf():
    response = requests.get("https://www.zhihu.com", headers = header)
    cookies_text = response.cookies
    return cookies_text["_xsrf"]


def get_index():
    response = session.get("https://www.zhihu.com", headers=header)
    with open("index_page.html", "wb") as f:
        f.write(response.text.encode("utf-8"))
    print ("ok")


def get_signature(time_str):
    # 生成signature,利用hmac加密
    # 根据分析之后的js，可发现里面有一段是进行hmac加密的
    # 分析执行加密的js 代码，可得出加密的字段，利用python 进行hmac几码
    h = hmac.new(key='d1b964811afb40118a12068ff74a12f4'.encode('utf-8'), digestmod=sha1)
    grant_type = 'password'
    client_id = 'c3cef7c66a1843f8b3a9e6a1e3160e20'
    source = 'com.zhihu.web'
    now = time_str
    h.update((grant_type + client_id + source + now).encode('utf-8'))
    return h.hexdigest()


def get_identifying_code(headers):
    # 判断页面是否需要填写验证码
    # 如果需要填写则弹出验证码，进行手动填写

    # 请求验证码的url 后的参数lang=en，意思是取得英文验证码
    # 原因是知乎的验证码分为中文和英文两种
    # 中文验证码是通过选择倒置的汉字验证的，破解起来相对来说比较困难，
    # 英文的验证码则是输入验证码内容即可，破解起来相对简单，因此使用英文验证码
    response = session.get('https://www.zhihu.com/api/v3/oauth/captcha?lang=en', headers=headers)
    # 盘但是否存在验证码
    r = re.findall('"show_captcha":(\w+)', response.text)
    if r[0] == 'false':
        return ''
    else:
        response = session.put('https://www.zhihu.com/api/v3/oauth/captcha?lang=en', headers=header)
        show_captcha = json.loads(response.text)['img_base64']
        with open('captcha.jpg', 'wb') as f:
            f.write(base64.b64decode(show_captcha))
        im = Image.open('captcha.jpg')
        im.show()
        im.close()
        captcha = input('输入验证码:')
        session.post('https://www.zhihu.com/api/v3/oauth/captcha?lang=en', headers=header,
                     data={"input_text": captcha})
        return captcha


def zhihu_login(account, password):
    #知乎登录
    if re.match("^1\d{10}", account):
        print("手机号码登录")
        post_url = "https://www.zhihu.com/api/v3/oauth/sign_in"
        time_str = str(int((time.time() * 1000)))
        post_data = {
            "_xsrf": get_xsrf(),
            "username": account,
            "password" : password,
            "grant_type": "password",
            "client_id": "c3cef7c66a1843f8b3a9e6a1e3160e20",
            "source": "com.zhihu.web",
            "timestamp": time_str,
            "captcha": "",
            "lang": "en",
            "ref_source": "homepage",
            "utm_source": "",
            "signature": get_signature(time_str),
            'captcha': get_identifying_code(header)
        }
        response_text = session.post(post_url, data=post_data, headers= header)
        session.cookies.save()


if __name__ == "__main__":
    get_xsrf()
    zhihu_login("13391906313", "skqsjh123")