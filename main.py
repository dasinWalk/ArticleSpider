import os
import sys
from scrapy.cmdline import execute

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# execute(["scrapy","crawl","jobbole"])
#execute(["scrapy", "crawl", "chictr"])
# execute(["scrapy", "crawl", "kjjys"])
# execute(["scrapy", "crawl", "kjxwzx"])
# execute(["scrapy", "crawl", "kjxwzx_gwy"])
execute(["scrapy", "crawl", "kxjsb_yw"])