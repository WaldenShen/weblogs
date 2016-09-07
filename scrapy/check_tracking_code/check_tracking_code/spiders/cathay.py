# -*- coding: utf-8 -*-
import scrapy
import urlparse

from scrapy.http import Request
from check_tracking_code.items import CheckTrackingCodeItem

PATTERN_1 = "celebrusinsert.js"
PATTERN_2 = "https://www.cathaybk.com.tw/cathaybk/card/card/costco/js/script.js"
PATTERN_3 = "analytics.js"

DONE = set()

class CathaySpider(scrapy.Spider):
    name = "cathay"
    allowed_domains = ["www.cathaybk.com.tw",
                       "www.mybank.com.tw",
                       "www.cathayholdings.com"]
    start_urls = (
        "https://www.cathaybk.com.tw/cathaybk/",
    )

    def parse(self, response):
        for a_tag in response.selector.xpath("//a/@href").extract():
            if ".pdf" not in a_tag and ("http" in a_tag or ".asp" in a_tag):
                url = urlparse.urljoin(response.url, a_tag)

                if url not in DONE:
                    end_idx = url.find("?")
                    if end_idx == -1:
                        end_idx = len(url)

                    yield Request(url[:end_idx], callback=self.parse)

        has_tracking_code = False

        body = response.body.lower()
        if PATTERN_1 in body:
            has_tracking_code = True

        if not has_tracking_code:
            for js in response.selector.xpath("//script/@src").extract():
                url_js = urlparse.urljoin(response.url, js)

                if url_js == PATTERN_2:
                    has_tracking_code = True

                    break

        item = CheckTrackingCodeItem()
        item["url"] = response.url
        item["has_tracking_code"] = has_tracking_code

        DONE.add(response.url)

        yield item
