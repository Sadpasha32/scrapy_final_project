from collections.abc import Iterable

import scrapy
from parsel import Selector
from playwright.async_api import Page
from scrapy import Request
from scrapy.http import Response
from ..items import NewsItem


def should_abort_request(request):
    return "yandex" in request.url or "ya" in request.url or "google" in request.url or "smi2" in request.url


class KpNewsSpider(scrapy.Spider):
    name = "kp_news"
    allowed_domains = ["kp.ru"]
    required_articles_count = 1000
    total_scanned_articles = 0

    custom_settings = {
        "PLAYWRIGHT_ABORT_REQUEST": should_abort_request,
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "MONGO_URI": "mongodb://admin:admin@localhost:27017",
        "MONGO_DATABASE": "admin",
        "ITEM_PIPELINES": {
            'project.pipelines.PhotoDownloaderPipeline': 100,
            'project.pipelines.MongoPipeline': 300,
        },
        "CLOSESPIDER_ITEMCOUNT": 1000
    }

    def start_requests(self) -> Iterable[Request]:
        yield scrapy.Request(
            url="https://www.kp.ru/online/",
            meta={"playwright": True, "playwright_include_page": True},
        )

    async def parse(self, response: Response):
        page: Page = response.meta["playwright_page"]
        while self.total_scanned_articles < self.required_articles_count:
            page_selector = Selector(await page.content())
            articles = page_selector.xpath("//a[@class='sc-1tputnk-2 drlShK']/@href")
            articles = articles[-25:]
            for article in articles:
                yield scrapy.Request(url=response.urljoin(str(article)), callback=self.parse_article)
            await page.locator(selector="//button[@class='sc-abxysl-0 cdgmSL']").click(position={"x": 176, "y": 26.5})
            await page.wait_for_timeout(10000)
            self.total_scanned_articles += len(articles)
            del articles
        await page.close()

    def parse_article(self, response: Response):
        self.logger.info(response.url)
        try:
            item = NewsItem()
            item["title"] = response.xpath("//h1/text()").get().strip()
            item["description"] = response.xpath("//div[@class='sc-j7em19-4 nFVxV']/text()").get().strip()
            item["article_text"] = "".join(response.xpath("//p[@class='sc-1wayp1z-16 dqbiXu']/text()").getall())
            item["publication_datetime"] = response.xpath("//span[@class='sc-j7em19-1 dtkLMY']/text()").get()
            photo_url = response.xpath("//img[@class='sc-foxktb-1 cYprnQ']/@src").get()
            if photo_url:
                item["header_photo_url"] = photo_url
            item["keywords"] = response.xpath("//div[@class='sc-j7em19-2 dQphFo']/a/text()").getall()
            item["authors"] = response.xpath("//a[@class='sc-1jl27nw-4 fsKCGr']/span/text()").getall()
            item["source_url"] = response.url
            return item
        except AttributeError as e:
            self.logger.warning(e)
            return
