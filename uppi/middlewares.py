"""Scrapy middleware hooks, які лишаються близькими до стандартного шаблону фреймворку."""

from scrapy import signals


class UppiSpiderMiddleware:
    """Базовий spider middleware без додаткової бізнес-логіки поверх Scrapy."""

    @classmethod
    def from_crawler(cls, crawler):
        """Створює middleware і підписує його на сигнали crawler-а."""
        middleware = cls()
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware

    def process_spider_input(self, response, spider):
        """Пропускає response далі без модифікацій."""
        return None

    def process_spider_output(self, response, result, spider):
        """Повертає результати spider-а без змін."""
        for item in result:
            yield item

    def process_spider_exception(self, response, exception, spider):
        """Дає Scrapy змогу обробити виняток стандартним шляхом."""
        return None

    def process_start_requests(self, start_requests, spider):
        """Прокидає start requests без додаткових змін."""
        for request in start_requests:
            yield request

    def spider_opened(self, spider):
        """Логує факт відкриття spider-а."""
        spider.logger.info("Spider opened: %s" % spider.name)


class UppiDownloaderMiddleware:
    """Базовий downloader middleware без зміни поточного request/response flow."""

    @classmethod
    def from_crawler(cls, crawler):
        """Створює downloader middleware і підписує його на сигнали."""
        middleware = cls()
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware

    def process_request(self, request, spider):
        """Не втручається в request і повертає None."""
        return None

    def process_response(self, request, response, spider):
        """Повертає response без змін."""
        return response

    def process_exception(self, request, exception, spider):
        """Лишає стандартний ланцюг обробки винятків Scrapy."""
        return None

    def spider_opened(self, spider):
        """Логує факт відкриття spider-а."""
        spider.logger.info("Spider opened: %s" % spider.name)
