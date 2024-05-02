from threading import Thread

from inspect import getsource
from urllib.parse import parse_qs, urlparse
from utils.download import download
from utils import get_logger
import scraper
import time
import xml.etree.ElementTree as ET


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        sitemap_urls = ["https://ics.uci.edu/post-sitemap.xml", "https://cs.ics.uci.edu/page-sitemap.xml"]
        for sitemap in sitemap_urls:
            sitemap_resp = download(sitemap, self.config, self.logger)
            try:
                content = sitemap_resp.raw_response.content
                text_content = content.decode('utf-8')
                root = ET.fromstring(text_content)

                print("sitemap found")
                
                urls = [url.text for url in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc')]
                for url in urls:
                    self.frontier.add_url(url)
            except:
                continue

        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")

            if resp.status == 200:
                scraped_urls = scraper.scraper(tbd_url, resp)
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)
        
        # create report.txt and record statistic
        scraper.report()
