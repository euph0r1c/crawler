from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, urlunparse
import urllib.request as Req
import urllib.error
import threading
import logging
import collections
from source import CrawlerExceptions

INDEX = ['index']


class Crawler(threading.Thread):
    url_lock = threading.Lock()
    url_tuple = collections.namedtuple('Url', 'position, url_name')

    def __init__(self, number, urls_queue, res_urls_queue, all_urls, depth, log):
        super().__init__()
        self.number = number
        self.urls_queue = urls_queue
        self.res_url_queue = res_urls_queue
        self.depth = depth
        self.all_urls = all_urls
        self.log = log

    def run(self):
        while True:
            try:
                url = self.urls_queue.get()
                self.process(url)
            finally:
                self.urls_queue.task_done()

    def process(self, url):
        # url is a tuple: (position, url); position in 0 .. depth - 1
        try:
            # set of local urls
            urls = set()

            # check that the crawler is still lower then depth level
            if url.position < self.depth:
                url_handler = Req.urlopen(url.url_name)
                html_from_url = url_handler.read()
                html_souped = BeautifulSoup(html_from_url)

                for a in html_souped.find_all('a'):
                    parsed_url = urlparse(a.get('href'))
                    try:
                        if parsed_url.netloc:
                            if not parsed_url.scheme:
                                raise ValueError('Scheme not given for url ' + parsed_url.netloc)
                            url_to_crawl = urlunparse(parsed_url)
                        elif parsed_url.path:
                            if parsed_url.query:
                                url_to_crawl = urljoin(url.url_name, urlunparse(parsed_url))
                            else:
                                for index in INDEX:
                                    if index in parsed_url.path:
                                        #logging.info(
                                            # 'Duplicate url: ' + str(urljoin(url.url_name, urlunparse(parsed_url))))
                                        #break
                                        raise CrawlerExceptions.IndexPageException(
                                            'Duplicate url: ' + str(urljoin(url.url_name, urlunparse(parsed_url))))
                                else:
                                    url_to_crawl = str(urljoin(url.url_name, parsed_url.path))
                        else:
                            continue
                    except CrawlerExceptions.IndexPageException as indexpageerror:
                        if self.log:
                            logging.info(indexpageerror)
                        pass

                    with self.url_lock:
                        is_new_url = url_to_crawl not in self.all_urls

                    if is_new_url:
                        with self.url_lock:
                            self.all_urls.add(url_to_crawl)

                        urls.add(url_to_crawl)
                        url_temp = self.url_tuple(url.position + 1, url_to_crawl)
                        self.urls_queue.put(url_temp)
                    else:
                        continue

                # check that the set of urls is not empty
                if urls:
                    self.res_url_queue.put((self.number, url.url_name, url.position, urls))

        except urllib.error.URLError as urlerror:
            if self.log:
                logging.error(urlerror)
            pass
        except ValueError as valerr:
            if self.log:
                logging.error(valerr)
            pass