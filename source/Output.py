from abc import ABCMeta, abstractmethod
import queue
import logging
from pymongo import MongoClient

NUMBER, URL_NAME, POSITION, URLS = range(4)


class Output(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, arg_queue):
        assert isinstance(arg_queue, queue.Queue), "Var arg_queue is not of Queue type!!!"
        self.__queue = arg_queue

    @property
    def output_queue(self):
        return self.__queue

    @output_queue.setter
    def output_queue(self, arg_queue):
        self.__queue = arg_queue

    @abstractmethod
    def write_data(self, url):
        pass

    def process(self):
        while True:
            try:
                url = self.output_queue.get()
                if url:
                    self.write_data(url)
            finally:
                self.output_queue.task_done()


class FileOutput(Output):
    def __init__(self, res_urls_queue, filename=None):
        super().__init__(res_urls_queue)
        self.file = filename if filename else '../data/urls.txt'
        self.writemod = 'w'
        self.encoding = 'utf8'
        self.stream = open(self.file, self.writemod, encoding=self.encoding)

    def write_data(self, url_info):
        try:
            self.stream.write("{0}Child urls from url {1} (depth {2}):"
                              "\n\t{3}\n".format(url_info[NUMBER], url_info[URL_NAME], url_info[POSITION],
                                                 "\n\t".join(url_info[URLS])))
        except TypeError:
            logging.info("Can't write info from " + url_info[URL_NAME])
            pass

    def close(self):
        self.stream.close()


class StdOutput(Output):
    def __init__(self, res_urls_queue):
        super().__init__(res_urls_queue)

    @staticmethod
    def write_data(url_info):
        print("{0}Child urls from url {1} (depth {2}):"
              "\n\t{3}\n".format(url_info[NUMBER], url_info[URL_NAME], url_info[POSITION],
                                 "\n\t".join(url_info[URLS])))


class DbOutput(Output):
    def __init__(self, res_urls_queue):
        super().__init__(res_urls_queue)
        self.client = MongoClient()
        self.base = self.client.crawler
        self.collection = self.base.output

    def write_data(self, url_info):
        url_to_insert = {"parent_url": url_info[URL_NAME],
                         "position": url_info[POSITION],
                         "urls": list(url_info[URLS])}
        self.collection.insert(url_to_insert)









