import argparse
import logging
import queue
import threading
import collections
from source import Crawler, Output, CrawlerExceptions


def parse_options():
    parser = argparse.ArgumentParser(
        usage=("usage: %(prog)s [options] url\n"
               "crawling all urls from given url"))
    parser.add_argument("-t", "--threads", nargs='?', dest="nthreads", default=7,
                        type=int,
                        help=("the number of threads to use (1..20) "
                              "[default %default]"))
    parser.add_argument("-l", "--log", dest="log",
                        default=False, action="store_true",
                        help="use logging")
    parser.add_argument("-d", "--depth", dest="depth", nargs='?', default=3,
                        type=int,
                        help="crawling depth")
    parser.add_argument("-o", "--output", dest="output", nargs='?', default='s',
                        type=str,
                        help=("results output:"
                              "\n\t's': stdout"
                              "\n\t'f': file"
                              "\n\t'd': database"))
    parser.add_argument('url', nargs='?')
    args = parser.parse_args()
    if not (1 <= args.nthreads <= 20):
        parser.error("thread count must be 1..20")
    if not 1 <= args.depth <= 100:
        parser.error("depth must be in (1, 100)")

    # checking url
    if not args.url.startswith('http://') and not args.url.startswith('https://'):
        args.url = 'http://' + args.url

    return args


def main():
    args = parse_options()
    logging.basicConfig(filename="../log/crawler.log", filemode='w', level=logging.INFO, datefmt="%H:%M:%S",
                        format='%(asctime)s %(levelname)s: %(message)s')
    if args.log:
        log_message = ("\n\tnumber of threads = {0}"
                       "\n\tdepth = {1}"
                       "\n\turl = {2}"
                       "\n\toutput = {3}").format(args.nthreads, args.depth, args.url, args.output)
        logging.info(log_message)
    
    if args.log:
        logging.info("Creating {0} threads".format(args.nthreads))
    urls_queue = queue.Queue()

    # res_urls_queue contains a tuple (thread number, parent url name, parent crawler depth, set of child urls)
    res_urls_queue = queue.Queue()
    url = collections.namedtuple('Url', 'position, url_name')
    start_url = url(position=0, url_name=args.url)
    all_urls = set()
    all_urls.add(start_url.url_name)

    for i in range(args.nthreads):
        number = "{0} ".format(i + 1) if args.log else ""
        crawler = Crawler.Crawler(number, urls_queue, res_urls_queue, all_urls, args.depth, args.log)
        crawler.daemon = True
        crawler.start()

    if args.output == 'f':
        output = Output.FileOutput(res_urls_queue)
    elif args.output == 's':
        output = Output.StdOutput(res_urls_queue)
    elif args.output == 'd':
        output = Output.DbOutput(res_urls_queue)
    else:
        raise CrawlerExceptions.OutputTypeException("unrecognized type of output")

    res_thread = threading.Thread(target=lambda: output.process())
    res_thread.daemon = True
    res_thread.start()

    urls_queue.put(start_url)
    urls_queue.join()
    res_urls_queue.join()

    if args.output == 'f':
        output.close()


if __name__ == '__main__':
    main()








