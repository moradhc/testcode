import sys
import os
import traceback
import multiprocessing
from contextlib import closing
import urllib.parse
from heapq import heappush, heappop
from multiprocessing import Lock, pool
from time import sleep, time
import random
from argparse import ArgumentParser, RawDescriptionHelpFormatter
import urllib.parse
import bs4
import bs4.element
import re
import pickle
import requests
import requests.adapters
import logging
import logging.config
import Amazoner.settings
sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))

# noinspection PyUnresolvedReferences
from proxyquest import ProxyQuest
# noinspection PyUnresolvedReferences
from excepciones import UndefinedFormatException, TooManyRetries, NonExistingPage
# noinspection PyUnresolvedReferences
import debug_interactivo


class Priority(object):
    NODE_PAGE = 1
    LEAF_PAGE = 2
    ITEM_PAGE = 4
    REFURBISH = 3


class PageLoader(object):
    """
        Multiprocess page loader
    """
    class __Context(object):
        def __init__(self, instance, dispose=None):
            self.instance = instance
            self.dispose = dispose

        def __enter__(self):
            return self.instance

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.dispose and self.dispose()

        def __lt__(self, other):
            return id(self) < id(other)

    class Task(object):
        def __init__(self, priority, task):
            self.priority = priority
            self.task = task

        def __lt__(self, other):
            return self.priority < other.priority or id(self) < id(other)

    def __init__(self, threads, session_factory):
        self.threads = threads
        self.pool = pool.ThreadPool(threads)
        self.mutex = Lock()
        self.session_factory = session_factory
        self.tasks = []
        self.counter = 0
        self.response = []
        self.reloads = 10
        self.log = logging.getLogger('scanner')

    def _open_context(self, instance):
        return self.__Context(instance, self._close_context)

    def _close_context(self):
        with self.mutex:
            self.counter -= 1

    def get_tasks(self, tasks):
        while True:
            sleep(0)

            empty = len(tasks) == 0
            done = self.counter == 0

            if empty:
                if done:
                    break
                sleep(0.5)
                continue

            with self.mutex:
                task = tasks.pop()
            yield task

    def load(self, priority, url, **kwargs):
        with self.mutex:
            try:
                heappush(self.tasks, PageLoader.Task(priority, (url, kwargs)))
                self.counter += 1
            except TypeError:
                pass

    # noinspection PyBroadException
    def get(self, tasks):
        try:
            while True:
                try:
                    session = self.session_factory()
                    break
                except:
                    sleep(1)
                    continue

            while True:
                sleep(0)

                if self.counter == 0 and len(tasks) == 0:
                    return

                self.mutex.acquire()
                if len(tasks) > 0:
                    task = heappop(tasks)
                else:
                    self.mutex.release()
                    sleep(1)
                    continue
                self.mutex.release()

                url, kwargs = task.task

                try:
                    response = session.get(url)

                    with self.mutex:
                        self.response.append(self._open_context((response, kwargs)))
                except:

                    with self.mutex:
                        self.counter -= 1
                    if task.priority < self.reloads:
                        self.load(task.priority + 1, url, **kwargs)
                    else:
                        self.log.warning("Can't load page(limit of retries): " + url)

        except Exception as ex:
            self.log.exception(ex)

    def __iter__(self):
        self.pool.imap_unordered(self.get, [self.tasks for _ in range(self.threads)])
        yield from self.get_tasks(self.response)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool and self.pool.terminate()


class PageClassifier(object):
    def __init__(self, node, leaf, item, refurbish):
        self.node = node
        self.leaf = leaf
        self.item = item
        self.refurbish = refurbish

    def __call__(self, soup):
        # Locate refinements section
        refinement = soup.find('div', class_='categoryRefinementsSection')
        logo_rea = soup.find('h2', class_='mLogo')

        # Locate refinements section
        if refinement or logo_rea:
            subcategory = refinement.find('span', class_='refinementLink')
            if subcategory:
                return self.node
            else:
                return self.leaf

        # Item page
        if soup.find('h1', id='title'):
            return self.item

        # Refurbished page
        if soup.find('div', class_='olpOfferList'):
            return self.refurbish

        # New version of page (UserAgent header require)
        left_nav = soup.find('div', id='leftNav')
        if left_nav:
            assert isinstance(left_nav, bs4.Tag)
            bold_links = left_nav.select('ul h4.a-text-bold')
            is_leaf = [tag for tag in bold_links if not tag.find_next_sibling()]

            if not is_leaf:
                """
                [Women]
                    Clothing
                    Shoes
                    [Jewelry] <----------- node page
                        Fine
                        Fashion
                    Watches
                """
                return self.node
            else:
                """
                [Women]
                    Clothing
                    Shoes
                    Jewelry
                    [Watches]
                        [Wrist Watches] <- leaf page
                        Watch Bands
                """
                return self.leaf
        return None


class Parser(object):
    def __init__(self, log, load, report):
        self.log = log
        self.load = load
        self.report = report

    def parse(self, soup, **kwargs):
        raise NotImplementedError


class PremiumFilter(object):
    def __init__(self, load):
        self.load = load

    @staticmethod
    def __verify_no_check(src):
        """Utility function to check if src contains 'checkbox_unselected_enabled'

        :param str src: parsed tag
        :rtype: boolean
        :return: True when img is found
        """
        return src and 'checkbox_unselected_enabled' in src

    @staticmethod
    def __verify_prime_img(src):
        """Utility function to check if src contains 'prime-check-badge'

        :param str src: parsed tag
        :rtype: boolean
        :return: True when img is found
        """
        return src and 'prime-check-badge' in src

    def __call__(self, soup):
        """Looks for Premium check and loads page turning the filter on if not already on.

        :param bs4.BeautifulSoup soup: original parsed page
        :rtype: bs4.BeautifulSoup
        :return: original soup if filter was on or new if it has been reloaded
        """
        for tag in soup.find_all('li', class_='refinementImage'):
            img = tag.find('img', src=self.__verify_no_check)
            if img and img.parent.find('img', src=self.__verify_prime_img):
                self.load(Priority.NODE_PAGE, img.parent.attrs['href'])
                return False

        return True


class ItemParser(Parser):
    __re_stars = re.compile(r'a-star-([0-5](-5)?)')

    def stars(self, string):
        """Looks for and gets valoration in soup.
        :param string: str to look into.
        :return None|str: stars valoration in string format with one decimal with point separator.
        """
        match = self.__re_stars.match(str(string))
        if match:
            return match.groups()[0].replace('-', '.')

    __re_price = re.compile(r'(\d+[\s,.]+(?:\d{3}(?:[\s,.]+|))*)(?:[\s,.]?(\d+))?')
    __number_cleaning = str.maketrans('1234567890', '1234567890', ' .,\xa0')

    def price(self, string):
        """Get price from string

        :param string: str
        :return str: Extracted price, as string and decimal point
        """
        if not string:
            self.log.error('Price element not found')
            return None

        price_match = self.__re_price.search(str(string))
        if not price_match:
            self.log.error('Price match not found at {}'.format(string))
            return None
        price = '{}.{}'.format(price_match.group(1).translate(self.__number_cleaning), price_match.group(2) or '00')
        return price

    __re_not_available = re.compile(
        'No disponible|Pas de stock|Nicht auf Lager|Attualmente non disponibile|Temporarily out of stock')

    __product_page_format = '/dp/{}'

    @staticmethod
    def product_page_url(asin):
        return ItemParser.__product_page_format.format(asin)

    def parse(self, soup, **kwargs):
        # Skips ads
        if soup.find('h5', class_='s-sponsored-list-header'):
            return
        try:
            asin = soup.attrs['data-asin']
        except KeyError:
            return

        self.log.debug('Processing item {}'.format(asin))

        description = soup.find('a', class_='s-access-detail-page')
        if not description:
            self.log.error('Item link not found at {}'.format(soup.prettify()))
            return

        if kwargs.get('refurbished', False):
            # return self.load(Priority.ITEM_PAGE, description.attrs['href'], kwargs)
            return self.load(Priority.ITEM_PAGE, self.__product_page_format.format(asin), kwargs)

        description = description.attrs['title']

        # Image
        image = soup.find('img', class_='s-access-image')
        if not image:
            image = None
            self.log.warning('Image not found at {}'.format(soup.prettify()))
        else:
            image = image.attrs['src']

        # Valoration
        valoration = self.stars(str(soup.find('i', class_='a-icon-star')))

        # Price
        price_tag = soup.find('span', class_='a-color-base')
        price_tag = price_tag or soup.select_one('span.a-offscreen')
        price_tag = price_tag or soup.select_one('span.a-color-price')
        if price_tag:
            price = self.price(str(price_tag))
            if price is None:
                return

            # Verifies plus. Anchor sibling has to be <i> with class a-icon-prime
            if not soup.select('i.a-icon-prime'):
                return

            # Verifies availability. div next to price has an span with localized message
            next_div = price_tag.parent.parent.find_next_sibling('div')
            available = True
            if next_div:
                if self.__re_not_available.search(str(next_div.find('span').string)):
                    available = False

            self.report(asin, 'N', description, price, available, image, valoration)


class ItemPageParser(ItemParser):
    __re_node = re.compile(r'node=([0-9]+)')
    __detail_page_format = '/gp/offer-listing/{}/ref=olp_f_primeEligible?ie=UTF8&f_primeEligible=true'

    @staticmethod
    def refurbish_page_url(asin):
        return ItemPageParser.__detail_page_format.format(asin)

    def parse(self, soup, refurbished=False, **kwargs):
        asin = soup.find('link', rel='canonical').attrs['href'][-10:]
        self.log.debug('Processing item {}'.format(asin))

        description = soup.find('span', id='productTitle')
        if not description:
            self.log.error('Item description not found at {}'.format(soup.prettify()))
            return
        description = description.text

        # Image
        image = soup.find('img', id='landingImage') or soup.find('img', id='imgBlkFront')
        if not image:
            image = None
            self.log.warning('Image not found at {}'.format(soup.prettify()))
        else:
            image = image.attrs.get('data-old-hires', None)

        # Valoration
        valoration = self.stars(str(soup.find('i', class_='a-icon-star')))

        # Hierarchy
        crumbs = soup.find('div', id='wayfinding-breadcrumbs_feature_div')
        if not crumbs:
            hierarchy = None
        else:
            hierarchy = list()
            assert isinstance(crumbs, bs4.element.Tag)
            for link in crumbs.find_all('a', class_='a-link-normal'):
                assert isinstance(link, bs4.element.Tag)
                node = self.__re_node.search(link.attrs['href'])
                if node:
                    hierarchy.append((node.group(1), link.string.strip()))
                else:
                    hierarchy = None
                    self.log.warning('Node identifier not found at {}'.format(link.prettify()))
                    break

        if refurbished:
            return self.load(Priority.REFURBISH, self.__detail_page_format.format(asin), dict(
                asin=asin,
                description=description,
                image=image,
                valoration=valoration,
                hierarchy=hierarchy,
                **kwargs
            ))

        price_tag = soup.find('span', id='priceblock_ourprice')
        if price_tag:
            price = self.price(price_tag.string)

            # Availability check. Looks into div id=availability one span and verifies class.
            availability_tag = soup.find('div', id='availability')
            available = bool(availability_tag.find('span', class_='a-color-success'))

            self.report(asin, 'N', description, price, available, image, valoration, hierarchy)


class LeafPageParser(Parser):
    re_replace_page1 = re.compile(r'sr_pg_[0-9]+')
    re_replace_page2 = re.compile(r'&page=[0-9]+')

    def _prepare_page_format(self, url):
        """Replace jump page numbers in URL with parameters and returns.

        :param str url: jump page url
        :return str: the same url, parametrized
        """
        url = self.re_replace_page1.sub(r'sr_pg_{0}', url)
        return self.re_replace_page2.sub(r'&page={0}', url)

    re_results = re.compile(r'^\S+ \S+ ([0-9]*[\s.,]?[0-9]*[\s.,]?[0-9]+) [^0-9]')
    number_cleaning = str.maketrans('1234567890', '1234567890', ' .,\xa0')

    def _total_results(self, soup):
        """In a results page, get the total results number.

        :param bs4.BeautifulSoup soup:
        :rtype: int
        :return: total results found. If not found, returns 0.
        """
        results = soup.find('h2', id='s-result-count')
        if not results:
            self.log.debug('No results found in h2 with id=s-result-count')
            return 0
        number = self.re_results.match(str(results.text))
        if number:
            self.log.debug('Found {} results at {}'.format(number.group(1), results.text))
            return int(number.group(1).translate(self.number_cleaning))
        # self.log.info('No results found at "{}" for {}'.format(results.text, self.progress_url))
        return 0

    def parse(self, soup, pagination=True, **kwargs):
        self.log.debug('Processing as leaf')
        if not PremiumFilter(self.load)(soup):
            return

        # Pagination
        if pagination:
            kwargs['pagination'] = False
            # results = self._total_results(soup)
            # pages = list(range(2, (results - 1) // 24 + 1))

            # noinspection PyBroadException
            try:
                disabled_page = soup.select('#pagn .pagnDisabled')
                last_active_page = soup.select('#pagn .pagnLink')
                page_count = int((disabled_page or last_active_page)[-1].string)
            except:
                page_count = 1
            pages = list(range(2, page_count + 1))

            next_page = soup.find('a', id='pagnNextLink')
            if next_page:
                next_page = self._prepare_page_format(next_page.attrs['href'])

                random.shuffle(pages)
                for page_index in pages:
                    self.load(Priority.LEAF_PAGE, next_page.format(page_index), kwargs)

        # Process items
        item_parser = ItemParser(self.log, self.load, self.report)
        for index, item in enumerate(soup.find_all('li', class_='s-result-item')):
            item_parser.parse(item, **kwargs)


class NodePageParser(Parser):
    def parse(self, soup, **kwargs):
        """Page including category distribution. Crawls every category if current category exceeds 9600 results.

                :param bs4.BeautifulSoup soup:
                :return:
                """
        self.log.debug('Processing as node')
        if not PremiumFilter(self.load)(soup):
            return

        # if 0 < self._total_results(soup) <= 9600:
        #     self.log.debug('Less than 9600 items, processing as leaf')
        #     return self._process_leaves(soup)

        container = soup.find('div', class_='categoryRefinementsSection')
        if container:
            for span in container.find_all('span', class_='refinementLink'):
                # Parent is an "a href" with next level address
                self.load(Priority.NODE_PAGE, span.parent.attrs['href'], kwargs)
        else:
            # <!> New version of page
            nav = soup.find('div', id='leftNav')
            bold_links = nav.select('h4.a-text-bold a.a-text-bold')

            container = [tag.parent.parent for tag in bold_links if tag.parent.find_next_sibling()]
            if not container:
                return

            for link in container[-1].select('span a'):
                self.load(Priority.NODE_PAGE, link.attrs['href'], kwargs)


class RefurbishParser(ItemParser):
    @staticmethod
    def quality_in_text(quality_tag, quality_dict):
        if quality_dict is None:
            return None

        text = ' '.join(quality_tag.string.split())
        for description, code in quality_dict.items():
            if description == text:
                return code

        return None

    def parse(self, soup, asin=None, description=None, image=None, valoration=None, hierarchy=None, **kwargs):
        for sale in soup.find_all('div', class_='olpOffer'):
            price_tag = sale.find('span', class_='olpOfferPrice')
            price = self.price(price_tag)
            if price is None:
                continue

            quality_string = kwargs.get('quality_string', None)
            quality = self.quality_in_text(sale.find('span', class_='olpCondition'), quality_string)
            if not quality:
                continue

            self.report(asin, quality, description, price, True, image, valoration, hierarchy)
            return

        self.log.warning("Product '{}' has not offers".format(asin))


class Reporter(object):
    def __init__(self, log, buffer_size, progress_url):
        self.log = log
        self.buffer = []
        self.buffer_size = buffer_size
        self.progress_url = progress_url

        self.count = 0
        self.start = time()

    def __call__(self, asin, quality, description, price, available, image, valoration, hierarchy=None):
        """Send data update

        :param str quality: Quality level id, N: new, A-E: degree, A best
        :param str asin: Amazon item id
        :param str description: Item description
        :param str price: Price as number with two decimals and point separator
        :param boolean available: Stock available
        :param None|str image: Image URL for item
        :param None|str valoration: Valoration with optional decimal digit
        :param None|list((str, str)) hierarchy: tuple list with node id and node name
        :return:
        """
        if not image or len(image) >= 128:
            image = ''

        self.log.debug('{}:"{}" at {}. Avail:{}'.format(asin, description, price, available))

        self._append_buffer((asin, quality, description, price, available, image, valoration, hierarchy))
        self.count += 1
        self.log.debug(str((asin, quality, '...', price, available, '...', valoration, hierarchy)))

    def _flush_buffer(self):
        """Send data from prices buffer to notification address and flushes buffer

        :return:
        """
        if self.buffer:
            self.log.info('Sending progress for {} items at {}'.format(len(self.buffer), self.progress_url))

            response = requests.put(self.progress_url, data=pickle.dumps(self.buffer, pickle.HIGHEST_PROTOCOL))
            if response.status_code != 200:
                self.log.warning("Server has an error: " + str(response.content))
        self.buffer.clear()

    def _append_buffer(self, product_data):
        """Appends tuple to send buffer and verifies sending limit.

        :param () product_data: data to send
        """
        self.buffer.append(product_data)
        if len(self.buffer) >= self.buffer_size:
            self._flush_buffer()

    def ending(self):
        """Makes a flush and send ending of scan
        """
        self._flush_buffer()
        self.log.info('Sends ending notification to {}terminacion'.format(self.progress_url))

        requests.put(self.progress_url + 'terminacion')

    def error(self, error_text):
        """Makes a flush and send ending of scan with error

        :param str error_text: Exception to notify
        """
        self._flush_buffer()
        self.log.error('Send error ending notification to {}error'.format(self.progress_url))
        requests.put(self.progress_url + 'error', data=pickle.dumps(error_text[-255:], pickle.HIGHEST_PROTOCOL))


class Scanner(object):
    BUFFER_SIZE = 5
    THREAD_COUNT = 20

    def __init__(self, log, country, progress_url, target_url, proxy_url, refurbished, captcha=''):
        self.log = log
        self.country = country
        self.progress_url = progress_url
        self.target_url = target_url
        self.proxy_url = proxy_url
        self.refurbished = refurbished
        self.captcha = captcha and captcha.split(':')
        self.session = None
        self.report = Reporter(log, self.BUFFER_SIZE, progress_url)
        # self.quality_string = dict()

    def run(self):
        with PageLoader(self.THREAD_COUNT, self.__session) as loader:
            classify_page = PageClassifier(
                NodePageParser,
                LeafPageParser,
                ItemPageParser,
                RefurbishParser
            )

            def load(priority, url, params):
                loader.load(priority, urllib.parse.urljoin(self.target_url, url), **(params or {}))

            quality_strings = self.quality_strings()

            load(0, self.target_url, {
                'refurbished': self.refurbished,
                'quality_string': quality_strings
            })

            try:
                for context in loader:
                    with context as page_args:
                        page, params = page_args
                        page_class = classify_page(page)
                        if page_class is not None:
                            # self.log.info("Page parsing as {}".format(page_class.__name__))
                            page_class(self.log, load, self.report).parse(page, **params)

                self.report.ending()
            except Exception:
                self.report.error(traceback.format_exc())

    def quality_strings(self):
        """Call to get quality texts from server
        """
        retries = 5
        while retries > 0:
            try:
                r = requests.get(self.progress_url + 'textos', timeout=15)
                if r.status_code == 200:
                    return pickle.loads(r.content)
            except (requests.Timeout, requests.HTTPError):
                self.log.exception(
                    'On getting quality prices for amazon.{}'.format(self.country))
            sleep(180)
            retries -= 1
        if retries <= 0:
            self.log.error('Retries exceeded to get quality texts from server.')
            raise TooManyRetries()

    @staticmethod
    def __validator(soup):
        """Checks if soup is valid (without captchas)

        :param bs4.BeautifulSoup soup: soup to validate
        :return boolean: True if has no captcha
        """
        return (soup.find('title', string=re.compile('Amazon CAPTCHA|(Rob|B)ot Check')) is None and
                soup.find('p', string=re.compile('Maximum number of open connections reached')) is None)

    def __session(self):
        return ProxyQuest(self.country, True, self.proxy_url, self.__validator, self.captcha)


def main(argv=None):
    """Command line processing"""
    if argv:
        sys.argv.extend(argv)

    # Expands environment in arguments
    for (pos, value) in enumerate(sys.argv):
        sys.argv[pos] = os.path.expandvars(value)

    # Parser config
    parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter,
                            fromfile_prefix_chars="@")
    parser.add_argument('country', help='Domain or country label from Amazon to classify proxies')
    parser.add_argument('progress_url', help='Base URL to report progress')
    parser.add_argument('target_url', help='URL to begin crawling')
    parser.add_argument('proxy_url', help='Proxies repository URL')
    parser.add_argument('--usado', action='store_true', help='Activate refurbished mode')
    parser.add_argument('--captcha', default='', help='User:Password for DBC')

    args = parser.parse_args()

    logging.config.fileConfig(os.path.join(os.path.dirname(__file__), '..', 'Amazoner', 'logging.conf'))
    log = logging.getLogger('scanner')
    log.debug('Spawned with progress URL ({}) and target URL ({})'.format(args.progress_url, args.target_url))

    # Does somthing
    scanner = Scanner(log, args.country, args.progress_url, args.target_url, args.proxy_url, args.usado, args.captcha)
    # noinspection PyBroadException
    try:
        scanner.run()
    except Exception as e:
        log.exception('Error during scanning of {}'.format(args.progress_url))
        raise e
    return 0

if __name__ == "__main__":
    if not Amazoner.settings.DEBUG:
        debug_interactivo.listen()

    sys.exit(main())


"""

"""