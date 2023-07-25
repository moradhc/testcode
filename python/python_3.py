import logging
import re
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
from price_parser import Price
import lxml.html
import requests
from app import db
from app.models import Product, User

class ZenScrapeClient:
    def __init__(self, api_key, retry_max=10):
        self.api_key = api_key
        self.s = requests.Session()
        self.url = None
        self.retry_it = 0
        self.retry_max = retry_max

    def get(self, url=None):
        headers = {
            "apikey": self.api_key
        }
        if url is None:
            self.retry_it += 1
        else:
            self.url = url
        params = (
            ("url", self.url),
            ("location", 'eu'),
        )
        response = self.s.get('api.com/test', headers=headers, params=params)

        return response

    def retry(self):
        if self.retry_it >= self.retry_max:
            logging.error('Max count o retries ' + str(self.retry_max))
            exit()
        return self.get()


flags = {
    'es': '🇪🇸',
    'uk': '🇬🇧',
    'de': '🇩🇪',
    'fr': '🇫🇷',
    'it': '🇮🇹',
}


def scrape_task():
    users = User.query.all()
    for user in users:
        bot_url = 'https://api.telegram.org/bot{}/'.format(user.token)
        client = ZenScrapeClient(user.api_key)
        for search in user.searches:
            if search.check == False:
                continue
            elif search.check is None:
                search.check = True
            url = search.url
            if 'language=en' not in url:
                url += '&language=en'
            pagination = True
            r = client.get(url)
            while pagination:
                if r.status_code == 200:
                    tree = lxml.html.fromstring(r.text)
                    items = tree.xpath('//div[@data-component-type="s-search-result" and @data-index]')
                    if len(items) == 0:
                        print(datetime.utcnow(), 'zero results found')
                        r = client.retry()
                        continue
                    for item in items:
                        product_id = item.attrib['data-asin']
                        item_name = item.xpath('.//a[@class="a-link-normal a-text-normal"]/span')[0].text
                        item_url = item.xpath('.//a[@class="a-link-normal a-text-normal"]')[0].attrib['href']
                        item_url = urljoin(url, item_url)
                        try:
                            price = Price.fromstring(item.xpath('.//span[@class="a-color-base"]')[0].text)
                            price = float(price.amount)
                        except:
                            logging.warning(f'price not found \npage:\t{url} \nitem\t{item_name}')
                            continue
                        product = Product.query.filter_by(product_id=product_id).first()
                        if product is None:
                            product = Product(
                                title=item_name,
                                url=item_url,
                                product_id=product_id,
                                timestamp=datetime.utcnow(),
                                price=price,
                                search=search.id
                            )
                            db.session.add(product)
                        else:
                            discount = 100 - (price / product.price) * 100
                            if discount >= user.discount:
                                if product.price_tg is None or price < product.price_tg:
                                    cc = urlparse(url).netloc.split('.')[-1]
                                    params = {
                                        'chat_id': user.chat_id,
                                        'text': f'{flags[cc]}{item_name}\n\n'
                                                f'<b>Saved Price:{product.price}€</b>\n\n'
                                                f'<b>New Price {discount:.1f}% {price}€</b>\n\n'
                                                f'<b>Link: </b>{product.url}',
                                        'parse_mode': 'html'
                                    }
                                    requests.post(bot_url + 'sendMessage', params)
                                    product.price_tg = price
                            else:
                                product.price_tg = None
                            if product.timestamp - datetime.utcnow() > timedelta(days=7):
                                product.timestamp = datetime.utcnow()
                                product.price = price
                                product.price_tg = None
                        product.checked = datetime.utcnow()
                        logging.info(str(price) + '\t' + item_name)
                        db.session.commit()

                    next_page = tree.xpath('//li[@class="a-last"]/a')
                    if len(next_page) > 0:
                        url = urljoin(url, next_page[0].attrib['href'])
                        r = client.get(url)
                    else:
                        pagination = False
                else:
                    logging.warning('scraper status code :' + str(r.status_code))
                    r = client.retry()
                    continue

            search.timestamp = datetime.utcnow()
            db.session.commit()


scrape_task()
