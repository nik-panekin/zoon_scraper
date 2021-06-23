"""This script performs data scraping from https://zoon.ru/ site. It retrieves
essential information for all the objects for category 'entertament'.

The results are saved to a CSV file.
"""
import re
import sys
import csv
import time
import json
import logging
from signal import signal, SIGINT
from urllib.parse import unquote, urlparse

import requests
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag

from scraping_utils import setup_logging, get_response, FATAL_ERROR_STR

TEMPLATE_SUBST = '[SUBDOMAIN]'
BASE_URL_TEMPLATE = f'https://{TEMPLATE_SUBST}zoon.ru/'
# SEARCH_URL = 'entertainment/type/batutnyj_tsentr/'
SEARCH_URL = 'entertainment/'
API_URL = '?action=listJson&type=service'

SUBDOMAINS = {
    'msk': {'city': 'Москва', 'region': 'Москва'},
    'spb': {'city': 'Санкт-Петербург', 'region': 'Санкт-Петербург'},
    'nsk': {'city': 'Новосибирск', 'region': 'Новосибирская область'},
    'ekb': {'city': 'Екатеринбург', 'region': 'Свердловская область'},
    'kazan': {'city': 'Казань', 'region': 'Республика Татарстан'},
    'nn': {'city': 'Нижний Новгород', 'region': 'Нижегородская область'},
    'chelyabinsk': {'city': 'Челябинск', 'region': 'Челябинская область'},
    'samara': {'city': 'Самара', 'region': 'Самарская область'},
    'omsk': {'city': 'Омск', 'region': 'Омская область'},
    'rostov': {'city': 'Ростов-на-Дону', 'region': 'Ростовская область'},
    'ufa': {'city': 'Уфа', 'region': 'Республика Башкортостан'},
    'krasnoyarsk': {'city': 'Красноярск', 'region': 'Красноярский край'},
}

# 'Syntactic sugar'
for key in SUBDOMAINS.keys():
    SUBDOMAINS[key]['name'] = key

DEFAULT_SUBDOMAIN_NAME = 'msk'
DEFAULT_SUBDOMAIN = SUBDOMAINS[DEFAULT_SUBDOMAIN_NAME]

"""
https://msk.zoon.ru/ # Москва
https://spb.zoon.ru/ # Санкт-Петербург
https://nsk.zoon.ru/ # Новосибирск
https://ekb.zoon.ru/ # Екатеринбург
https://kazan.zoon.ru/ # Казань
https://nn.zoon.ru/ # Нижний Новгород
https://chelyabinsk.zoon.ru/ # Челябинск
https://samara.zoon.ru/ # Самара
https://omsk.zoon.ru/ # Омск
https://rostov.zoon.ru/ # Ростов-на-Дону
https://ufa.zoon.ru/ # Уфа
https://krasnoyarsk.zoon.ru/ # Красноярск
"""

REDIRECTED_URL_RE = re.compile(r'redirect/\?to=(.+)&hash=')

# This is the maximum amount of items per page in API calls
ITEMS_PER_PAGE = 30

# This is the maximum amount of pages per search filter (API restriction)
PAGE_LIMIT = 8

# Search filters are here
FILTERS_FILENAME = 'filters.html'

NL = '\r\n'

CSV_DELIMITER = ','

CSV_FILENAME = 'entertainment.csv'
JSON_FILENAME = 'entertainment.json'

COLUMNS = [
    'Берется из URL',
    'Регион России',
    'Город',
    'Адрес',
    'Название',
    'Описание',
    'Телефон',
    'Фото',
    'Полный URL без параметров',
    'Категория',
    'Время работы',
]

SOCIAL_NETS_IND = COLUMNS.index('Время работы')

def get_search_link(subdomain: str) -> str:
    if subdomain == DEFAULT_SUBDOMAIN_NAME:
        link = (BASE_URL_TEMPLATE.replace(TEMPLATE_SUBST, '')
                + DEFAULT_SUBDOMAIN_NAME + '/')
    else:
        link = BASE_URL_TEMPLATE.replace(TEMPLATE_SUBST, subdomain + '.')

    return link + SEARCH_URL

def get_api_link(subdomain: str) -> str:
    return get_search_link(subdomain) + API_URL

def get_search_links() -> list:
    return [get_search_link(subdomain) for subdomain in SUBDOMAINS]

def get_api_links() -> list:
    return [get_api_link(subdomain) for subdomain in SUBDOMAINS]

def get_subdomain(url: str) -> dict:
    subdomain_str = urlparse(url).hostname.split('.')[0]
    return SUBDOMAINS.get(subdomain_str, DEFAULT_SUBDOMAIN)

def clean_text(text: str) -> str:
    return re.sub(r'\s+', ' ', text.strip())

def get_item_param_data(soup: BeautifulSoup, param_caption: str) -> Tag:
    for caption in soup.find_all('dt'):
        if clean_text(caption.get_text()) == param_caption:
            return caption.find_next_sibling('dd')

    return None

def get_item_links(url: str=None, html: str=None) -> list:
    if html == None:
        response = get_response(url)
        if response == None:
            return None
        html = response.text

    soup = BeautifulSoup(html, 'html.parser')
    item_links = []
    for item_div in soup.find_all('div', class_='service-description'):
        item_links.append(item_div.find('a', class_='js-item-url')['href'])

    return item_links

def is_last_page(html: str) -> list:
    soup = BeautifulSoup(html, 'html.parser')
    if soup.find('span', text='Показать еще'):
        return False

    return True

def load_filters(filename: str=FILTERS_FILENAME) -> list:
    with open(filename, 'rt', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')
        return [checkbox['name'] for checkbox in soup.find_all('input')]

def get_ajax_html(api_url: str, item_filter: str, page: int) -> str:
    params = {
        'need[]': 'items',
        'search_query_form': 1,
        'page': page,
        # Filter example: 'm[5a7bf6f2c1098a2bef1ecea6]'
        item_filter: 1,
    }
    r = get_response(api_url, params=params, post=True)

    if not r:
        return None

    try:
        json = r.json()
    except Exception as e:
        logging.error('Failure while getting JSON: ' + str(e))
        return None

    if json.get('html') == None:
        logging.error('API request via POST was not successful.')
        return None

    return json['html']

def scrape_item(url: str) -> dict:
    """Scrapes single item with given URL.

    Returns a dict as follows:
    {
        'Берется из URL': str,
        'Регион России': str,
        'Город': str,
        'Адрес': str,
        'Название': str,
        'Описание': str,
        'Телефон': str,
        'Фото': str,
        'Полный URL без параметров': str,
        'Категория': str,
        'Время работы': str,
        + Dynamic key-value pairs for social networks
    }
    """
    item = {}

    # URL example:
    # https://zoon.ru/msk/entertainment/semejnyj_park_priklyuchenij_zamaniya_v_tts_4daily/
    item['Берется из URL'] = url.split('/')[-2]

    subdomain = get_subdomain(url)
    item['Регион России'] = subdomain['region']
    item['Город'] = subdomain['city']

    response = get_response(url)
    if response == None:
        return None

    try:
        soup = BeautifulSoup(response.text, 'html.parser')

        address_tag = soup.find('address', class_='iblock')
        item['Адрес'] = clean_text(address_tag.get_text())
        # Address extension
        address_ext = address_tag.find_next_sibling('div')
        if address_ext:
            item['Адрес'] += NL + clean_text(address_ext.get_text())
        # Metros
        metros = soup.find_all('div', class_='address-metro')
        if metros:
            item['Адрес'] += NL + 'Метро: '
            for metro in metros:
                item['Адрес'] += clean_text(metro.get_text()) + ' '
            item['Адрес'] = item['Адрес'].strip() # Remove last \n character

        item['Название'] = clean_text(soup.find('h1').get_text())

        description_lines = []
        for paragraph in get_item_param_data(soup, 'Описание').find_all('p'):
            paragraph = clean_text(paragraph.get_text())
            if paragraph:
                description_lines.append(paragraph)
        item['Описание'] = NL.join(description_lines)

        phone_list = soup.find('div', class_='service-phones-list')
        if phone_list:
            phones = [clean_text(phone['data-number'])
                      for phone in phone_list.find_all('span',
                                                       class_='js-phone')]
            item['Телефон'] = ', '.join(phones)
        else:
            item['Телефон'] = ''

        image_links = [image['data-original']
            for image in soup.find_all('a',
                                       class_='s-icons-white-dot-opacity')]
        # item['Фото'] = '; '.join(image_links[:2])
        # item['Фото'] = '; '.join(image_links)
        item['Фото'] = '; '.join(image_links[:16])

        item['Полный URL без параметров'] = url

        category_cell = get_item_param_data(soup, 'Развлечения')
        if category_cell:
            category_links = category_cell.find_all('a')
            categories = [clean_text(category_link.get_text())
                          for category_link in category_links]
            item['Категория'] = ', '.join(categories)
        else:
            item['Категория'] = ''

        open_time_cell = get_item_param_data(soup, 'Время работы')
        if open_time_cell:
            open_time_lines = []
            for tag in open_time_cell.div.children:
                if isinstance(tag, NavigableString):
                    text = str(tag)
                else:
                    text = tag.get_text()
                open_time_line = clean_text(text)
                if open_time_line:
                    open_time_lines.append(open_time_line)
            item['Время работы'] = '; '.join(open_time_lines)
        else:
            item['Время работы'] = ''

        item['Соц. сети'] = {}
        social_nets = get_item_param_data(soup, 'Страница в соцсетях')
        if social_nets:
            for social_net in social_nets.div.find_all('a'):
                social_net_name = clean_text(social_net.get_text())
                social_net_link = unquote(social_net['href'])
                social_net_link = re.findall(REDIRECTED_URL_RE,
                                             social_net_link)[0]
                item[social_net_name] = social_net_link
                # This should be removed after COLUMNS modification
                item['Соц. сети'][social_net_name] = social_net_link
    except Exception as e:
        logging.error(f'Failure while scraping item {url}: ' + str(e))
        return None

    return item

def get_all_social_nets(items: list) -> list:
    social_nets = []
    for item in items:
        for social_net in item['Соц. сети']:
            if social_net not in social_nets:
                social_nets.append(social_net)

    return social_nets

# Changes global variable COLUMNS and updates items
def social_nets_fix(social_nets: list, items: list = []):
    # Inserts social_nets list into COLUMNS list at SOCIAL_NETS_IND position
    COLUMNS[SOCIAL_NETS_IND:SOCIAL_NETS_IND] = social_nets

    # Fixing each item dictionary
    for index, item in enumerate(items):
        del items[index]['Соц. сети']
        for social_net in social_nets:
            if items[index].get(social_net) == None:
                items[index][social_net] = ''

def get_item_urls(items: list) -> list:
    return [item['Полный URL без параметров'] for item in items]

def items_sort(items: list):
    items.sort(key=lambda item: (item['Город'], item['Название']))

# items parameter may contain previous scraping result
def scrape_items(items: list=[]) -> list:
    item_urls = get_item_urls(items)
    item_filters = load_filters()
    for subdomain_api_link in get_api_links():
        logging.info(f'>>>Starting scraping for {subdomain_api_link}<<<')
        for item_filter in item_filters:
            logging.info(f'>>>Starting scraping for filter "{item_filter}"<<<')
            page = 1
            modified = False
            while page <= PAGE_LIMIT:
                logging.info(f'>>>Starting scraping for page {page}<<<')
                html = get_ajax_html(api_url=subdomain_api_link,
                                     item_filter=item_filter,
                                     page=page)

                # Possible anti-scraping protection activated
                if html == None:
                    logging.info('Access fail. '
                                 'Maybe CAPTCHA solving is needed.')
                    input('Press ENTER when CAPTCHA is solved.')
                    continue

                item_links = get_item_links(html=html)
                if item_links == None:
                    return None
                logging.info(f'Item count on page: {len(item_links)}.')
                for item_link in item_links:
                    if item_link in item_urls:
                        logging.info(f'Item {item_link} already fetched.')
                        continue

                    logging.info(f'Scraping item {item_link}')
                    new_item = scrape_item(item_link)
                    # Error while item scraping
                    if new_item == None:
                        continue

                    item_urls.append(item_link)
                    items.append(new_item)
                    modified = True

                # Definitely the last page
                if len(item_links) < ITEMS_PER_PAGE or is_last_page(html=html):
                    break

                page += 1

            if modified:
                # Saving intermediate scraping results for each search filter
                logging.info(f'Saving scraping results.')
                save_items_json(items, JSON_FILENAME)
                modified = False

    return items

# Saving prepared item data to a CSV file
def save_item(item: dict, filename: str, first_item=False) -> bool:
    try:
        with open(filename, 'w' if first_item else 'a',
                  newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=CSV_DELIMITER)
            if first_item:
                writer.writerow(COLUMNS)
            writer.writerow([item[key] for key in COLUMNS])
    except OSError:
        logging.error(f'Can\'t write to CSV file {filename}.')
        return False
    except Exception as e:
        logging.error('Scraped data saving fault. ' + str(e))
        return False

    return True

# Saves prepared items list to a CSV file
def save_items_csv(items: list, filename: str) -> bool:
    # Workaround for dynamic social columns
    social_nets_fix(get_all_social_nets(items), items)

    for index, item in enumerate(items):
        if not save_item(item, filename, first_item = (index == 0)):
            return False

    return True

# Saves item list to a JSON file
def save_items_json(items: list, filename: str) -> bool:
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(items, f, ensure_ascii=False, indent=4)
    except OSError:
        logging.error(f"Can't write to the file {filename}.")
        return False

    return True

def load_items_json(filename: str) -> list:
    try:
        with open(filename, encoding='utf-8') as f:
            items = json.load(f)
    except OSError:
        logging.warning(f"Can't load the file {filename}.")
        return []

    return items

# System handler for correct CTRL-C processing
def sigint_handler(signal_received, frame):
    logging.info('SIGINT or CTRL-C detected. Program execution halted.')
    sys.exit(0)

# For debug:
def _fix_items():
    items = load_items_json(JSON_FILENAME)
    for index in range(len(items) - 1, -1, -1):
        if items[index].get('Соц. сети') == None:
            del items[index]
    if save_items_json(items, JSON_FILENAME):
        print('Fixing complete.')

# For debug:
def _json_to_csv():
    items = load_items_json(JSON_FILENAME)
    items_sort(items)
    if save_items_csv(items, CSV_FILENAME):
        print('Saving complete.')

# Script entry point
def main():
    setup_logging()
    signal(SIGINT, sigint_handler)

    logging.info('Starting scraping process.')
    items = scrape_items(load_items_json(JSON_FILENAME))
    if items == None:
        logging.error(FATAL_ERROR_STR)
        return
    logging.info('Scraping process complete. Now saving the results.')

    items_sort(items)
    if not save_items_csv(items, CSV_FILENAME):
        logging.error(FATAL_ERROR_STR)
        return
    logging.info('Saving complete.')


if __name__ == '__main__':
    # main()
    # _fix_items()
    _json_to_csv()
