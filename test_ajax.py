import requests
from bs4 import BeautifulSoup

HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 6.1; rv:88.0)'
                   ' Gecko/20100101 Firefox/88.0'),
    'Accept': '*/*',
}

'https://zoon.ru/msk/entertainment/?search_query_form=1&m[5a7bf6f2c1098a2bef1ecea6]=1'
url = 'https://zoon.ru/msk/entertainment/?action=listJson&type=service'
payload = {
    'need[]': 'items',
    'search_query_form': 1,
    'page': 8,
    'm[5a7bf6f2c1098a2bef1ecea6]': 1,
}

r = requests.post(url, headers=HEADERS, data=payload)
# print(r.json()['html'])

soup = BeautifulSoup(r.json()['html'], 'html.parser')
item_links = []
for item_div in soup.find_all('div', class_='service-description'):
    item_links.append(item_div.find('a', class_='js-item-url')['href'])
for item_link in item_links:
    print(item_link)

if soup.find('span', text='Показать еще'):
    print('MOAR!!!')
