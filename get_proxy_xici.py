import requests
from bs4 import BeautifulSoup
import time

proxy_file = open('proxies.txt', 'w', encoding='utf-8')
url_prefix = 'https://www.xicidaili.com/nn/'
max_pages = 10
proxy_count = 0
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
                    (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'
}
for i in range(max_pages):
    try:
        page = i + 1
        url = url_prefix + str(page)
        print('Requesting %s...' % url)
        response = requests.get(url, headers=headers)
        print('Status: %d' % response.status_code)
        if response.status_code == 200:
            html = response.text
            soup = BeautifulSoup(html, 'lxml')
            for item in soup.select('tr[class="odd"]'):
                ip = item.select_one('td:nth-child(2)').get_text()
                port = item.select_one('td:nth-child(3)').get_text()
                protocol = item.select_one('td:nth-child(6)').get_text().lower()
                if protocol == 'http':
                    proxy = '%s://%s:%s' % (protocol, ip, port)
                    proxy_count += 1
                    print('No.%d: %s' % (proxy_count, proxy))
                    proxy_file.write(proxy + '\n')
                time.sleep(1)
        else:
            print(response.reason)
    except Exception as e:
        print('我草被发现了，赶紧跑路= =错误信息：%s' % e)
        break
proxy_file.close()
