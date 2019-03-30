import requests
from bs4 import BeautifulSoup
import time


def get_start_url(page):
    # page = 0 <=> real page = 1
    start_num = 25 * page
    return 'https://movie.douban.com/top250?start=%d&filter=' % start_num


if __name__ == '__main__':
    cnt = 0
    movie_url_file = open('movie_urls.txt', 'w', encoding='utf-8')
    html_parser = 'lxml'
    for i in range(10):
        url = get_start_url(i)
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, html_parser)
            for movies in soup.find_all('div', class_='hd'):
                cnt += 1
                movie_url = movies.find('a')['href']
                movie_name = movies.find('span', class_='title').get_text()
                print('%d --- %s: %s' % (cnt, movie_name, movie_url))
                movie_url_file.write(movie_url + '\n')
        else:
            print('Cannot get content of page %d with status code %d!' % (i + 1, response.status_code))
            time.sleep(3)
    movie_url_file.close()
