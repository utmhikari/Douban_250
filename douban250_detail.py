from bs4 import BeautifulSoup
import asyncio
import aiohttp
import datetime
import random
import sys
import json

# function map for achieving specific info in html body
content_func_map = {
    'title': lambda sp: sp.select_one('#content > h1 > span:nth-child(1)').get_text(),
    'year': lambda sp: sp.select_one('#content > h1 > span.year').get_text().replace('(', '').replace(')', ''),
    'time': lambda sp: sp.select_one('#info > span[property="v:runtime"]').get_text(),
    'director': lambda sp: sp.select_one('#info > span:nth-child(1) > span.attrs > a').get_text(),
    'genre': lambda sp: list(map(lambda item: item.get_text(), sp.select('#info > span[property="v:genre"]'))),
    'score': lambda sp: sp.select_one('#interest_sectl > div.rating_wrap.clearbox > \
                                        div.rating_self.clearfix > strong').get_text()
}
# html parser, maybe 'html', 'lxml', 'html5lib'
html_parser = 'lxml'
# http header
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
                    (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'
}
# http proxies
proxies = set()
proxies_used = set()
proxy_delay_time = 3
proxy_connection_timeout = 3
# movie urls
movie_urls = []
# task count
task_count = 0
# results
results = []
# asyncio lock
lock = asyncio.Lock()


def get_proxies():
    """
    get proxies (HTTP)
    :return:
    """
    contents = open('proxies.txt', encoding='utf-8').read().splitlines()
    for i in range(len(contents)):
        if not contents[i].startswith('http'):
            contents[i] = 'http://' + contents[i]
    proxies.update(contents)


def get_movie_urls():
    """
    get movie urls
    :return: movie urls
    """
    with open('movie_urls.txt', 'r', encoding='utf-8') as f:
        movie_urls.extend(f.read().splitlines())
        f.close()


async def remove_proxy(proxy_set, proxy):
    """
    remove a proxy from proxy set
    """
    await lock.acquire()
    try:
        if proxy in proxy_set:
            proxy_set.remove(proxy)
    finally:
        lock.release()


def log(movie_num, msg):
    """
    log message
    :param movie_num: movie number
    :param msg: message
    :return:
    """
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print('[%s --- 电影No.%d] %s' % (current_time, movie_num, msg))


async def get_random_proxy(movie_num):
    """
    get a random proxy
    if no proxy, return empty string
    """
    no_proxy = False
    random_proxy = ''
    while True:
        delay = False
        await lock.acquire()
        try:
            proxy = random.sample(proxies, 1)[0]
            if proxy in proxies_used:
                # limit the speed of one proxy, delay 1s in schedule
                # log(movie_num, '代理%s还在使用中，延迟%d秒再找= =' % (proxy, proxy_delay_time))
                delay = True
            else:
                proxies_used.add(proxy)
                random_proxy = proxy
        except Exception as e:
            if len(proxies) == 0:
                no_proxy = True
            else:
                log(movie_num, '获取代理出错，延迟%d秒再找= =错误信息：%s' % (proxy_delay_time, e))
                delay = True
        finally:
            lock.release()
            if no_proxy:
                return ''
            if delay:
                await asyncio.sleep(proxy_delay_time)
            else:
                return random_proxy


async def parse_movie_url(session, url, movie_num):
    """
    fetch html body from url and parse it to get info of movie
    :param session: client session
    :param url: douban movie url
    :param movie_num: the number of movie
    :return: nothing~
    """
    while True:
        proxy = await get_random_proxy(movie_num)
        if not proxy:
            log(movie_num, 'TMD没代理了，凉凉= =')
            return
        # result contains info of a movie
        result = {'number': movie_num, 'url': url, 'proxy': proxy}
        # log(movie_num, '代理%s正在访问%s...' % (proxy, url))
        success = False
        try:
            response = await session.get(url, proxy=proxy, headers=headers, timeout=proxy_connection_timeout)
            status_code = response.status
            if status_code == 200:
                # if no error on response, parse html
                html_body = await response.text()
                soup = BeautifulSoup(html_body, html_parser)
                crawl_success = True
                for k in content_func_map.keys():
                    try:
                        # crawl content on specific rules
                        content = content_func_map[k](soup)
                        result[k] = content
                    except Exception as e:
                        # if cannot crawl the content, maybe the correct html body is inavailable
                        log(movie_num, '代理%s爬取%s信息失败！果断放弃掉！错误信息：%s\n' % (proxy, k, e))
                        await remove_proxy(proxies, proxy)
                        crawl_success = False
                        break
                if not crawl_success:
                    continue
            else:
                log(movie_num, '代理%s获取数据失败！果断放弃掉！状态码: %d！' % (proxy, status_code))
                await remove_proxy(proxies, proxy)
            # append result
            global results
            results.append(result)
            log(movie_num, '爬到信息：%s' % str(result))
            success = True
        except Exception as e:
            # proxy is unavailable
            log(movie_num, '代理%s连接出错，果断放弃掉！！！错误信息：%s！' % (proxy, e))
            await remove_proxy(proxies, proxy)
        finally:
            await remove_proxy(proxies_used, proxy)
            if success:
                # end task only if success is true
                # actually need a lock here lol~
                global task_count
                task_count = task_count + 1
                log(movie_num, '当前爬到信息的电影数: %d' % task_count)
                log(movie_num, '当前幸存的代理数：%d' % len(proxies))
                break


async def main():
    """
    main task
    """
    start_time = datetime.datetime.now()
    # get movie urls
    get_movie_urls()
    # get proxies
    get_proxies()
    print('代理总数: %d\n' % len(proxies))
    if len(proxies) == 0:
        print('代理列表没代理，凉凉= =')
        sys.exit(0)
    # create client session connected to the internet
    async with aiohttp.ClientSession() as session:
        # generate tasks for spider
        tasks = list()
        num_urls = len(movie_urls)
        for i in range(num_urls):
            tasks.append(parse_movie_url(session, movie_urls[i], i + 1))
        # execute tasks
        await asyncio.gather(*tasks)
        # write result to file
        with open('movie_info_async.txt', 'w', encoding='utf-8') as movie_file:
            global results
            results = sorted(results, key=lambda k: k['number'])
            movie_file.write(json.dumps(results, indent=2, ensure_ascii=False))
            movie_file.close()
        # calculate task time
        time_period = datetime.datetime.now() - start_time
        print('\n幸存的代理（%d个）：%s' % (len(proxies), str(list(proxies))))
        print('获取结果数：%d个！' % len(results))
        print('完成时间：%d秒!' % time_period.seconds)


if __name__ == '__main__':
    asyncio.run(main())
