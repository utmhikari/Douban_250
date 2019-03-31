from bs4 import BeautifulSoup
import asyncio
import aiohttp
import datetime
import random
import sys
import json
from collections import deque

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
proxies = deque()
proxies_used = set()
proxy_connection_timeout = 3
# proxy delay time
proxy_delay_center = 4
proxy_delay_radius = 1
proxy_search_period = 0.001
# movie urls
movie_urls = []
# task count
task_count = 0
# results
results = []
# asyncio lock & cond
cond = None


def get_proxies():
    """
    get proxies (HTTP)
    :return:
    """
    contents = open('proxies.txt', encoding='utf-8').read().splitlines()
    for i in range(len(contents)):
        if not contents[i].startswith('http'):
            contents[i] = 'http://' + contents[i]
    proxies.extend(set(contents))


def get_movie_urls():
    """
    get movie urls
    :return: movie urls
    """
    with open('movie_urls.txt', 'r', encoding='utf-8') as f:
        movie_urls.extend(f.read().splitlines())
        f.close()


def get_proxy_delay_time():
    """
    generate random delay time
    """
    return (proxy_delay_center - proxy_delay_radius) + (2 * proxy_delay_radius * random.random())


async def allocate_proxy(max_tasks):
    """
    allocate an available proxy
    if no proxy available, notify all
    if all tasks are finished, return
    :param max_tasks:
    :return:
    """
    print('开始分配代理...')
    while True:
        await cond.acquire()
        will_break = False
        will_delay = False
        try:
            if task_count == max_tasks:
                print('已经完成所有%d个任务！不再分配代理啦~' % max_tasks)
                will_break = True
            len_proxies = len(proxies)
            if len_proxies == 0:
                if len(proxies_used) == 0:
                    print('代理全部挂了，不再分配代理啦= = = = =')
                    cond.notify_all()
                    will_break = True
                else:
                    will_delay = True
            else:
                # notify len(proxies) each time
                cond.notify(len_proxies)
        finally:
            cond.release()
            if will_break:
                break
            elif will_delay:
                delay_time = get_proxy_delay_time()
                print('代理暂时还都在用，延迟%f秒再分配代理！' % delay_time)
                await asyncio.sleep(delay_time)
            else:
                # let other tasks run
                await asyncio.sleep(proxy_search_period)


async def recycle_proxy(proxy):
    """
    append a successfully used proxy to proxies
    :return:
    """
    await cond.acquire()
    try:
        proxies_used.discard(proxy)
        proxies.append(proxy)
    finally:
        cond.release()


async def get_proxy():
    """
    get an available proxy, if no proxy available, return empty string
    :return: proxy
    """
    await cond.acquire()
    proxy = ''
    try:
        await cond.wait()
        if len(proxies) > 0:
            proxy = proxies.popleft()
            proxies_used.add(proxy)
    finally:
        cond.release()
    return proxy


async def remove_proxy(proxy):
    """
    remove a used proxy
    """
    await cond.acquire()
    try:
        proxies_used.discard(proxy)
        print('当前代理池代理数：%d --- 当前在用代理数：%d' % (len(proxies), len(proxies_used)))
    finally:
        cond.release()


def log(movie_num, msg):
    """
    log message
    :param movie_num: movie number
    :param msg: message
    """
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print('[%s --- 电影No.%d] %s' % (current_time, movie_num, msg))


async def crawl_movie_url(session, url, movie_num):
    """
    fetch html body from url and parse it to get info of movie
    :param session: client session
    :param url: douban movie url
    :param movie_num: the number of movie
    :return: nothing~
    """
    while True:
        proxy = await get_proxy()
        if not proxy:
            log(movie_num, 'TMD代理全部挂了，凉凉= =')
            return
        # result contains info of a movie
        result = {'number': movie_num, 'url': url, 'proxy': proxy}
        log(movie_num, '代理%s正在访问%s...' % (proxy, url))
        success = True
        try:
            response = await session.get(url, proxy=proxy, headers=headers, timeout=proxy_connection_timeout)
            status_code = response.status
            if status_code == 200:
                # if no error on response, parse html
                html = await response.text()
                soup = BeautifulSoup(html, html_parser)
                for k in content_func_map.keys():
                    try:
                        # crawl content on specific rules
                        content = content_func_map[k](soup)
                        result[k] = content
                    except Exception as e:
                        # if cannot crawl the content, maybe the correct html body is inavailable
                        log(movie_num, '代理%s爬取%s信息失败！果断放弃掉！错误信息：%s\n' % (proxy, k, e))
                        success = False
                        break
            else:
                log(movie_num, '代理%s获取数据失败！果断放弃掉！状态码: %d！' % (proxy, status_code))
                success = False
        except Exception as e:
            # proxy is unavailable
            log(movie_num, '代理%s连接出错，果断放弃掉！！！错误信息：%s！' % (proxy, e))
            success = False
        finally:
            if success:
                # append result, add task count, recycle proxy
                global results
                global task_count
                results.append(result)
                task_count = task_count + 1
                log(movie_num, '当前爬到信息的电影数: %d，爬到信息：%s' % (task_count, str(result)))
                await recycle_proxy(proxy)
                break
            else:
                # remove proxy
                await remove_proxy(proxy)


async def main():
    """
    main task
    """
    # set start time
    start_time = datetime.datetime.now()
    # get movie urls
    get_movie_urls()
    # get proxies
    get_proxies()
    print('代理总数: %d\n' % len(proxies))
    if len(proxies) == 0:
        print('代理列表没代理啦，凉凉= =')
        sys.exit(0)
    # create client session connected to the internet
    async with aiohttp.ClientSession() as session:
        num_urls = len(movie_urls)
        # set condition variable
        global cond
        cond = asyncio.Condition()
        # generate tasks for spider
        tasks = list()
        for i in range(num_urls):
            tasks.append(crawl_movie_url(session, movie_urls[i], i + 1))
        # put proxy allocation task to the last one
        tasks.append(allocate_proxy(len(tasks)))
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
        print('获取结果数：%d个！' % len(results))
        print('完成时间：%d秒!' % time_period.seconds)


if __name__ == '__main__':
    asyncio.run(main())
