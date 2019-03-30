# 豆瓣二百五

asyncio并发加上代理池爬取豆瓣二百五的例子

## 文件说明

- douban250_home.py：爬取豆瓣250的链接
- douban250_detail.py：爬取豆瓣250每个链接点进去里边的内容
- get_proxy_xici.py：一个获取西刺代理的脚本（其实自己在其它地方搜也可）

## 注意事项

- Python3.7下aiohttp貌似有bug报SSL Error，不过没关系，代理池人海战术解决即可