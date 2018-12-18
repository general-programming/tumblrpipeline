import random
import time
import json
import threading
import os
import traceback

from sqlalchemy import or_, Integer, func
from redis import StrictRedis
from apipipeline import sentry_sdk
from apipipeline.connections import redis_pool, create_tumblr
from apipipeline.model import Blog, sm

# Connectors
redis = StrictRedis(connection_pool=redis_pool)
tumblr = create_tumblr()

# Redis lua
GET_REMAINING_SCRIPT = """
local url_total = redis.call('SCARD', 'tumblr:urls')
local done_total = redis.call('SCARD', 'tumblr:done')
return url_total - done_total"""
get_remaining = redis.register_script(GET_REMAINING_SCRIPT)

# Actual grabber
backoff = 2
running = True
do_worker_queue = False

urls = []
workers = []

def process_url(sql, url):
    global backoff

    # Skip over blogs we've already passed through.
    # Adding directly from SQL means we are reprocessing.
    if not do_worker_queue and redis.sismember("tumblr:done", url):
        return

    # Query
    info = tumblr.blog_info(url)

    # Ignore 404s
    if "meta" in info:
        if info["meta"]["status"] == 404:
            print(f"{url} - 404")
            redis.sadd("tumblr:done", url)
            redis.sadd("tumblr:404", url)
            return False
        elif info["meta"]["status"] == 429:
            urls.append(url)
            print(f"Got 429. Backing off for {backoff} secs.")
            time.sleep(backoff)
            backoff = min(120, backoff ** random.uniform(1, 2))
            return

    # wot how
    if "blog" not in info:
        print(info)
        print()
        redis.sadd("tumblr:done", url)
        redis.sadd("tumblr:badinfo", url)
        return

    # Reset backoff if we make it this far.
    if backoff != 2:
        backoff = 2

    # Make a new blog and log.
    redis.sadd("tumblr:queue:blogs", json.dumps(info))
    print(f"{url} - {info['blog']['posts']} posts; {get_remaining() - 1} remaining.")

    return info

def worker():
    sql = sm()

    while running and len(urls) > 0:
        url = urls.pop(random.randrange(len(urls)))

        try:
            info = process_url(sql, url)
            if info and do_worker_queue:
                print("added")
                redis.sadd("tumblr:queue:manualqueue", info["blog"]["name"])
        except:
            if sentry_sdk:
                sentry_sdk.capture_exception()
            traceback.print_exc()

if __name__ == "__main__":
    main_sql = sm()

    # urls must be set to a list with strings blah.tumblr.com
    # urls = list(redis.sdiff("tumblr:urls", "tumblr:done"))

    do_worker_queue = True

    # load file
    # with open("blogs", "r") as f:
    #     content = f.read().split("\n")
    #     for url in content:
    #         # url = url.split("tumblr-blog:", 1)[1]
    #         urls.append(url.strip() + ".tumblr.com")

    # load db
    i = 0
    blogs = main_sql.query(Blog).filter(or_(
        Blog.updated != Blog.last_crawl_update,
        Blog.last_crawl_update == None
    )).filter(Blog.data['posts'].cast(Integer) < 10000).order_by(func.random()).limit(2048).all()

    for blog in blogs:
        print(i, blog.name, blog.data['posts'])
        urls.append(blog.name.strip() + ".tumblr.com")
        i += 1

    for x in range(0, int(os.environ.get("WORKERS", 4))):
        t = threading.Thread(target=worker)
        t.start()
        workers.append(t)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        running = False
        print("Stopping!")

    for t in workers:
        t.join()
