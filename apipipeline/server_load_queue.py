import os
import math
import random
import threading
import time
import json

from sqlalchemy.sql.expression import func
from sqlalchemy import or_

from apipipeline.connections import create_redis, create_tumblr
from apipipeline.model import Blog, Post, sm

redis = create_redis()
running = True

# Worker feeder

def load_blog(db, redis, tumblr, blog, use_db=False):
    if not use_db:
        info = tumblr.blog_info(blog.name)
    else:
        info = {
            "meta": {"status": 200},
            "blog": blog.data
        }

        # In case bad data gets saved.
        if "posts" not in blog.data or not blog.data["posts"]:
            info = tumblr.blog_info(blog.name)

    info_status_code = info.get("meta", {}).get("status", None) 

    # Handle errors
    if info_status_code == 404:
        print(info)
        blog.last_crawl_update = blog.updated
        db.commit()
        return

    if info_status_code in (503, 504, 429):
        time.sleep(5)
        print(info)
        return

    if "blog" not in info or "posts" not in info["blog"]:
        print(info)
        return

    # Shoot the job off.
    print("Adding %s offsets for %s" % (
        math.ceil(info['blog']['posts'] / 20),
        blog.name
    ), flush=True)

    for offset in range(0, info['blog']['posts'] + 20, 20):
        redis.sadd("tumblr:queue:import", json.dumps({
            "name": blog.name,
            "offset": offset,
            "last_crawl": str(blog.last_crawl_update.timestamp()) if blog.last_crawl_update else "0"
        }))

    blog.last_crawl_update = blog.updated
    db.commit()

def get_blogs(db, manual_count):
    use_db = False
    blogs = []

    if manual_count == 0:
        blogs = db.query(Blog).filter(or_(
            Blog.updated != Blog.last_crawl_update,
            Blog.last_crawl_update == None
        )).order_by(func.random()).limit(random.randint(1, 25)).all()
        for blog in blogs:
            yield blog, use_db
    else:
        use_db = True

        while True:
            blog_name = redis.spop("tumblr:queue:manualqueue")
            if not blog_name:
                break

            new_blog = db.query(Blog).filter(Blog.name == blog_name).order_by(Blog.updated.desc()).all()
            if new_blog:
                yield new_blog[0], use_db

    # Force sleep if there are no blogs.
    if not blogs:
        print("No blogs left to add.")
        time.sleep(15)

def worker_feeder():
    global running
    db = sm()
    tumblr = create_tumblr()
    redis = create_redis()

    while running:
        import_count = redis.scard("tumblr:queue:import")
        working_count = redis.scard("tumblr:queue:import:working")
        manual_count = redis.scard("tumblr:queue:manualqueue")

        print(f"{import_count} offsets queued. {working_count} being worked on.", flush=True)

        if import_count > 420 and manual_count <= 0:  # Archiving secured.
            time.sleep(1)
            continue

        for blog, use_db in get_blogs(db, manual_count):
            load_blog(db, redis, tumblr, blog, use_db)

# Worker repusher

def worker_repusher():
    global running
    redis = create_redis()

    while running:
        for raw_work in redis.smembers("tumblr:queue:import:working"):
            started, work = raw_work.split(";", 1)
            started_delta = (time.time() - float(started))

            if started_delta > 180:
                print("Requing work that has been idle for %s seconds." % (started_delta))
                redis.srem("tumblr:queue:import:working", raw_work)
                redis.sadd("tumblr:queue:import", work)

        time.sleep(5)

if __name__ == "__main__":
    threads = [
        threading.Thread(target=worker_repusher)
    ]

    # Start multiple pushers
    for x in range(0, int(os.environ.get("WORKERS", 4))):
        threads.append(threading.Thread(target=worker_feeder))

    # Thread startup
    for t in threads:
        t.start()

    # Thread holding
    try:
        while running:
            # If the any thread is dead, stop running.
            for t in threads:
                if not t.is_alive():
                    running = False
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping!")
        running = False

    # Wait for all threads to stop before shutting down.
    for t in threads:
        t.join()
