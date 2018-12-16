import threading
import time
import json

from sqlalchemy.sql.expression import func
from sqlalchemy import or_

from apipipeline.connections import create_redis, create_tumblr
from apipipeline.model import Blog, Post, sm

redis = create_redis()
running = True

def worker_feeder():
    global running
    db = sm()
    tumblr = create_tumblr()
    redis = create_redis()

    while running:
        import_count = redis.scard("tumblr:queue:import")
        working_count = redis.scard("tumblr:queue:import:working")
        print(f"{import_count} offsets queued. {working_count} being worked on.", flush=True)

        if import_count > 420:  # Archiving secured.
            time.sleep(1)
            continue

        random_blog = db.query(Blog).filter(or_(
            Blog.updated != Blog.last_crawl_update,
            Blog.last_crawl_update == None
        )).order_by(func.random()).limit(1).scalar()

        info = tumblr.blog_info(random_blog.name)

        if info.get("meta", {}).get("status", None) in (503, 429):
            time.sleep(5)
            print(info)
            continue

        # Shoot the job off.
        print("Adding %s offsets for %s" % (
            info['blog']['posts'] // 20,
            random_blog.name
        ))
        for offset in range(0, info['blog']['posts'] + 20, 20):
            redis.sadd("tumblr:queue:import", json.dumps({
                "name": random_blog.name,
                "offset": offset,
                "last_crawl": str(random_blog.last_crawl_update.timestamp()) if random_blog.last_crawl_update else "0"
            }))

        random_blog.last_crawl_update = random_blog.updated
        db.commit()

def worker_repusher():
    global running
    redis = create_redis()

    while running:
        for raw_work in redis.smembers("tumblr:queue:import:working"):
            started, work = raw_work.split(";", 1)
            started_delta = (time.time() - float(started))

            if started_delta > 60:
                print("Requing work that has been idle for %s seconds." % (started_delta))
                redis.srem("tumblr:queue:import:working", raw_work)
                redis.sadd("tumblr:queue:import", work)

        time.sleep(5)

if __name__ == "__main__":
    threads = [
        threading.Thread(target=worker_feeder),
        threading.Thread(target=worker_repusher)
    ]

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
