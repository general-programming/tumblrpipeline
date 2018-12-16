import os
import sys
import time
import datetime
import threading
import collections
import traceback
import random
import json

from apipipeline import sentry_sdk
from apipipeline.connections import create_tumblr, create_redis


class BlogManager(object):
    def __init__(self):
        self.tumblr = create_tumblr()
        self.redis = create_redis()
        self.bad = collections.defaultdict(lambda: 0)

        self.last_request = time.time()
        self.running = True

    def log(self, *args):
        print(f"[{threading.current_thread().name}]", *args)

    def add(self, data, oldest=None):
        # Set timestamps.
        posted = float(data.get("timestamp", 0.0))
        if not oldest:
            oldest = 0.0

        # Don't queue up the post if the post is too old.
        if oldest > posted:
            return False

        # Queue post and continue.
        self.redis.sadd("tumblr:queue:posts", json.dumps(data))

        return True

    def get_posts(self, name, offset):
        # Limit us to 5 req/s
        last_delta = (time.time() - self.last_request)
        if last_delta < 0.25:
            time.sleep(0.25 - last_delta)

        # Update the time and return the posts.
        self.last_request = time.time()

        return self.tumblr.posts(name, offset=offset)

    def process(self, name, offset, last_crawl):
        added_posts = 0

        if self.bad[name] >= 5:
            if self.bad[name] != 999:
                self.log(f"All posts crawled for {name}. (Probarly)")
                self.bad[name] = 999
            return

        # Get posts of the offset.
        posts_response = self.get_posts(name, offset)

        # Handle errors
        if (
            posts_response.get("meta", {}).get("status", None) in (502, 503)
            or "posts" not in posts_response
        ):
            self.log(posts_response)
            time.sleep(10)
            self.process(name, offset, last_crawl)
            return

        # Add the posts one by one.
        posts = posts_response["posts"]
        for post in posts:
            post_ok = self.add(post, last_crawl)
            if post_ok:
                added_posts += 1
            else:
                self.bad[name] += 1
                continue

        # Print every time a fetch is completed and pushed.
        self.log(f"{len(posts)} @ '{name}'.")

        # This is not secure but have some honor!
        self.redis.hincrby("tumblr:work_stats", os.environ.get("WORKER_NAME"), len(posts))

    def work(self):
        while self.running:
            if self.redis.scard("tumblr:queue:import") == 0:
                time.sleep(1)
                continue

            started_prefix = "%s;%s"
            started_time = int(time.time())

            raw_item = self.redis.spop("tumblr:queue:import")
            self.redis.sadd("tumblr:queue:import:working", started_prefix % (started_time, raw_item))
            
            try:
                item = json.loads(raw_item)
            except json.decoder.JSONDecodeError:
                continue

            try:
                self.process(item["name"], item["offset"], float(item["last_crawl"]))
                self.redis.srem("tumblr:queue:import:working", started_prefix % (started_time, raw_item))
            except:
                if sentry_sdk:
                    sentry_sdk.capture_exception()
                traceback.print_exc()

if __name__ == "__main__":
    workers = []
    blog_manager = BlogManager()

    # Thread startup
    for x in range(0, int(os.environ.get("WORKERS", 2))):
        t = threading.Thread(target=blog_manager.work)
        t.start()
        workers.append(t)

    try:
        while blog_manager.running:
            # If the any thread is dead, stop running.
            for t in workers:
                if not t.is_alive():
                    blog_manager.running = False

            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping!")
        blog_manager.running = False

    for t in workers:
        t.join()
