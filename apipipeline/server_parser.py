import time
import json

from apipipeline.connections import create_redis
from apipipeline.model import Blog, Post, sm

redis = create_redis()
running = True

def add_bulk(db, model_type, key):
    start = time.time()

    if model_type == "blogs":
        model = Blog
    elif model_type == "posts":
        model = Post

    raw_items = redis.smembers(key)
    i = 0

    while True:
        raw_item = redis.spop(key)
        if not raw_item:
            break

        try:
            item = json.loads(raw_item)
        except (TypeError, json.decoder.JSONDecodeError):
            continue

        new_item = model.create_from_metadata(db, item)
        i += 1
        if i % 100 == 0:
            db.commit()
            i = 0

    db.commit()

    # Print time.
    end = time.time()
    total_time = float(end - start)
    print(f"Took {total_time} seconds to add all {model_type}.", flush=True)

def worker():
    db = sm()

    while True:
        post_count = redis.scard("tumblr:queue:posts")
        blog_count = redis.scard("tumblr:queue:blogs")
    
        has_items = (post_count + blog_count) > 0
        if not has_items:
            time.sleep(1)
            continue
    
        print(f"{post_count} posts, {blog_count} blogs in queue.")

        # Parse blogs
        if blog_count > 0:
            add_bulk(db, "blogs", "tumblr:queue:blogs")

        # Parse posts
        if post_count > 0:
            add_bulk(db, "posts", "tumblr:queue:posts")

def main():
    try:
        worker()
    except KeyboardInterrupt:
        running = False

main()