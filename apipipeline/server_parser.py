import os
import threading
import time
import json

from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import JSONB, ARRAY, insert

from apipipeline.connections import create_redis
from apipipeline.model import Blog, Post, sm

running = True

def get_item(db, model, raw_item):
    try:
        item = json.loads(raw_item)
    except (TypeError, json.decoder.JSONDecodeError):
        return

    return model.create_from_metadata(db, item, insert_only=True)

def add_bulk(db, redis, model_type, key):
    if model_type == "blogs":
        model = Blog
        uniques = ["tumblr_id"]
    elif model_type == "posts":
        model = Post
        uniques = ["tumblr_id", "author_id"]

    before_commit = time.time()
    bulks = []

    while True:
        raw_items = redis.spop(key, count=500)
        if not raw_items:
            break

        for raw_item in raw_items:
            new_bulk = get_item(db, model, raw_item)
            if new_bulk:
                bulks.append(new_bulk)

            if bulks and len(bulks) % 500 == 0:
                fast_commit = True

                try:
                    db.bulk_insert_mappings(
                        model,
                        bulks
                    )
                    db.commit()
                except IntegrityError:
                    fast_commit = False
                    db.rollback()
                    for data in bulks:
                        db.execute(insert(model).values(
                            **data
                        ).on_conflict_do_nothing(index_elements=uniques))
                    db.commit()

                # stats
                delta_commit = time.time() - before_commit
                print(f"Took {delta_commit} seconds to generate and commit {len(bulks)} items. {fast_commit}", flush=True)
                before_commit = time.time()
                bulks.clear()

    fast_commit = True
    try:
        db.bulk_insert_mappings(
            model,
            bulks
        )
        db.commit()
    except IntegrityError:
        fast_commit = False
        db.rollback()
        for data in bulks:
            db.execute(insert(model).values(
                **data
            ).on_conflict_do_nothing(index_elements=uniques))
    db.commit()

    # Final stats print
    delta_commit = time.time() - before_commit
    print(f"Took {delta_commit} seconds to generate and commit {len(bulks)} items. {fast_commit}", flush=True)
    before_commit = time.time()

def worker():
    global running
    db = sm()
    redis = create_redis()

    while running:
        post_count = redis.scard("tumblr:queue:posts")
        blog_count = redis.scard("tumblr:queue:blogs")
    
        has_items = (post_count + blog_count) > 0
        if not has_items:
            time.sleep(1)
            continue
    
        print(f"{post_count} posts, {blog_count} blogs in queue.")

        # Parse blogs
        if blog_count > 0:
            add_bulk(db, redis, "blogs", "tumblr:queue:blogs")

        # Parse posts
        if post_count > 0:
            add_bulk(db, redis, "posts", "tumblr:queue:posts")

if __name__ == "__main__":
    threads = []

    # Start multiple parsers
    for x in range(0, int(os.environ.get("WORKERS", 3))):
        threads.append(threading.Thread(target=worker))

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
