from apipipeline.connections import create_redis
from apipipeline.model import Post, engine

redis = create_redis()
redis_pipe = redis.pipeline()

author_uids = {}
i = 0

def get_author_uid(db, author_id):
    if author_id not in author_uids:
        query = db_conn.execute("SELECT tumblr_uid from blogs WHERE blogs.id = %d" % (author_id))
        author_uid = query.fetchall()[0][0]
        author_uids[author_id] = author_uid

    return author_uids[author_id]

def get_total_posts(db):
    query = db_conn.execute("SELECT count(*) from posts_warehouse")
    return query.fetchall()[0][0]

with engine.connect() as db_conn:
    total_posts = get_total_posts(db_conn)
    warehouse_keys = []

    with engine.connect() as db_other_conn:
        query = db_conn.execution_options(stream_results=True).execute("SELECT author_id, data->'id' from posts_warehouse")
        for author_id, post_id in query:
            blog_uuid = get_author_uid(db_other_conn, author_id)
            warehouse_keys.append("%s:%s" % (post_id, blog_uuid))
    
            i += 1
            if i % 1500 == 0:
                redis.sadd("tumblr:warehoused", *warehouse_keys)
                print(f"{i} done. {total_posts - i} remaining. {len(author_uids)} in cache.", flush=True)
                warehouse_keys.clear()

print(i, flush=True)
