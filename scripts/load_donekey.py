from apipipeline.connections import create_redis
from apipipeline.model import Blog, sm

db = sm()
redis = create_redis()

i = 0

for blog in db.query(Blog).yield_per(1024):
    redis.sadd("tumblr:done", blog.name.strip() + ".tumblr.com")
    i += 1
    if i % 500 == 0:
        print(i, flush=True)
