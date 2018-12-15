import os

import pytumblr
from redis import ConnectionPool, StrictRedis

redis_pool = ConnectionPool(
    host=os.environ.get('REDIS_PORT_6379_TCP_ADDR', os.environ.get('REDIS_HOST', '127.0.0.1')),
    port=int(os.environ.get('REDIS_PORT_6379_TCP_PORT', os.environ.get('REDIS_PORT', 6379))),
    db=int(os.environ.get('REDIS_DB', 0)),
    decode_responses=True
)

def create_tumblr():
    if "TUMBLR_TOKEN_SECRET" in os.environ:
        return pytumblr.TumblrRestClient(
            os.environ.get("TUMBLR_CONSUMER_KEY"),
            os.environ.get("TUMBLR_CONSUMER_SECRET"),
            os.environ.get("TUMBLR_TOKEN"),
            os.environ.get("TUMBLR_TOKEN_SECRET")
        )
    elif "TUMBLR_CONSUMER_SECRET" in os.environ:
        return pytumblr.TumblrRestClient(
            os.environ.get("TUMBLR_CONSUMER_KEY"),
            os.environ.get("TUMBLR_CONSUMER_SECRET")
        )
    else:
        return pytumblr.TumblrRestClient(
            os.environ.get("TUMBLR_CONSUMER_KEY")
        )

def create_redis():
    return StrictRedis(connection_pool=redis_pool)
