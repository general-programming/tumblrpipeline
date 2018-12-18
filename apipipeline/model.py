# This Python file uses the following encoding: utf-8
import os
import platform
import datetime

from urllib.parse import urlparse
from contextlib import contextmanager

# Compat before doing anything with SQL if on pypy.
if platform.python_implementation() == "PyPy":
    from psycopg2cffi import compat
    compat.register()

from apipipeline.connections import create_redis

from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, BigInteger, DateTime, Unicode, create_engine, inspect
from sqlalchemy.dialects.postgresql import JSONB, ARRAY, insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker, relationship
from sqlalchemy.schema import Index

from apipipeline.utils import clean_data

debug = os.environ.get('DEBUG', False)

if "POSTGRES_URL" not in os.environ or not os.environ["POSTGRES_URL"]:
    print("POSTGRES_URL is missing. This is bad if you're running server processes.")

engine = create_engine(os.environ.get("POSTGRES_URL", "postgres://placeholder/placeholder"), convert_unicode=True, pool_recycle=3600)

if debug:
    engine.echo = True

sm = sessionmaker(autocommit=False,
                  autoflush=False,
                  bind=engine)

base_session = scoped_session(sm)

Base = declarative_base()
Base.query = base_session.query_property()

@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = sm()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

db_redis = create_redis()
BLOG_ID_CACHE = {}


class Post(Base):
    __tablename__ = 'posts'
    id = Column(Integer, primary_key=True)
    author_id = Column(ForeignKey("blogs.id"))
    tumblr_id = Column(BigInteger)

    posted = Column(DateTime)

    author = relationship("Blog")
    data = Column(JSONB, nullable=False)

    @classmethod
    def create_from_metadata(cls, db, info, insert_only=False):
        # Try to work with an existing blog object first.
        if insert_only:
            post_object = None
        else:
            post_object = db.query(Post).filter(Post.tumblr_id == info["id"]).scalar()

        # Setup the new data to update.
        post_data = dict(
            tumblr_id=info.get("id"),
            posted=datetime.datetime.fromtimestamp(info.get("timestamp", 0)),
            data=info
        )

        # Set the author id if it is not set
        if (not post_object or not post_object.author_id) and "blog" in info:
            blog_name = info.get("blog_name", "")
            if blog_name in BLOG_ID_CACHE:
                author_id = BLOG_ID_CACHE[blog_name]
            else:
                author_id = db_redis.hget("tumblr:blogids", blog_name)
                if author_id:
                    BLOG_ID_CACHE[blog_name] = author_id

            # Create an author ID if it is not in cache.
            if not author_id:
                author = db.query(Blog).filter(Blog.name == blog_name).order_by(Blog.updated.desc()).all()

                try:
                    author_id = author[0].id
                except IndexError:
                    if "uuid" in info["blog"]:
                        new_author = Blog.create_from_metadata(db, info.get("blog"))
                        db.flush()
                        author_id = new_author.id

                if author_id:
                    db_redis.hset("tumblr:blogids", blog_name, author_id)
                    BLOG_ID_CACHE[blog_name] = author_id

            if author_id:
                post_data["author_id"] = author_id

        # Clean the data of null bytes.
        clean_data(post_data)

        # Create / Update the post object.
        if not post_object:
            # Insert and query the blog object if it does not exist.
            if insert_only:
                return post_data
            else:
                post_object = db.query(Post).filter(
                    Post.tumblr_id == post_data["tumblr_id"]
                ).scalar()
        else:
            for key, value in post_data.items():
                setattr(post_object, key, value)

        return post_object

class Blog(Base):
    __tablename__ = "blogs"
    id = Column(Integer, primary_key=True)
    tumblr_uid = Column(String, nullable=False)
    name = Column(String(200))

    updated = Column(DateTime)
    last_crawl_update = Column(DateTime)

    data = Column(JSONB, nullable=False)
    extra_meta = Column(JSONB)

    @classmethod
    def create_from_metadata(cls, db, info, insert_only=False):
        if "blog" in info:
            blog_info = info["blog"]
        else:
            blog_info = info

        # Return nothing if there is no name in the blog.
        if "name" not in info or not info["name"]:
            return None

        # Setup the new data to update.
        blog_data = dict(
            name=blog_info["name"],
            extra_meta=info.get("meta", {}),
            tumblr_uid=blog_info.get("uuid"),
            updated=datetime.datetime.fromtimestamp(blog_info.get("updated", 0)),
            data=blog_info
        )

        # Clean the data of null bytes.
        clean_data(blog_data)

        # Insert and query the blog object if it does not exist.
        if insert_only:
            return blog_data
        else:
            db.execute(insert(Blog).values(
                **blog_data
            ).on_conflict_do_update(index_elements=["tumblr_uid"], set_=blog_data))

            blog_object = db.query(Blog).filter(
                Blog.tumblr_uid == blog_data["tumblr_uid"]
            ).scalar()

        return blog_object

# Index for querying by url.
Index("index_blog_name", Blog.name)
Index("post_tumblr_id_unique", Post.tumblr_id, Post.author_id, unique=True)
Index("blog_uid_unique", Blog.tumblr_uid, unique=True)
