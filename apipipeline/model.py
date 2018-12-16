# This Python file uses the following encoding: utf-8
import os
import datetime

from urllib.parse import urlparse
from contextlib import contextmanager

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
        )

        # Post time
        if not post_object or not post_object.posted:
            post_epoch = info.get("timestamp", 0)
            post_data["posted"] = max(
                datetime.datetime.fromtimestamp(post_epoch),
                getattr(post_object, "posted", datetime.datetime.fromtimestamp(post_epoch))
            )

        # Set the author id if it is not set
        if (not post_object or not post_object.author_id) and "blog" in info:
            blog_name = info.get("blog_name", "")
            author_id = db_redis.hget("tumblr:blogids", blog_name)

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

            if author_id:
                post_data["author_id"] = author_id

        # Insert what's left of the data into the data
        post_data["data"] = info

        # Clean the data of null bytes.
        clean_data(post_data)

        # Create / Update the post object.
        if not post_object:
            # Insert and query the blog object if it does not exist.
            db.execute(insert(Post).values(
                **post_data
            ).on_conflict_do_update(index_elements=["tumblr_id", "author_id"], set_=post_data))

            if not insert_only:
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

        # Try to work with an existing blog object first.
        blog_object = db.query(Blog).filter(Blog.tumblr_uid == blog_info["uuid"]).scalar()

        # Setup the new data to update.
        blog_data = dict(
            name=blog_info.get("name", getattr(blog_object, "name", None)),
            extra_meta=info.get("meta", {})
        )

        # Update the updated time.
        updated_epoch = blog_info.get("updated", 0)
        blog_data["updated"] = max(
            datetime.datetime.fromtimestamp(updated_epoch),
            getattr(blog_object, "updated", datetime.datetime.fromtimestamp(updated_epoch))
        )

        # The UUID never changes.
        if not blog_object:
            blog_data["tumblr_uid"] = blog_info.get("uuid")

        # Insert what's left of the data into the data
        blog_data["data"] = blog_info

        # Clean the data of null bytes.
        clean_data(blog_data)

        if not blog_object:
            # Insert and query the blog object if it does not exist.
            db.execute(insert(Blog).values(
                **blog_data
            ).on_conflict_do_update(index_elements=["tumblr_uid"], set_=blog_data))

            blog_object = db.query(Blog).filter(
                Blog.tumblr_uid == blog_data["tumblr_uid"]
            ).scalar()
        else:
            for key, value in blog_data.items():
                setattr(blog_object, key, value)

        return blog_object

# Index for querying by url.
Index("index_blog_name", Blog.name)
Index("post_tumblr_id_unique", Post.tumblr_id, Post.author_id, unique=True)
Index("blog_uid_unique", Blog.tumblr_uid, unique=True)
