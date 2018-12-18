import json
import os
import asyncio
import functools
import logging

import asyncpg
import aiofiles
import traceback

from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

from bs4 import BeautifulSoup

from apipipeline import sentry_sdk

COMMON_WIDTHS = [2048, 1680, 1600, 1280, 1024, 1080, 768, 728, 500, 400, 250]
logger = logging.getLogger(__name__)

def extract_photos(data: dict, light: bool=False):
    urls = defaultdict(lambda: set())

    # Parse the trails too.
    for post in data.get("trail", []):
        trail_urls = extract_photos(post)
        urls.update(trail_urls)

    # Photo posts
    if "photos" in data:
        for photo in data["photos"]:
            urls["photo_original"].add(photo["original_size"]["url"])
            for alt_size in photo["alt_sizes"]:
                # Common widths from Tumblr and other digital sources.
                if alt_size["width"] in COMMON_WIDTHS:
                    photo_type_key = "photo_" + str(alt_size["width"])
                else:
                    photo_type_key = "photo_otheralts"
                urls[photo_type_key].add(alt_size["url"])

    # Link posts
    if "link_image" in data and data["link_image"]:
        urls["link"].add(data["link_image"])

    # General
    if "body" in data and data["body"]:
        parsed = BeautifulSoup(data["body"], 'html5lib')
        for tag in parsed.findAll("img"):
            urls["body"].add(tag["src"])

    if "content" in data and data["content"]:
        parsed = BeautifulSoup(data["content"], 'html5lib')
        for tag in parsed.findAll("img"):
            urls["content"].add(tag["src"])

    if "content_raw" in data and data["content_raw"]:
        parsed = BeautifulSoup(data["content_raw"], 'html5lib')
        for tag in parsed.findAll("img"):
            urls["content"].add(tag["src"])

    return dict(urls)


class ImageListGenerator:
    def __init__(self, loop):
        loop.set_exception_handler(self.on_asyncio_exception)

        self.loop = loop
        self.pool = ProcessPoolExecutor(max_workers=24)

        self.items = []
        self.images = []
        self.tasks = []

        self.files = {}
        self.completed = defaultdict(lambda: 0)

        self.running = True

    def on_asyncio_exception(self, loop, ctx):
        if sentry_sdk:
            sentry_sdk.capture_exception(ctx["exception"])
        traceback.print_exc()

    # File writing
    def get_file(self, filename):
        if filename not in self.files:
            self.files[filename] = open("urllists/" + filename, "w")
            # await self.files[filename].write("blog_name,post_id,url\n")
            self.files[filename].write("url\n")

        return self.files[filename]

    async def file_writer(self):
        while self.running or len(self.images) > 0:
            # Sleep a short while, we will get images soon.
            if len(self.images) == 0:
                await asyncio.sleep(0.1)
                continue

            # Pop a item, go back to start if we are late.
            try:
                image_type, image_data = self.images.pop()
            except IndexError:
                continue

            # Write CSV row.
            self.get_file(image_type).write("%s\n" % image_data)
            # await file.write("%s,%s,%s\n" % image_data)

            # Add totals up
            self.completed["total"] += 1
            self.completed[image_type] += 1

    # Row processing
    def get_items(self, to_pop=16):
        if len(self.items) == 0:
            return []

        result = []

        try:
            for x in range(0, to_pop):
                result.append(json.loads(self.items.pop()["data"]))
        except IndexError:
            pass

        return result

    async def process_content(self):
        while self.running or len(self.items) > 0:
            # Sleep a short while, we will get images soon.
            if len(self.items) == 0:
                await asyncio.sleep(0.1)
                continue

            # Pop a lot of items and gather them.
            items = self.get_items(512)
            if not items:
                continue

            futures = [self.loop.run_in_executor(
                self.pool,
                functools.partial(extract_photos, item)
            ) for item in items]

            for index, photoset in enumerate(await asyncio.gather(*futures)):
                data = items[index]
                # Do something with the photos.
                for image_type, images in photoset.items():
                    for image in images:
                        # self.images.append(
                        #     [image_type, (data["blog_name"], data["id"], image)]
                        # )
                        self.images.append(
                            [image_type, (image)]
                        )

    # Stats printing
    async def stats_printer(self):
        while self.running or len(self.images) > 0:
            output = f"self.items: {len(self.items)}; self.images: {len(self.images)}; "
            if self.completed:
                for item_type, item_count in self.completed.items():
                    output += f"{item_type}: {item_count}; "

            print(output)
            await asyncio.sleep(1)

    async def _run(self):
        # Connect to postgres.
        conn = await asyncpg.connect(dsn=os.environ["POSTGRES_URL"])
        logger.debug("PG connected.")

        # Spin up tasks
        for x in range(0, 8):
            self.tasks.append(asyncio.ensure_future(self.process_content()))
            logger.debug("Processor %d started.", x)

        for x in range(0, 2):
            self.tasks.append(asyncio.ensure_future(self.file_writer()))
            logger.debug("File writer %d started.", x)

        self.tasks.append(asyncio.ensure_future(self.stats_printer()))
        logger.debug("Stats printer started.")

        # Grab config from Redis
        # XXX Make this grab from Redis for real.
        last_crawled = 0

        # Iterate all posts
        async with conn.transaction():
            async for post in conn.cursor("SELECT * FROM posts_warehouse"):
                self.items.append(post)

        # Cleanup
        self.running = False
        await conn.close()
        await asyncio.gather(*self.tasks)
        for file_obj in self.files.values():
            await file_obj.close()

    async def run(self):
        try:
            await self._run()
        except KeyboardInterrupt:
            self.running = False

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    app = ImageListGenerator(loop)

    loop.run_until_complete(app.run())