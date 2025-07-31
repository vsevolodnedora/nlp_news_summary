import csv
import hashlib
import os
import re
import sqlite3
import zlib
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

from logger import get_logger

logger = get_logger(__name__)

class PostsDatabase:
    """Connects to the Posts database."""

    def __init__(self, db_path: str) -> None:
        """Initialize the database connection."""

        self.db_path = db_path
        # connect and enable parsing of timestamps
        self.conn = sqlite3.connect(
            self.db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        self.conn.execute("PRAGMA foreign_keys = ON;")
        logger.info(f"Connected to database: {db_path}")

    def close(self) -> None:
        """Close the database connection."""

        self.conn.close()

    def check_create_table(self, table_name: str) -> None:
        """Checks if the given table exists in the database."""

        # Validate table name
        if not re.match(r'^[A-Za-z0-9_]+$', table_name):
            raise ValueError(f"Invalid table name: {table_name}")
        sql = f"""
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            ID TEXT PRIMARY KEY,
            published_on TIMESTAMP NOT NULL,
            title TEXT NOT NULL,
            added_on TIMESTAMP NOT NULL,
            url TEXT NOT NULL,
            post BLOB NOT NULL
        );
        """
        self.conn.execute(sql)
        self.conn.commit()
        logger.info(f"Ensured table exists: {table_name}")

    def is_table(self, table_name: str) -> bool:
        """Returns whether the given table is present in the database."""
        if not re.match(r"^[A-Za-z0-9_]+$", table_name):
            return False
        cursor = self.conn.execute(
            "SELECT count(name) FROM sqlite_master WHERE type='table' AND name=?;", (table_name,)
        )
        exists = cursor.fetchone()[0] > 0
        return exists

    def create_post_id(self, post_url: str) -> str:
        """Creates a new post id for the given URL which is assumed to be unique."""
        return hashlib.sha256(post_url.encode("utf-8")).hexdigest()

    def compress_post_text(self, article_id: str, text: str) -> bytes:
        """Compresses the given article ID and text into bytes."""

        logger.debug(f"Compressing article ID: {article_id}")
        return zlib.compress(text.encode('utf-8'), level=6)

    def decompress_post_text(self, article_id: str, text: bytes) -> str:
        """Decompresses the given article ID and text into bytes."""

        logger.debug(f"Decompressing article ID: {article_id}")
        try:
            return zlib.decompress(text).decode("utf-8")
        except zlib.error:
            # fallback: assume text is plain bytes
            return text.decode("utf-8", errors="ignore")
        except Exception as e:
            logger.error(f"Failed to decompress article ID {article_id}: {e}")
            raise ValueError(
                f"Failed to decompress article (ID={article_id})."
            ) from e

    def is_post(self, table_name: str, post_id: str) -> bool:
        """Returns whether the given article ID is present in the database in the given table."""

        if not self.is_table(table_name):
            return False
        cursor = self.conn.execute(
            f"SELECT COUNT(ID) FROM \"{table_name}\" WHERE ID = ?;", (post_id,)
        )
        return cursor.fetchone()[0] > 0

    def add_post(
        self,
        table_name: str,
        published_on: str,
        title: str,
        post_url: str,
        post: str,
        overwrite: bool = False
    ) -> None:
        """Add post to the given table, overwriting existing one if needed, compressing before adding."""
        logger.debug(f"Adding post to table: {table_name}")
        # ensure table exists
        if not self.is_table(table_name):
            raise ValueError(f"Table {table_name} does not exist.")
        # determine post ID
        post_id = self.create_post_id(post_url)
        exists = self.is_post(table_name, post_id)
        if exists and not overwrite:
            logger.debug(
                f"Post exists in {table_name} (url={post_url}, id={post_id}), skipping."
            )
            return
        # parse published_on
        # if only date, add default time 12:00
        if re.match(r'^\d{4}-\d{2}-\d{2}$', published_on):
            published_on = published_on + ' 12:00:00'
        try:
            published_dt = datetime.fromisoformat(published_on)
        except ValueError as e:
            raise ValueError(
                f"Invalid published_on format: {published_on}. Use YYYY-MM-DD or YYYY-MM-DD HH:MM[:SS]"
            ) from e
        # timestamp for insertion
        added_dt = datetime.now()
        compressed = self.compress_post_text(post_id, post)
        if exists and overwrite:
            sql = f"""
            UPDATE "{table_name}"
               SET published_on = ?, title = ?, added_on = ?, url = ?, post = ?
             WHERE ID = ?;
            """
            params = (
                published_dt,
                title,
                added_dt,
                post_url,
                compressed,
                post_id,
            )
        else:
            sql = f"""
            INSERT INTO "{table_name}"
                   (ID, published_on, title, added_on, url, post)
            VALUES (?, ?, ?, ?, ?, ?);
            """
            params = (
                post_id,
                published_dt,
                title,
                added_dt,
                post_url,
                compressed,
            )
        self.conn.execute(sql, params)
        self.conn.commit()
        logger.info(
            f"Post {'updated' if exists else 'added'} in {table_name}: id={post_id}, title={title}"
        )

    def get_post(self, table_name: str, post_id: str) -> str:
        """Returns decompressed post text from the given table."""

        logger.debug(f"Retrieving post from {table_name} with id: {post_id}")
        if not self.is_table(table_name):
            raise ValueError(f"Table {table_name} does not exist.")
        if not self.is_post(table_name, post_id):
            raise ValueError(
                f"Post id {post_id} does not exist in table {table_name}."
            )
        sql = f"SELECT post FROM \"{table_name}\" WHERE ID = ?;"
        cursor = self.conn.execute(sql, (post_id,))
        row = cursor.fetchone()
        if not row:
            raise ValueError(
                f"No data retrieved for post id {post_id} in table {table_name}."
            )
        compressed = row[0]
        return self.decompress_post_text(post_id, compressed)

    def list_posts(self, table_name: str) -> list[dict]:
        """Returns a list of all posts in the given table in a json format."""

        if not self.is_table(table_name):
            raise ValueError(f"Table {table_name} does not exist.")
        logger.debug(f"Listing all posts in table: {table_name}")
        sql = f"SELECT ID, published_on, title, added_on, url, post FROM \"{table_name}\";"
        cursor = self.conn.execute(sql)
        posts = []
        for row in cursor.fetchall():
            pid, pub_dt, title, add_dt, url, blob = row
            text = self.decompress_post_text(pid, blob)
            posts.append({
                "ID": pid,
                "published_on": pub_dt.isoformat() if isinstance(pub_dt, datetime) else str(pub_dt),
                "title": title,
                "added_on": add_dt.isoformat() if isinstance(add_dt, datetime) else str(add_dt),
                "url": url,
                "post": text,
            })
        return posts

    def dump_posts_as_markdown(self, table_name: str, out_dir: str) -> None:
        """Saves each post as a markdown file in out_dir."""

        if not self.is_table(table_name):
            raise ValueError(f"Table {table_name} does not exist.")
        logger.debug(
            f"Dumping posts from {table_name} to markdown in directory: {out_dir}"
        )
        os.makedirs(out_dir, exist_ok=True)
        posts = self.list_posts(table_name)
        for article in posts:
            # parse datetime
            pub_dt = datetime.fromisoformat(article["published_on"])

            # sanitize title for filename
            safe_title = re.sub(r"[^A-Za-z0-9_-]", "_", article["title"])
            date_str = pub_dt.strftime("%Y-%m-%d_%H-%M")
            fname = f"{date_str}__{safe_title}.md"
            path = os.path.join(out_dir, fname)
            # write markdown with front matter
            content = article["post"]
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
            logger.info(f"Wrote markdown: {path}")
