#!/usr/bin/env python3
import os
import sqlite3
import time
from typing import List, Set

import requests
from playwright.sync_api import sync_playwright

START_URL = "https://kerebyudlejning.dk/"
DB_PATH = os.environ.get("KEREBY_DB", "kereby_seen.sqlite3")

NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "").strip()
NTFY_SERVER = os.environ.get("NTFY_SERVER", "https://ntfy.sh").rstrip("/")
MAX_LISTINGS = int(os.environ.get("MAX_LISTINGS", "80"))
PAGE_TIMEOUT_MS = int(os.environ.get("PAGE_TIMEOUT_MS", "45000"))


def db_init(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS seen (
            url TEXT PRIMARY KEY,
            first_seen_ts INTEGER NOT NULL
        )
        """
    )
    conn.commit()


def already_seen(conn: sqlite3.Connection, url: str) -> bool:
    cur = conn.execute("SELECT 1 FROM seen WHERE url = ?", (url,))
    return cur.fetchone() is not None


def mark_seen(conn: sqlite3.Connection, url: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO seen (url, first_seen_ts) VALUES (?, ?)",
        (url, int(time.time())),
    )
    conn.commit()


def send_ntfy(session: requests.Session, title: str, message: str, link: str) -> None:
    if not NTFY_TOPIC:
        raise RuntimeError("NTFY_TOPIC is missing (set env var NTFY_TOPIC).")

    url = f"{NTFY_SERVER}/{NTFY_TOPIC}"
    headers = {"Title": title, "Click": link, "Priority": "high"}
    session.post(url, data=message.encode("utf-8"), headers=headers, timeout=15).raise_for_status()


def fetch_bolig_urls() -> List[str]:
    """
    Render homepage and collect /bolig/ links (fast settings).
    """
    urls: List[str] = []
    seen: Set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        def route_handler(route):
            if route.request.resource_type in ("image", "media", "font"):
                return route.abort()
            return route.continue_()

        page.route("**/*", route_handler)

        page.goto(START_URL, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        page.wait_for_selector('a[href*="/bolig/"]', timeout=PAGE_TIMEOUT_MS)

        anchors = page.query_selector_all('a[href*="/bolig/"]')
        for a in anchors:
            href = (a.get_attribute("href") or "").split("#")[0]
            if "/bolig/" not in href:
                continue
            if href.startswith("/"):
                href = "https://kerebyudlejning.dk" + href
            if href not in seen:
                seen.add(href)
                urls.append(href)
            if len(urls) >= MAX_LISTINGS:
                break

        browser.close()

    return urls


def main() -> None:
    session = requests.Session()
    session.headers.update({"User-Agent": "kereby-new-listings/1.0"})

    with sqlite3.connect(DB_PATH) as conn:
        db_init(conn)

        urls = fetch_bolig_urls()

        new_count = 0
        for url in urls:
            if already_seen(conn, url):
                continue

            send_ntfy(session, "Ny Kereby listing", url, url)
            mark_seen(conn, url)
            new_count += 1

        print(f"Done. New listings: {new_count}. Checked: {len(urls)}")


if __name__ == "__main__":
    main()
