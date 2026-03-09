#!/usr/bin/env python3
import os
import sqlite3
import time
from typing import List, Set, Tuple

import requests
from playwright.sync_api import sync_playwright

KEREBY_URL = "https://kerebyudlejning.dk/"
CEJ_URL = "https://udlejning.cej.dk/find-bolig"

DB_PATH = os.environ.get("KEREBY_DB", "kereby_seen.sqlite3")
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "").strip()
NTFY_SERVER = os.environ.get("NTFY_SERVER", "https://ntfy.sh").rstrip("/")

MAX_LISTINGS = int(os.environ.get("MAX_LISTINGS", "30"))
PAGE_TIMEOUT_MS = int(os.environ.get("PAGE_TIMEOUT_MS", "45000"))


def db_init(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS seen (
            key TEXT PRIMARY KEY,
            first_seen_ts INTEGER NOT NULL
        )
        """
    )
    conn.commit()


def already_seen(conn: sqlite3.Connection, key: str) -> bool:
    cur = conn.execute("SELECT 1 FROM seen WHERE key = ?", (key,))
    return cur.fetchone() is not None


def mark_seen(conn: sqlite3.Connection, key: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO seen (key, first_seen_ts) VALUES (?, ?)",
        (key, int(time.time())),
    )
    conn.commit()


def send_ntfy(session: requests.Session, title: str, message: str, link: str) -> None:
    if not NTFY_TOPIC:
        raise RuntimeError("NTFY_TOPIC mangler.")

    url = f"{NTFY_SERVER}/{NTFY_TOPIC}"
    headers = {
        "Title": title,
        "Click": link,
        "Priority": "high",
    }
    session.post(
        url,
        data=message.encode("utf-8"),
        headers=headers,
        timeout=15,
    ).raise_for_status()


def _block_heavy_resources(page) -> None:
    def route_handler(route):
        if route.request.resource_type in ("image", "media", "font"):
            return route.abort()
        return route.continue_()

    page.route("**/*", route_handler)


def fetch_kereby_urls() -> List[str]:
    urls: List[str] = []
    seen_urls: Set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        _block_heavy_resources(page)

        page.goto(KEREBY_URL, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        page.wait_for_selector('a[href*="/bolig/"]', timeout=PAGE_TIMEOUT_MS)

        anchors = page.query_selector_all('a[href*="/bolig/"]')
        for a in anchors:
            href = (a.get_attribute("href") or "").split("#")[0]

            if "/bolig/" not in href:
                continue

            if href.startswith("/"):
                href = "https://kerebyudlejning.dk" + href

            if href in seen_urls:
                continue

            seen_urls.add(href)
            urls.append(href)

            if len(urls) >= MAX_LISTINGS:
                break

        browser.close()

    return urls


def fetch_cej_urls() -> List[Tuple[str, str]]:
    """
    Returnerer [(url, tekst)] for CEJ:
    - kun /boliger/ links
    - kun hvis anchor/korttekst indeholder 'København'
    """
    results: List[Tuple[str, str]] = []
    seen_urls: Set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        _block_heavy_resources(page)

        page.goto(CEJ_URL, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        page.wait_for_selector('a[href*="/boliger/"]', timeout=PAGE_TIMEOUT_MS)

        anchors = page.query_selector_all('a[href*="/boliger/"]')
        for a in anchors:
            href = (a.get_attribute("href") or "").split("#")[0]
            text = (a.inner_text() or "").strip()

            if "/boliger/" not in href:
                continue

            if "københavn" not in text.lower():
                continue

            if href.startswith("/"):
                href = "https://udlejning.cej.dk" + href

            if href in seen_urls:
                continue

            seen_urls.add(href)
            results.append((href, text))

            if len(results) >= MAX_LISTINGS:
                break

        browser.close()

    return results


def main() -> None:
    session = requests.Session()
    session.headers.update({"User-Agent": "housing-watch/1.0"})

    with sqlite3.connect(DB_PATH) as conn:
        db_init(conn)

        kereby_urls = fetch_kereby_urls()
        cej_results = fetch_cej_urls()

        kereby_new = 0
        cej_new = 0

        for url in kereby_urls:
            key = f"kereby:{url}"
            if already_seen(conn, key):
                continue

            send_ntfy(session, "Ny Kereby listing", url, url)
            mark_seen(conn, key)
            kereby_new += 1

        for url, text in cej_results:
            key = f"cej:{url}"
            if already_seen(conn, key):
                continue

            message = text if text else url
            send_ntfy(session, "Ny CEJ listing i København", message, url)
            mark_seen(conn, key)
            cej_new += 1

        print(
            f"Done. Kereby new: {kereby_new}. "
            f"CEJ København new: {cej_new}. "
            f"Kereby checked: {len(kereby_urls)}. "
            f"CEJ checked: {len(cej_results)}"
        )


if __name__ == "__main__":
    main()
