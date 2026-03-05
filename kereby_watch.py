#!/usr/bin/env python3
import os
import re
import sqlite3
import time
from typing import List, Set, Tuple, Optional

import requests
from playwright.sync_api import sync_playwright

START_URL = "https://kerebyudlejning.dk/"
DB_PATH = os.environ.get("KEREBY_DB", "kereby_seen.sqlite3")

# ntfy
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "").strip()
NTFY_SERVER = os.environ.get("NTFY_SERVER", "https://ntfy.sh").rstrip("/")

# Optional speed knobs
MAX_LISTINGS = int(os.environ.get("MAX_LISTINGS", "60"))  # only check newest N urls
PAGE_TIMEOUT_MS = int(os.environ.get("PAGE_TIMEOUT_MS", "45000"))  # Playwright page timeout
REQ_TIMEOUT_S = int(os.environ.get("REQ_TIMEOUT_S", "20"))  # requests timeout per listing

STATUS_RE = re.compile(r"\b(Ledig|Reserveret|Udlejet)\b\.?", re.IGNORECASE)
RENT_RE = re.compile(r"Leje\s+([\d\.\s]+)\s*kr\./md\.", re.IGNORECASE)
TITLE_RE = re.compile(r"<title>\s*(.*?)\s*</title>", re.IGNORECASE | re.DOTALL)


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
        raise RuntimeError("NTFY_TOPIC is missing (set env var NTFY_TOPIC).")

    url = f"{NTFY_SERVER}/{NTFY_TOPIC}"
    headers = {
        "Title": title,
        "Click": link,
        "Priority": "high",
    }
    session.post(url, data=message.encode("utf-8"), headers=headers, timeout=15).raise_for_status()


def fetch_bolig_urls() -> List[str]:
    """
    Open homepage (JS) and collect links matching /bolig/...
    Faster settings:
    - domcontentloaded (not networkidle)
    - block images/fonts/media to reduce load
    """
    urls: List[str] = []
    seen: Set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        def route_handler(route):
            rtype = route.request.resource_type
            if rtype in ("image", "media", "font"):
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

        browser.close()

    # Keep page order (newest typically first). Only check the first MAX_LISTINGS.
    return urls[:MAX_LISTINGS]


def fetch_status_title_rent(session: requests.Session, url: str) -> Tuple[str, str, str]:
    r = session.get(url, timeout=REQ_TIMEOUT_S)
    r.raise_for_status()
    html = r.text

    m = STATUS_RE.search(html)
    status = m.group(1).lower() if m else "ukendt"

    title_m = TITLE_RE.search(html)
    title = title_m.group(1).strip() if title_m else "Kereby bolig"

    rent_m = RENT_RE.search(html)
    rent = (rent_m.group(1).strip() + " kr./md.") if rent_m else "Husleje: ukendt"

    return status, title, rent


def main() -> None:
    # Reuse connections (faster)
    session = requests.Session()
    session.headers.update({"User-Agent": "kereby-watch/1.1"})

    with sqlite3.connect(DB_PATH) as conn:
        db_init(conn)

        bolig_urls = fetch_bolig_urls()

        new_ledig = 0
        for url in bolig_urls:
            try:
                status, title, rent = fetch_status_title_rent(session, url)
            except Exception as e:
                # Don’t fail the whole run due to one slow/bad listing
                print(f"WARN: failed to fetch listing: {url} ({e})")
                continue

            if status != "ledig":
                continue

            key = url
            if already_seen(conn, key):
                continue

            msg = f"{title}\n{rent}\n{url}"
            send_ntfy(session, "Ny ledig Kereby-bolig", msg, url)
            mark_seen(conn, key)
            new_ledig += 1

        print(f"Done. Nye ledige boliger notificeret: {new_ledig}")


if __name__ == "__main__":
    main()
