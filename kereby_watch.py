#!/usr/bin/env python3
import os
import re
import sqlite3
import time
from typing import List, Set, Tuple

import requests
from playwright.sync_api import sync_playwright

START_URL = "https://kerebyudlejning.dk/"
DB_PATH = os.environ.get("KEREBY_DB", "kereby_seen.sqlite3")

# ntfy
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "").strip()  # fx "kereby-oeupdate"
NTFY_SERVER = os.environ.get("NTFY_SERVER", "https://ntfy.sh").rstrip("/")

# Hvor ofte du kører scriptet bestemmer "tempoet" – scriptet selv kører én gang og stopper.
# Statusord vi genkender på boligsiden (første “ord.” i toppen)
STATUS_RE = re.compile(r"\b(Ledig|Reserveret|Udlejet)\b\.?", re.IGNORECASE)


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


def send_ntfy(title: str, message: str, link: str) -> None:
    if not NTFY_TOPIC:
        raise RuntimeError("NTFY_TOPIC mangler. Sæt env var NTFY_TOPIC.")

    url = f"{NTFY_SERVER}/{NTFY_TOPIC}"
    headers = {
        "Title": title,
        "Click": link,
        "Priority": "high",
    }
    requests.post(url, data=message.encode("utf-8"), headers=headers, timeout=15).raise_for_status()


def fetch_bolig_urls() -> List[str]:
    """
    Åbner forsiden (JS) og samler links der matcher /bolig/...
    """
    urls: Set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(START_URL, wait_until="networkidle", timeout=60000)

        # Vent på at der er mindst ét bolig-link
        page.wait_for_selector('a[href*="/bolig/"]', timeout=60000)

        anchors = page.query_selector_all('a[href*="/bolig/"]')
        for a in anchors:
            href = a.get_attribute("href") or ""
            if "/bolig/" in href:
                if href.startswith("/"):
                    href = "https://kerebyudlejning.dk" + href
                urls.add(href.split("#")[0])

        browser.close()

    return sorted(urls)


def fetch_status_title_rent(url: str) -> Tuple[str, str, str]:
    """
    Henter boligsiden og udtrækker:
    - status (Ledig/Reserveret/Udlejet)
    - en titel (fra <title> eller første store tekst)
    - husleje (best-effort)
    """
    r = requests.get(url, timeout=30, headers={"User-Agent": "kereby-watch/1.0"})
    r.raise_for_status()
    html = r.text

    # status
    m = STATUS_RE.search(html)
    status = (m.group(1).lower() if m else "ukendt")

    # title (simpelt)
    title_m = re.search(r"<title>\s*(.*?)\s*</title>", html, re.IGNORECASE | re.DOTALL)
    title = title_m.group(1).strip() if title_m else "Kereby bolig"

    # husleje (simpelt: find “Leje 12.211 kr./md.”-agtigt)
    rent_m = re.search(r"Leje\s+([\d\.\s]+)\s*kr\./md\.", html, re.IGNORECASE)
    rent = (rent_m.group(1).strip() + " kr./md.") if rent_m else "Husleje: ukendt"

    return status, title, rent


def main() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        db_init(conn)

        bolig_urls = fetch_bolig_urls()

        new_ledig = 0
        for url in bolig_urls:
            status, title, rent = fetch_status_title_rent(url)

            # Kun ledige
            if status != "ledig":
                continue

            key = url  # brug URL som unik nøgle
            if already_seen(conn, key):
                continue

            msg = f"{title}\n{rent}\n{url}"
            send_ntfy("Ny ledig Kereby-bolig", msg, url)
            mark_seen(conn, key)
            new_ledig += 1

        print(f"Done. Nye ledige boliger notificeret: {new_ledig}")


if __name__ == "__main__":
    main()

while True:
    main()
    time.sleep(30)