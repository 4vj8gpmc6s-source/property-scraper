# scraper.py
import yaml, os
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlayTimeout
from utils import choose_user_agent, retry_backoff, send_telegram
from db import init_db, save_rows
import pandas as pd
from time import sleep
import random

CFG_PATH = "config.yml"

def load_config(path=CFG_PATH):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def scrape_site(page, site, cfg):
    rows = []
    url = site["url"]
    ua = choose_user_agent(cfg.get("user_agents", []))
    try:
        page.set_extra_http_headers({"User-Agent": ua})
    except Exception:
        pass
    page.goto(url, timeout=cfg.get("scrape", {}).get("timeout", 60000))
    page.wait_for_timeout(1000)
    try:
        page.wait_for_selector(site["list_selector"], timeout=15000)
    except Exception:
        return rows

    cards = page.query_selector_all(site["list_selector"])
    for c in cards:
        try:
            title = ""
            price = ""
            details = ""
            url_el = None

            if site.get("title_selector") and c.query_selector(site.get("title_selector")):
                title = c.query_selector(site.get("title_selector")).inner_text().strip()
            if site.get("price_selector") and c.query_selector(site.get("price_selector")):
                price = c.query_selector(site.get("price_selector")).inner_text().strip()
            if site.get("details_selector") and c.query_selector(site.get("details_selector")):
                details = c.query_selector(site.get("details_selector")).inner_text().strip()
            if site.get("url_selector") and c.query_selector(site.get("url_selector")):
                url_el = c.query_selector(site.get("url_selector"))

            link = url_el.get_attribute("href") if url_el else ""
            if link and not link.startswith("http"):
                from urllib.parse import urljoin
                link = urljoin(site["url"], link)

            rows.append({
                "source": site["name"],
                "url": link,
                "title": title,
                "price": price,
                "details": details,
                "scraped_date": datetime.utcnow().strftime("%Y-%m-%d")
            })
        except Exception as e:
            print("Card parse error:", e)
    sleep(random.uniform(cfg.get("throttle", {}).get("min_sleep_ms",800)/1000.0, cfg.get("throttle", {}).get("max_sleep_ms",2000)/1000.0))
    return rows

def main():
    cfg = load_config()
    out_folder = cfg.get("output_folder", "data")
    os.makedirs(out_folder, exist_ok=True)
    db_path = cfg.get("db_path", os.path.join(out_folder, "properties.db"))
    init_db(db_path)
    all_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=cfg.get("scrape", {}).get("headless", True))
        context = browser.new_context()
        page = context.new_page()
        for site in cfg.get("sites", []):
            try:
                rows = retry_backoff(lambda: scrape_site(page, site, cfg), retries=3)
                print(f"Scraped {len(rows)} from {site['name']}")
                all_rows.extend(rows)
            except PlayTimeout as e:
                print(f"Timeout for {site['name']}: {e}")
            except Exception as e:
                print(f"Failed site {site['name']}: {e}")
        browser.close()

    today = datetime.utcnow().strftime("%Y_%m_%d")
    csv_path = os.path.join(out_folder, f"scraped_{today}.csv")
    df = pd.DataFrame(all_rows)
    if not df.empty:
        df.to_csv(csv_path, index=False)
        save_rows(db_path, all_rows)

    if cfg.get("telegram", {}).get("enabled"):
        token = os.environ.get("TELEGRAM_TOKEN") or cfg["telegram"].get("bot_token")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID") or cfg["telegram"].get("chat_id")
        summary = f"Scraping finished: {len(all_rows)} listings saved. File: {csv_path}"
        send_telegram(token, chat_id, summary)
    print("Done.")

if __name__ == "__main__":
    main()
