import feedparser, time, logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
seen = set()

def scrape_tc_rss():
    logging.info("Fetching TechCrunch RSS feed")
    feed = feedparser.parse("https://techcrunch.com/tag/funding/feed/")
    posts = [{"title": e.title, "url": e.link} for e in feed.entries[:5] if e.link not in seen]
    for p in posts: seen.add(p["url"])
    logging.info(f"Found {len(posts)} new posts")
    return posts

def poll_tc():
    while True:
        logging.info("Polling TechCrunch...")
        new = scrape_tc_rss()
        if new:
            for i, p in enumerate(new, 1): print(f"{i}. {p['title']} → {p['url']}")
            print("-"*50)
        else: logging.info("No new posts found")
        time.sleep(600)

if __name__ == "__main__": poll_tc()
