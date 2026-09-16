import random
import time
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

BASE_URL = "http://books.toscrape.com/"

def create_robust_session() -> requests.Session:
    session = requests.Session()
    
    # Standard desktop User-Agent
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0"
    })
    
    # Retry policy for transient HTTP errors
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def production_scrape(url: str, limit: int = 2):
    session = create_robust_session()
    response = session.get(url, timeout=10)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, "html.parser")
    books = []
    
    for article in soup.select("article.product_pod")[:limit]:
        title = article.h3.a["title"]
        price = article.select_one("p.price_color").text.strip()
        books.append({"title": title, "price": price})
        
        # Rate-limiting delay (polite crawling)
        time.sleep(random.uniform(0.5, 1.2))
        
    return books

if __name__ == "__main__":
    results = production_scrape(BASE_URL, limit=2)
    print("Production scrape complete. Results:")
    for item in results:
        print(f"- {item['title']}: {item['price']}")
