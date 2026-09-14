import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "http://books.toscrape.com/"

def scrape_multiple_pages(start_url: str, max_pages: int = 3):
    current_url = start_url
    all_books = []
    pages_scraped = 0
    
    while current_url and pages_scraped < max_pages:
        response = requests.get(current_url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        for article in soup.select("article.product_pod"):
            title = article.h3.a["title"]
            price = article.select_one("p.price_color").text.strip()
            all_books.append({"title": title, "price": price})
            
        pages_scraped += 1
        
        next_btn = soup.select_one("li.next a")
        if next_btn:
            current_url = urljoin(current_url, next_btn["href"])
        else:
            current_url = None
            
    return all_books, pages_scraped

if __name__ == "__main__":
    books, pages = scrape_multiple_pages(BASE_URL, max_pages=3)
    print(f"Scraped {len(books)} books across {pages} pages.")
