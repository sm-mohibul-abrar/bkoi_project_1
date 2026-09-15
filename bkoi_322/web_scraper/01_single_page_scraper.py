import requests
from bs4 import BeautifulSoup

URL = "http://books.toscrape.com/"

def scrape_single_page(url: str):
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, "html.parser")
    books = []
    
    for article in soup.select("article.product_pod"):
        title = article.h3.a["title"]
        price = article.select_one("p.price_color").text.strip()
        books.append({"title": title, "price": price})
        
    return books

if __name__ == "__main__":
    results = scrape_single_page(URL)
    print(f"Scraped {len(results)} books.")
    print("First item sample:", results[0] if results else "No data")
