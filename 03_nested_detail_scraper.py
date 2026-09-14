import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "http://books.toscrape.com/"

def scrape_book_details(detail_url: str):
    response = requests.get(detail_url, timeout=10)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, "html.parser")
    title = soup.find("h1").text
    price = soup.select_one("p.price_color").text.strip()
    availability = soup.select_one("p.instock.availability").text.strip()
    
    return {"title": title, "price": price, "availability": availability}

def scrape_nested_catalog(catalog_url: str, limit: int = 3):
    response = requests.get(catalog_url, timeout=10)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, "html.parser")
    books = []
    
    # Target detail links inside article pods
    for article in soup.select("article.product_pod")[:limit]:
        relative_link = article.h3.a["href"]
        full_detail_url = urljoin(catalog_url, relative_link)
        
        detail_data = scrape_book_details(full_detail_url)
        books.append(detail_data)
        
    return books

if __name__ == "__main__":
    results = scrape_nested_catalog(BASE_URL, limit=3)
    for idx, item in enumerate(results, 1):
        print(f"[{idx}] {item['title']} | {item['price']} | {item['availability']}")
