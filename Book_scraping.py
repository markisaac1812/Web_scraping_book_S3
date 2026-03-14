from json import load
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine


# Load .env from script directory so it's found when run from anywhere
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

username = (os.getenv("DB_USERNAME") or "").strip()
password = (os.getenv("PASSWORD") or "").strip()
host = (os.getenv("HOST") or "localhost").strip()
port = (os.getenv("PORT") or "5432").strip()
database = (os.getenv("DATABASE") or "").strip()

base_url = 'https://books.toscrape.com/catalogue/category/books/fiction_10/'
coulmns_in_dataframe = ['Title','Price','Rating','Availability']

def extarct(base_url:str, table_attribus: list,max_pages:int = None):
    all_books = []
    page =1
    while True:
        if page == 1:
            url = f"{base_url}index.html"
        else:
            url = f"{base_url}page-{page}.html"
        
        print(f"Scraping page {page}: {url}")
        
        try:
            response = requests.get(url)
            if response.status_code == 404:
                print(f"No more pages found. Stopped at page {page}")
                break
            
            response.raise_for_status() 
            
            data = BeautifulSoup(response.text, 'html.parser')
            books = data.select('article.product_pod')
            
            if not books:
                print(f"No books found on page {page}. Stopping.")
                break
            
            # Extract data from each book
            for book in books:
                title = book.h3.a['title']
                price = book.select_one('.price_color').text
                rating = book.select_one('.star-rating')['class'][1]
                availability = book.select_one('.availability').text.strip()
                
                all_books.append({
                    'Title': title,
                    'Price': price,
                    'Rating': rating,
                    'Availability': availability
                })
            
            print(f"  Found {len(books)} books on page {page}")
            
            # Check if we've hit the max_pages limit
            if max_pages and page >= max_pages:
                print(f"Reached maximum pages limit: {max_pages}")
                break
            
            page += 1
            
        except requests.RequestException as e:
            print(f"Error fetching page {page}: {e}")
            break
    
    # Convert list of dictionaries to DataFrame (much more efficient than appending rows)
    dataframe = pd.DataFrame(all_books, columns=table_attribus)
    return dataframe

def transform(dataframe):
    # transform price to egp
    dataframe['Price'] = dataframe['Price'].str.replace('Â£','')  # remove symbol Â£
    dataframe['Price'] = pd.to_numeric(dataframe['Price'], errors='coerce')  # convert to float
    dataframe['Price'] = dataframe['Price'] * 69.98  # convert to egp
    dataframe['Price'] = dataframe['Price'].round() # round to nearest integer
    return dataframe


db_url = f"postgresql+psycopg2://{quote_plus(username)}:{quote_plus(password)}@{host}:{port}/{database}"

def load_to_postgres_db(dataframe: pd.DataFrame, connection_string: str, table_name: str) -> None:
    """Load a DataFrame into a PostgreSQL table."""
    try:
        engine = create_engine(connection_string)
        
        dataframe.to_sql(
            table_name,
            con=engine,
            if_exists="replace",
            index=False
            )

        print(f"✓ Data loaded successfully to table '{table_name}'")
        engine.dispose()

    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        raise

data = extarct(base_url,coulmns_in_dataframe)
transformed_data = transform(data)
load_to_postgres_db(transformed_data,db_url,'books')

          