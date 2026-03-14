from json import load
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Load .env from script directory so it's found when run from anywhere
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

username = (os.getenv("DB_USERNAME") or "").strip()
password = (os.getenv("PASSWORD") or "").strip()
host = (os.getenv("HOST") or "localhost").strip()
port = (os.getenv("PORT") or "5432").strip()
database = (os.getenv("DATABASE") or "").strip()
url = 'https://books.toscrape.com/catalogue/category/books/fiction_10/index.html'
coulmns_in_dataframe = ['Title','Price','rating','Availability']

def extarct(url: str, table_attribus: list):
    response = requests.get(url).text
    data = BeautifulSoup(response,'html.parser')
    dataframe = pd.DataFrame(columns = table_attribus);
    books = data.select('article.product_pod')
    for book in books:
        title = book.h3.a['title'] # for tags use .tags but for sths inside tags use .tag['sth']
        price = book.select_one('.price_color').text
        rating = book.select_one('.star-rating')['class'][1]
        avalaibility = book.select_one('.availability').text.strip()
        dataframe.loc[len(dataframe)] = [title,price,rating,avalaibility]
    return dataframe

def transform(dataframe):
    # transform price to egp
    dataframe['Price'] = dataframe['Price'].str.replace('Â£','')  # remove symbol Â£
    dataframe['Price'] = pd.to_numeric(dataframe['Price'], errors='coerce')  # convert to float
    dataframe['Price'] = dataframe['Price'] * 69.98  # convert to egp
    dataframe['Price'] = dataframe['Price'].round() # round to nearest integer
    return dataframe


connection_url = (
    f"postgresql://{quote_plus(username)}:{quote_plus(password)}@{host}:{port}/{database}"
)

print(f"\nConnection URL (masked): postgresql://{username}:****@{host}:{port}/{database}")

def load_to_postgres_db(dataframe: pd.DataFrame, connection_string: str, table_name: str) -> None:
    """Load a DataFrame into a PostgreSQL table."""
    try:
        engine = create_engine(connection_string)
        # Test connection
        with engine.connect() as conn:
            print("✓ Database connection successful!")
        
        # Load data
        with engine.begin() as conn:
            dataframe.to_sql(
                table_name,
                conn,
                index=False,
                if_exists="append",
                method="multi",
                chunksize=500,
            )
        print(f"✓ Data loaded successfully to table '{table_name}'")
        engine.dispose()
        
    except SQLAlchemyError as e:
        err = str(e.orig) if getattr(e, "orig", None) else str(e)
        if "password authentication failed" in err.lower():
            print("✗ Database error: Password authentication failed. Check that:")
            print("  1. PASSWORD in .env matches the PostgreSQL password for user", username)
            print("  2. PostgreSQL is running and accepting connections on", host, ":", port)
        else:
            print(f"✗ Database error: {e}")
        raise
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        raise

data = extarct(url,coulmns_in_dataframe)
transformed_data = transform(data)
load_to_postgres_db(transformed_data,connection_url,'books')