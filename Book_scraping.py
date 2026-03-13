import requests
from bs4 import BeautifulSoup
import pandas as pd

url = 'https://books.toscrape.com/catalogue/category/books/fiction_10/index.html'

coulmns_in_dataframe = ['Title','Price','rating','Availability']

def extarct(url:string,table_attribus:list):
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

df = extarct(url,coulmns_in_dataframe)
print(df.head())        
    



