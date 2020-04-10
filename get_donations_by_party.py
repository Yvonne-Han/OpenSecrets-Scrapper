import re
import requests
import pandas as pd
from bs4 import BeautifulSoup

def get_donations_by_party(some_url):
    page = requests.get(some_url)
    soup = BeautifulSoup(page.content, "html.parser")

    # To get an idea of the webpage structure,
    # print(soup.prettify()) 

    table = soup.find('table', attrs = {'class': "DataTable-Partial", 'data-title': 'Contributions by Party of Recipient'})
    table_rows = table.find_all('tr')[1:]

    l = []
    k = []

    for tr in table_rows:
        td = tr.find_all('td')
        row = [re.sub("\D", "", tr.text) for tr in td]
        l.append(row)

    k = ['cycle', 'total', 'democrats', 'pct_democrats', 'republicans', 'pct_republicans']
    
    df = pd.DataFrame(l, columns = k)
    
    cols = df.columns
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
    
    df['pct_democrats'] = df['pct_democrats'].div(100).round(2)
    df['pct_republicans'] = df['pct_republicans'].div(100).round(2)
    
    
    return df

# Sample call: 
# get_donations_by_party("https://www.opensecrets.org/orgs//totals?id=D000037780&cycle=2016")
