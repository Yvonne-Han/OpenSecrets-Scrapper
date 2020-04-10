import re
import requests
import pandas as pd
from bs4 import BeautifulSoup

def get_donations_by_source(some_url):
    page = requests.get(some_url)
    soup = BeautifulSoup(page.content, "html.parser")

    # print(soup.prettify())

    table = soup.find('table', attrs = {'class': "DataTable-Partial", 'data-title': 'Contributions by Source of Funds'})
    table_rows = table.find_all('tr')[1:]

    l = []
    k = []

    for tr in table_rows:
        td = tr.find_all('td')
        row = [re.sub("\D", "", tr.text) for tr in td]
        l.append(row)

    k = ['cycle', 'individuals', 'pacs', 'soft_individual', 'soft_organisation']
    
    df = pd.DataFrame(l, columns = k)
    
    cols = df.columns
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
    
    
    return df

# Sample Call:
# get_donations_by_source("https://www.opensecrets.org/orgs//totals?id=D000037780&cycle=2016")
