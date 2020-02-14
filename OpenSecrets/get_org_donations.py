import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.types import Integer

# for a given url, return the donation data
def get_donation_data(some_url):
    page = requests.get(some_url)
    soup = BeautifulSoup(page.content, "html.parser")

    # print(soup.prettify())

    table = soup.find('table', attrs = {'class': "datadisplay"})
    table_rows = table.find_all('tr')[1:-1]

    l = []

    for tr in table_rows:
        td = tr.find_all('td')
        row = [re.sub("\D", "", tr.text) for tr in td]
        l.append(row)

    k = ['cycle', 'total', 'democrats', 'republicans', 'pct_democrats', 'pct_republicans', 'individuals', 'pacs', 'soft_indivs', 'soft_orgs']
    df = pd.DataFrame(l, columns = k)
    
    cols = df.columns
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
    
    df['pct_democrats'] = df['pct_democrats'].div(100).round(2)
    df['pct_republicans'] = df['pct_republicans'].div(100).round(2)
    
    return df
    
# Sample call: Starbucks
# get_donation_data("https://www.opensecrets.org/orgs/totals.php?id=D000037780&cycle=2016") 

# Scrap the data from OpenSecrets website and save it in the server
ticker_comnam_id_matched = pd.read_csv("ticker_comnam_id_matched.csv")

conn_string = 'postgresql://localhost/crsp'
engine = create_engine(conn_string)
conn = engine.connect()

# Get the corp summary data from OpenSecrets for all firms in ticker_comnam_id_matched
for i in range(len(ticker_comnam_id_matched)):
    try: 
        org_id = ticker_comnam_id_matched.loc[i, 'org_id']
        org_summary_url = "https://www.opensecrets.org/orgs/totals.php?id=" + org_id

        df = get_donation_data(org_summary_url)

        # add firm identifiers
        df.insert(0, 'ticker', ticker_comnam_id_matched.loc[i, 'ticker'])
        df.insert(0, 'comnam', ticker_comnam_id_matched.loc[i, 'comnam'])
        df.insert(0, 'org_id', org_id)

        df.to_sql("org_donations", conn, schema="mschabus", if_exists='append', dtype={"cycle": Integer()}, index=False)
        
    except:
        pass
        
conn.execute("ALTER TABLE nytimes OWNER TO mschabus")
conn.close()
