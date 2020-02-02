import re
import requests
import json
import pandas as pd
import urllib
import csv

# take a string as input and output the keywords of comnam
def clean_comnam(comnam):
    suffixes = r"\s(\bco\b|\bcorp\b|\bcorporation\b|\bgroup\b|\bltd\b|\binc\b|\bdel\b\s{0}|\bnew\b\s{0})"
    comnam_no_suffixes = re.sub(suffixes, "", comnam.lower())
    single_letter = re.compile(r'\b([a-z]) (?=[a-z]\b)', re.I)
    comnam_compiled = re.sub(single_letter, r'\g<1>', comnam_no_suffixes)
    return re.sub("\s", "%20", comnam_compiled)

# import ticker_comnam and create two new columns
ticker_comnams = pd.read_csv("~/Campaign-Finances/ticker_comname.csv")
ticker_comnams['donor_name'] = None
ticker_comnams['candidates'] = None


# access the api and get a donor_name if exists
# if multiple exist, put them into candidates and leave donor_name blank
for i in range(len(ticker_comnams)):
    donor_name = ""
    candidates = []
    comnam = clean_comnam(ticker_comnams['comnam'][i])
    
    # api url: see https://maplight.org/data_guide/contribution-search-api-documentation/
    url_comnam = "https://api.maplight.org/maplight-api/fec/donors/" + comnam
    r = requests.get(url_comnam)
    response = json.loads(r.content.decode('utf-8'))
    len_response = len(response['data']['donors'])
    
    # API requests successful: status_code = 200
    if r.status_code == 200:
        if len_response >= 1:
            if len(response['data']['donors']) == 1:
                donor_name = response['data']['donors'][0]['Donor']
            else:
                candidates = [donor for dic in response['data']['donors'] for donor in dic.values()]
    
    # only fill in the columns if donor_name/candidates exists
    if donor_name: 
        ticker_comnams.loc[i,'donor_name'] = donor_name 
    if candidates:
        ticker_comnams.loc[i, 'candidates'] = ", ".join(candidates)
    
    # export to ticker_comnams_donors.csv
    ticker_comnams.to_csv("ticker_comnams_donors.csv", encoding='utf-8', index=False)
