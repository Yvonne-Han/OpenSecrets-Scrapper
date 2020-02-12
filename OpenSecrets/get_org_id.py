import re
import requests
import xmltodict
import pandas as pd
from bs4 import BeautifulSoup

api_key_list = ['62c49e1642d9142073baa7a32d066e1e', '17a3e5111c89debb2469a473d0cc1dad','7c4252bc145851b63185f71a96e7623a']

# take a string as input and output the keywords of comnam
def clean_comnam(comnam):
    suffixes = r"\s(\bco\b|\bcorp\b|\bcorporation\b|\bltd\b|\binc\b|\bdel\b\s{0}|\bnew\b\s{0}|\b&\b)"
    comnam_no_suffixes = re.sub(suffixes, "", comnam.lower())
    single_letter = re.compile(r'\b([a-z]) (?=[a-z]\b)', re.I)
    comnam_compiled = re.sub(single_letter, r'\g<1>', comnam_no_suffixes)
    return re.sub("\s", "%20", comnam_compiled)

# for a given company name, return possible matches of corporation ids on OpenSecrets
def get_org_id(some_text, api_key):
    api_query = 'https://www.opensecrets.org/api/?method=getOrgs&org=' + some_text + '&apikey=' + api_key
    api_response = requests.get(api_query)
    
    if api_response.status_code == 200: 
        api_response_dict = xmltodict.parse(api_response.content)
        
        if isinstance(api_response_dict['response']['organization'], list):
            return '; '.join((', '.join(item.values())) for item in api_response_dict['response']['organization'])
        
        else:
            return api_response_dict['response']['organization']['@orgid']
    
    else:
        return None
    
# sample call: starbucks
# get_org_id("starbucks")

# create a new dataframe with org_id and org_id_candidates as blank
ticker_comnam_id = pd.read_csv("ticker_comname.csv")
ticker_comnam_id['org_id'] = None
ticker_comnam_id['org_id_candidates'] = None

# First attempt: use the normal clean_comnam as search keywords
# Found ids for 368/498 comnams

for i in range(len(ticker_comnam_id)):
    donor_name = ""
    candidates = []
    
    # deal with the max 200 calls for each api_key
    api_key_no = i//195
    api_key = api_key_list[api_key_no]
    
    comnam = clean_comnam(ticker_comnam_id['comnam'][i])
    org_id_result = get_org_id(comnam, api_key)

    if org_id_result:
        if ',' in org_id_result:
            ticker_comnam_id.loc[i,'org_id_candidates'] = org_id_result
        else:
            ticker_comnam_id.loc[i,'org_id'] = org_id_result
    
    print(i, org_id_result)

ticker_comnam_id.to_csv("ticker_comnam_id.csv", encoding='utf-8', index=False)

# hand-collected for very short comnams: 394/498 matches (79%)

# Second attempt: use the first word in the comnam to get a candidate list of possible matches
# To allow for manual judgment, I put all results in candidates no matter how many are returned for each query
# Found 380/398 ids
ticker_comnam_id_second = pd.read_csv("ticker_comnam_id.csv")

for i in range(len(ticker_comnam_id_second)):
    donor_name = ""
    candidates = []
    
    # set your own key for this
    api_key = '7c4252bc145851b63185f71a96e7623a'
    
    # only deal with those with blanks for both org_id and org_id_candidates
    if not isinstance(ticker_comnam_id_second.loc[i, 'org_id_candidates'], str) and not isinstance(ticker_comnam_id_second.loc[i, 'org_id'], str):
        comnam = clean_comnam(ticker_comnam_id_second['comnam'][i].split(" ")[0])
        print(i, comnam)

        org_id_result = get_org_id(comnam, api_key)

        if org_id_result:
            if ';' in org_id_result:
                ticker_comnam_id_second.loc[i,'org_id_candidates'] = org_id_result
            else:
                ticker_comnam_id_second.loc[i,'org_id'] = org_id_result

ticker_comnam_id_second.to_csv("ticker_comnam_id.csv", encoding='utf-8', index=False)
