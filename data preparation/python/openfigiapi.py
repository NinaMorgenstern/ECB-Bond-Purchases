import pandas as pd
import requests
import json
import math

# Function for OpenFIGI API
# Documentation on API:
# https://www.openfigi.com/api

def map_ISIN(key,url,id_list,content='text/json'):
    """
    This function implements OpenFIGI API requests.
    """
    headers = {'X-OPENFIGI-APIKEY' : key,
               'Content-Type' : content}
    # Max 100 jobs per request
    df = []
    for i in range(math.ceil(len(id_list)/100)):
        data=''
        for code in id_list[i*100:i*100+100]:
            data = data+'{"idType":"ID_ISIN","idValue":"'+code+'"},'
        with requests.post(url, headers = headers, data = '['+data[:-1]+']') as request:
            r = request.json()
            for d in r:
                df.append(pd.DataFrame(d['data']))
    df = pd.concat(df, ignore_index=True)['name']
    return(df)
