import pandas as pd
import requests
import json
from time import sleep

# Functions for Record matching API, Entity Search API and Entity Lookup API
# Documentation on APIs:
# https://developers.refinitiv.com/content/dam/devportal/api-families/open-permid/permid-entity-search/documentation/permid-apis-user-guide-apr-2020.pdf

def record_matching(token,url,template,content='text/plain',data='Organization'):
    """
    This function implements PermID Record Matching API requests.
    """
    headers = {'x-ag-access-token' : token,
               'Content-Type' : content,
               'x-openmatch-dataType' : data}
    with requests.post(url, headers = headers, data = template) as request:
        r = request.json()
        print('Processed: ' + str(r['numProcessedRecords']) +
              '\nMatched: \n  Total ' + str(r['matched']['total']) +
              '\n  Excellent ' + str(r['matched']['excellent']) +
              '\n  Good ' + str(r['matched']['good']) +
              '\n  Possible ' + str(r['matched']['possible']) +
              '\nUnmatched: ' + str(r['unMatched']))
        r = pd.DataFrame([d for d in r['outputContentResponse']])[['Match OpenPermID','Input_Name']]
        return r
    
def entity_search(token,url,q,entity='organization',num=5):
    """
    This function implements PermID Entity Search API requests.
    """
    parameters = {'access-token': token,
                  'entitytype': entity,
                  'num': num,
                  'q': q}
    with requests.get(url, params = parameters) as request:
        r = request.json()['result']['organizations']['entities']
        return r
    
def entity_lookup(token,url,resp='json-ld'):
    """
    This function implements PermID Entity Lookup API requests.
    """
    parameters =  {'access-token': token,
                   'format' : resp}
    with requests.get(url, params = parameters) as request:
        r = request.json()
        return r

