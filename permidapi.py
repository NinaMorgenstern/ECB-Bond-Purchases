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
    # Sometimes the request is processed successfully, but does not return any matches due to unexpected server errors
    # If there are no matches, the request is sent again, 3 times in total
    r = ''
    matched = False
    Ntries = 0
    while not matched and Ntries < 3:
        Ntries += 1
        try:
            with requests.post(url, headers = headers, data = template) as request:
                r = request.json()
                if r['errorCodeMessage'] == 'Success':
                    if r['numReceivedRecords'] != r['unMatched']:
                        matched = True
                        print('Processed: ' + str(r['numProcessedRecords']) +
                              '\nMatched: \n  Total ' + str(r['matched']['total']) +
                              '\n  Excellent ' + str(r['matched']['excellent']) +
                              '\n  Good ' + str(r['matched']['good']) +
                              '\n  Possible ' + str(r['matched']['possible']) +
                              '\nUnmatched: ' + str(r['unMatched']))
                        r = pd.DataFrame([d for d in r['outputContentResponse']])[['Match OpenPermID','Input_Name']]
                    elif Ntries < 3:
                        print('No matches found. Trying again...')
                        sleep(5)
                    else:
                        print('No matches found. Check the template or run the cell again.')
        except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError) as err:
            print('Exception occurred. Suspending and trying again...')
            sleep(30)
            continue
    return r
    
    
def entity_search(token,url,q,entity='organization',num=5):
    """
    This function implements PermID Entity Search API requests.
    """
    parameters = {'access-token': token,
                  'entitytype': entity,
                  'num': num,
                  'q': q}
    r = ''
    Ntries = 0
    while r == '' and Ntries < 3:
        Ntries += 1
        try:
            sleep(2)
            with requests.get(url, params = parameters) as request:
                r = request.json()['result']['organizations']
        except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError) as err:
            print('Exception occurred. Suspending and trying again...')
            sleep(100)
            continue
    return r

    
def entity_lookup(token,url,resp='json-ld'):
    """
    This function implements PermID Entity Lookup API requests.
    """
    parameters =  {'access-token': token,
                   'format' : resp}
    r = ''
    Ntries = 0
    while r == '' and Ntries < 10:
        Ntries += 1
        try:
            with requests.get(url, params = parameters) as request:
                r = request.json()
        except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError) as err:
            print('Exception occurred. Suspending and trying again...')
            sleep(30)
            continue
    return r

