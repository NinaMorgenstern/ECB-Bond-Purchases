import pandas as pd
import glob
import requests
import json
from time import sleep



# Function to load all files with the same name pattern

def pd_read_csv(pattern, encoding):
    """
    This function reads several .csv files with the same name pattern and extracts the month of the report from the filename. 
    """
    files = glob.glob(pattern)
    df = pd.concat((pd.read_csv(f, encoding = encoding).\
                    #column with the month of the report
                    assign(MONTH = f[len(f)-12:-8] + '/' + f[len(f)-8:-6]) for f in files),\
                   ignore_index=True)
    return df



# Functions for Record matching API, Entity Search API and Entity Lookup API

def record_matching(token,template,url='https://api-eit.refinitiv.com/permid/match',content='text/plain',data='Organization'):
    """
    This function implements PermID Record Matching API requests.
    """
    parameters = {'x-ag-access-token' : token,
                  'Content-Type' : content,
                  'x-openmatch-dataType' : data} 
    matched = False
    Ntries = 0
    while not matched and Ntries < 3:
        Ntries += 1
        sleep(2)
        r = requests.post(url, headers = parameters, data = template).json()
        if r['errorCodeMessage'] == 'Success':
            if r['numReceivedRecords'] != r['unMatched']:
                matched = True
                print('Processed: ' + str(r['numProcessedRecords']) +
                      '\nMatched: \n  Total ' + str(r['matched']['total']) +
                      '\n  Excellent ' + str(r['matched']['excellent']) +
                      '\n  Good ' + str(r['matched']['good']) +
                      '\n  Possible ' + str(r['matched']['possible']) +
                      '\nUnmatched: ' + str(r['unMatched']))
            elif Ntries < 3:
                print('No matches found. Trying again...')
            else:
                print('No matches found. Check the template or run the cell again.')
    return r
    
def entity_search(token,q,url='https://api-eit.refinitiv.com/permid/search',entity='organization'):
    """
    This function implements PermID Entity Search API requests.
    """
    parameters = {'access-token': token,
                  'entitytype': entity,
                  'q' : q}
    r = requests.get(url, params = parameters).json()
    return r
    
def entity_lookup(token,url,resp='json-ld'):
    """
    This function implements PermID Entity Lookup API requests.
    """
    parameters =  {'access-token': token,
                   'format' : resp}
    r = requests.get(url, params = parameters).json()
    return r

