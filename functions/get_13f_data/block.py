###### Get 13F Data
###### This function returns data from the jivedata detail API for 13f filings 
###### from the endpoint: http://api.jivedata.com/13F/detail/?cik={{cik}}
###### What does thsi data look like? More info?
###### 
######  
###### Arguments:
######     cik: fund identifier //string


import blockspring
import requests
from json import dumps, loads

## Helper function to 
def query_jive(cik):
    url = 'https://api.jivedata.com/13F/detail/?cik=' + cik
    get = requests.get(url, verify=False)
    if get.status_code == requests.codes.ok:
        return get.text
    else:
        return "Error of some sort"

## Main function registered to blockspring 
def get_13f_data(request, response):  

    ## pass cik to query function
    cik = request.params['cik']
    query = query_jive(cik)

    results = loads(query)['_results_']

    ## need to turn a nested JSON into array of arrays 
    first_key = 'date'

    ## interesting data is in _holdings_ which is an array of dicts with the keys listed
    inner_keys = {
        '_cover_': ['city', 'zip', 'street1', 'street2', 'state', 'manager'], 
        '_signature_': ['city', 'state', 'title', 'name', 'phone'], 
        '_holdings_': ['cusip', 'name', 'ticker', 'shares', 'value', 'type', 'class'], 
        '_summary_': ['total_value', 'others', 'entries'] 
    }

    columns = [first_key] + inner_keys['_holdings_']
    
    #start building return array with column headers as first row
    to_return = [columns]

    #add a row for each holding
    for date in results.keys():
        holdings = results[date]['_holdings_']
        for holding in holdings:
            row = []
            row.append(date)
            #add date to first column in row
            keys_to_check = columns[1:]
            for key in keys_to_check:
                if key in holding:
                    row.append(holding[key])
                else:
                    row.append(0)

            to_return.append(row)


    response.addOutput("results", to_return)

    ## Return the response
    response.end()

## Defining write_to_es function for blockspring
blockspring.define(get_13f_data)