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
    to_return = []
    for date in results.keys():
        to_return.append([date, str(results[date])])


    response.addOutput("results", to_return)

    ## Return the response
    response.end()

## Defining write_to_es function for blockspring
blockspring.define(get_13f_data)