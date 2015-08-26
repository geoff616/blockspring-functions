###### Write Table To Elasticsearch
###### This function takes a rectangular selection with a column header and will index each row as a document in elasticsearch
######
###### Arguments:
######     address: Location and port of the Elasticsearch instance - 127.0.0.1:9300
######     index: The Elasticsearch Index to write to - ecommerce_store_data
######     type: The type of document you are indexing - monthly revenue by product category
######     documets: Column header plus the rows to index - A1:J4

## Test comment 

import blockspring
from datetime import datetime
import requests
from json import dumps
import time

## Helper funtion that takes a string like "This one" and returns "this_one"
def to_es_friendly_string(string):
    lower_case = string.lower()
    no_spaces = lower_case.replace(' ', '_')
    return no_spaces


# Main function registered to blockspring that does the magic
def write_table_to_es(request, response):  

    ## Where is ES?
    address = request.params['address']

    ## Clean up index and doc type to use for writes
    index = to_es_friendly_string(request.params['index'])
    datatype = to_es_friendly_string(request.params['type'])

    ## An array of arrays where the first element in the outer array is the column headers: [["Product Type", "Sales"], ["Red Shoes", 87], ...]
    rows = request.params['documents']

    ## Get the set of properties to use for each document and remove that row from the rows array
    columns = rows.pop(0)
    properties = [to_es_friendly_string(prop) for prop in columns]

    ## Array to track if writes succeeded
    ## TODO: Better error handling
    write_status = [[]]



    ## Index each row as a document
    for row in rows:
        ## Create a dictionary with the clean labels to index
        doc = dict(zip(properties, row))

        ## Adding timestamp to the document
        doc['indexed_at'] = str(int(datetime.now().strftime("%s")))

        ## Build the URL and index the dictionary as a JSON document in elasticsearch
        url = address + '/' + index + '/' + datatype + '/'
        json_doc = dumps(doc)
        req = requests.post(url, json_doc)
        print req.ok

        ## Keep track of which writes were sucessful 

        if req.ok == True:
             write_status[0].append('True')
        else:
            write_status[0].append('ERROR')

        ##Add array of statuses to response 
        response.addOutput("Indexed", write_status)

    ## Return the response
    response.end()

## Defining write_to_es function for blockspring
blockspring.define(write_table_to_es)