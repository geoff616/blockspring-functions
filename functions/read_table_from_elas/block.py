###### Read Table From Elasticsearch
###### This function takes an elasticsearch querystring query and returns a table of the results
######
###### Arguments:
######     address: Location and port of the Elasticsearch instance - 127.0.0.1:9300
######     index: The Elasticsearch index(es) to read from - ecommerce_store_data or 2015-01-01,2015-01-02
######     size: The number of rows to return to the table - 1000
######     query_string: The Elasticsearch query that will return the desired documents - _type:sales_by_day AND store_location:(10001 10002)
######     fields: The comma seperated list of fields to return, if blank will return all 

import blockspring
from datetime import datetime
import requests
from json import dumps, loads
import time


## Helper function that takes a list of fields like 'cat, dog, frog' and returns ', "fields":["cat", "dog", "frog"]' so ES will be happy
## TODO: Confirm the quotes in str(field_array) are espcaing properly!
def build_fields(fields):
    #if the value was entered, build the fancy thing to add to the es_query
    if fields:
        field_array = fields.split(', ')
        to_return = ', "fields": ' + str(field_array) 
    #no fields
    else:
        to_return = ''
    return to_return



# Main function registered to blockspring that does the magic
def read_table_from_es(request, response):  

    ## Where is ES?
    address = request.params['address']
    index = request.params['index']
    ## Params for ES query
    size = request.params['size']
    query_string = request.params['query_string']
    fields = request.params['fields']    

    ## Define the ElasticSearch Query

    es_query = """{
      "size": %s
      "query": {
        "query_string": {
          "query": "%s"
        }
      }%s
    }""" % (size, query_string, fields)


    ## Build the URL to query
    url = address + '/' + index + '/' + datatype + '/'
    json_query = dumps(es_query)
    req = requests.post(url, json_query)

    results = []
    if req.ok == True:
        query_response = loads(req.text)
        #check if there were results
        if 'hits' in query_response:
            if 'hits' in query_response['hits']:
                #things are about to get messy
                #TODO: Clean this up!!!!
                es_internal_stuff = ["_index", "_type", "_id", "_score"]
                fields_in_docs = []
                for result in query_response['hits']['hits']:
                    #Logic to determine column headers
                    #Note: this is tricky because there isn't a guarantee that all docs will have the same headers!
                    #Naive way is to loop through the results twice... Maybe use pandas?
                    result_keys = result.keys()
                    field_set = set(fields_in_docs)
                    for key in result_keys:
                        if key not in field_set:
                            fields_in_docs.append(key)

                columns = es_internal_stuff + fields_in_docs
                results.append(columns)
                for result in query_response['hits']['hits']:
                    row = []
                    for key in columns:
                        if key in set(es_internal_stuff):
                            row.append(result[key])
                        else:
                            if key in result['_source']:
                                row.append(result['_source'][key])
                            else:
                                row.append(0)
                    results.append(row)
            else:
                results.append(['no hits'])
        else:
            results.append(['no hits'])
    else:
        results.append(['ERROR', req.reason])

    ##Add array of statuses to response 
    response.addOutput("results", results)

    ## Return the response
    response.end()

## Defining write_to_es function for blockspring
blockspring.define(read_table_from_es)