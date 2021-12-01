import re
import csv
import sys
import os
import pandas as pd
import requests

global global_dic
global_dic = {}
global functions_pool

#####################################################################################################
########### ADD THE IMPLEMENTATION OF YOUR FUNCTIONS HERE FOLLOWING THE EXAMPLES ####################
#####################################################################################################

## For each new function that you define, add an entry as "function_name":"" to the dictionary below 
functions_pool = {"tolower":"","chomp":"","concat2":"","falcon_UMLS_CUI_function":""}


## Define your functions here following examples below, the column "names" from the csv files 
## that you aim to use as the input parameters of functions are only required to be provided 
## as the keys of "global_dic"
def tolower(): 
    return global_dic["value"].lower()

def chomp():
    return global_dic["value"].replace(global_dic["toremove"], '')



headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}

def falcon_UMLS_CUI_function():
    value = global_dic["value"]
    output = ""
    url = 'http://node1.research.tib.eu:9002/umlsmatching?type=cui'
    text = str(value).replace("_"," ")
    payload = '{"data":"'+text+'"}'
    r = requests.post(url, data=payload.encode('utf-8'), headers=headers)
    if r.status_code == 200:
        response=r.json()
        if response['cui'] != "":
            return response['cui'][0]
        else:
            return ""
    else:
        return ""

def concat2():
    value1 = global_dic["value1"]
    value2 = global_dic["value2"]
    if bool(value1) and bool(value2):
        result = str(str(value1)+str(value2))
    else:
        result = ""  
    return(result)



################################################################################################
############################ Static (Do NOT change this code) ##################################
################################################################################################

def execute_function(row,header,dic):
    func = dic["function"].split("/")[len(dic["function"].split("/"))-1]
    if func in functions_pool:
        global global_dic
        global_dic = execution_dic(row,header,dic)
        return eval(func + "()")             
    else:
        print("Invalid function")
        print("Aborting...")
        sys.exit(1)

def execution_dic(row,header,dic):
    output = {}
    for inputs in dic["inputs"]:
        if "constant" not in inputs: 
            if isinstance(row,dict):
                output[inputs[2]] = row[inputs[0]]
            elif isinstance(global_row,list):
                output[inputs[2]] = row[header.index(global_dic["func_par"][inputs[2]])]
        else:
            output[inputs[2]] = inputs[0]
    return output
