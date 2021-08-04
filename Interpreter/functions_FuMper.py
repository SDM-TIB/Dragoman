import re
import csv
import sys
import os
import pandas as pd

global global_dic
global_dic = {}
global functions_pool

#####################################################################################################
########### ADD THE IMPLEMENTATION OF YOUR FUNCTIONS HERE FOLLOWING THE EXAMPLES ####################
#####################################################################################################

## For each new function that you define, add an entry as "function_name":"" to the dictionary below 
functions_pool = {"tolower":"","exact_match":"","falcon_UMLS_CUI_function":""}


### Non-injective, surjective 
def toLower(): 
    return global_dic["value"].lower()

### bijective
def exactMatch():    
    value = global_dic["value"]      
    exactMatchDic = {1:"A", 2:"R", 3:"N", 4:"D", 5:"B", 6:"C", 7:"E", 8:"Q", 9:"Z", 10:"G"}               
    if value != "":
        replacedValue = exactMatchDic[value]
    else:
        replacedValue = "" 
    return(replacedValue)

### non-injective, non-surjective
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
        return response['cui']
    else:
        return ""


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
