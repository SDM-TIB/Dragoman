import re
import csv
import sys
import os
from .string_subs import *
import pandas as pd
from pathlib import Path
import requests
import json

global global_dic
global_dic = {}
global functions_pool
global exactMatchDic
exactMatchDic = dict()

#####################################################################################################
########### ADD THE IMPLEMENTATION OF YOUR FUNCTIONS HERE FOLLOWING THE EXAMPLES ####################
#####################################################################################################

## For each new function that you define, add an entry as "function_name":"" to the dictionary below 
functions_pool = {"reverseString":"","toLower":"","replaceExactMatch":"","falcon_UMLS_CUI_function":""}


### Non-injective, surjective 
def toLower(): 
    return global_dic["value"].lower()

### Bijective
def dictionaryCreation():
    directory = Path(os.path.abspath(os.path.join(os.getcwd(), os.path.dirname(__file__)))).parent.absolute()
    with open(str(directory)+"/Sources/label_cui_dictionary.csv",'r') as data:
        for row in csv.DictReader(data):
            exactMatchDic.update({row['SampleOriginLabel']:row['CUI']}) 
dictionaryCreation()

def replaceExactMatch():    
    value = global_dic["value"]                   
    if value != "":
        replacedValue = exactMatchDic[value]
    else:
        replacedValue = "" 
    return(replacedValue)

def reverseString():    
    value = str(global_dic["value"])
    if value != "":
        output = value[::-1]
    else:
        output = ""
    return(output) 


### non-injective, non-surjective
headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
def falcon_UMLS_CUI_function():
    value = global_dic["value"]
    output = ""
    url = 'https://labs.tib.eu/sdm/biofalcon/api?mode=short'
    text = str(value).replace("_"," ")
    payload = '{"text":"'+text+'"}'
    r = requests.post(url, data=payload.encode('utf-8'), headers=headers)
    if r.status_code == 200:
        response=r.json()
        if len(response['entities'][1])>0:
            return response['entities'][1][0]
        else:
            return ""
    else:
        return ""


################################################################################################
############################ Static (Do NOT change this code) ##################################
################################################################################################

def execute_function(row,header,dic):
    if "#" in dic["function"]:
        func = dic["function"].split("#")[1]
    else:
        func = dic["function"].split("/")[len(dic["function"].split("/"))-1]
    #print(dic)
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
            if "reference" in inputs[1]:
                if isinstance(row,dict):
                    output[inputs[2]] = row[inputs[0]]
                else:
                    output[inputs[2]] = row[header.index(inputs[0])]
            elif "template" in inputs:
                if isinstance(row,dict):
                    output[inputs[2]] = string_substitution(inputs[0], "{(.+?)}", row, "subject", "yes", "None")
                else:
                    output[inputs[2]] = string_substitution_array(inputs[0], "{(.+?)}", row, header, "subject", "yes")
        else:
            output[inputs[2]] = inputs[0]
    return output
