import re
import csv
import sys
import os
import unidecode
from .string_subs import *
################################################################################################
############################ Static (Do NOT change this code) ##################################
################################################################################################

import pandas as pd
import requests
import unidecode

global global_dic
global_dic = {}
global functions_pool
global semantic_dict
semantic_dict = dict()

#####################################################################################################
########### ADD THE IMPLEMENTATION OF YOUR FUNCTIONS HERE FOLLOWING THE EXAMPLES ####################
#####################################################################################################

functions_pool = {"tolower":"","replaceValue":"","concat2":"","falcon_UMLS_CUI_function":""}


## Define your functions here following examples below, the column "names" from the csv files 
## that you aim to use as the input parameters of functions are only required to be provided 
## as the keys of "global_dic"
def tolower(): 
    return str(global_dic["value"]).lower()

#replace value2 by value3
def replaceValue():
    value = unidecode.unidecode(str(global_dic["value"]))
    return value.replace(global_dic["value2"], global_dic["value3"])

def concat2():
    value1 = global_dic["value1"]
    value2 = global_dic["value2"]
    #print(value1 + " " + value2)
    if bool(value1) and bool(value2):
        result = str(str(value1)+str(value2))
    else:
        result = ""  
    return(result)




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