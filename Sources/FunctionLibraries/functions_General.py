import re
import csv
import sys
import os
import pandas as pd

global global_dic
global_dic = {}
global functions_pool
functions_pool = {"tolower":"","toupper":"","totitle":"","trim":"","chomp":"","substring":"","replaceValue":"",
                "concat2":"","concat3":"","concat4":"","concat5":"","concat6":"","match":"","replaceRegex":"","split":""}

## row: the current record of the datasource that is being provided to the function including all the columns/attributes
## dic["func_par"]: is a dictionary itself; the keys are the name of the input parameter and the value of each key is the column/attribute of the datasource which is the input value of the input parameter(key)
## header: is a list of the column names in the RDB that is provided as the input data source of the function 

# returns a string in lower case
def tolower():
    ## The definition of function to be executed over a csv file: 
    return global_dic["value"].lower()

# return a string in upper case
def toupper():
    return  global_dic["value"].upper()


# return a string in title case
def totitle():
    return global_dic["value"].title()


# return a string after removing leading and trailing whitespaces
def trim():
    return global_dic["value"].strip()


# return a string without s2
def chomp():
    return global_dic["value"].replace(global_dic["toremove"], '')


#return the substring (index2 can be null, index2 can be negative value)
def substring():
    value = global_dic["value"]
    if "index2" is global_dic:
        return value[int(global_dic["index1"]):]
    else:
        return value[int(global_dic["index1"]):int(global_dic["index2"])]


#replace value2 by value3
def replaceValue():
    value = global_dic["value"]
    return str(value).replace(global_dic["value2"], global_dic["value3"])


#returns the first appearance of the regex in value
def match():
    value = global_dic["value"]
    return re.match(global_dic["regex"], value)[0]


def concat2():
    if bool(global_dic["value1"]) and bool(global_dic["value2"]):
        result = str(str(global_dic["value1"])+str(global_dic["value2"]))
    else:
        result = ""  
    return(result)

def concat3():
    if bool(global_dic["value1"]) and bool(global_dic["value2"]) and bool(global_dic["value3"]):
        result = str(str(global_dic["value1"])+str(global_dic["value2"])+str(global_dic["value3"]))
    else:
        result = ""  
    return(result)

def concat4():
    if bool(global_dic["value1"]) and bool(global_dic["value2"]) and bool(global_dic["value3"]) and bool(global_dic["value4"]):
        result = str(str(global_dic["value1"])+str(global_dic["value2"])+str(global_dic["value3"])+str(global_dic["value4"]))
    else:
        result = ""  
    return(result)

def concat5():
    if bool(global_dic["value1"]) and bool(global_dic["value2"]) and bool(global_dic["value3"]) and\
       bool(global_dic["value4"]) and bool(global_dic["value5"]):
        result = str(str(global_dic["value1"])+str(global_dic["value2"])+str(global_dic["value3"])+\
                 str(global_dic["value4"])+str(global_dic["value5"]))
    else:
        result = ""  
    return(result)

def concat6():
    if bool(global_dic["value1"]) and bool(global_dic["value2"]) and bool(global_dic["value3"]) and\
       bool(global_dic["value4"]) and bool(global_dic["value5"]) and bool(global_dic["value6"]):
        result = str(str(global_dic["value1"])+str(global_dic["value2"])+str(global_dic["value3"])+\
                 str(global_dic["value4"])+str(global_dic["value5"])+str(global_dic["value6"]))
    else:
        result = "" 
    return(result)

# returns the regex match with the replvalue in the column
def replaceRegex():
    return re.sub(global_dic["regex"],str(global_dic["replvalue"]),str(global_dic["value"]))

# returns the index-th string obtained by splitting the string of the column at the first aprearance of the separator
def split():
    return str(global_dic["column"]).split(global_dic["separator"])[int(global_dic["index"])]


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