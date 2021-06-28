import re
import csv
import sys
import os
import pandas as pd

global global_dic
global_dic = {}
global functions_pool
functions_pool = {"tolower":"","toupper":"","totitle":"","trim":"","chomp":"","substring":"","replaceValue":"","variantIdentifier":"",
                "concat2":"","concat3":"","concat4":"","concat5":"","concat6":"","match_gdna":"","match_aa":"",
                "match_exon":"","rearrange_cds":"","match_pFormat":"","match":"","replaceRegex":"","split":""}

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


def variantIdentifier():
    value = ""
    column1 = global_dic["column1"]
    column2 = global_dic["column2"]
    if (str(column1) != "nan"):
        value = re.sub('_.*','',str(column2))+"_"+str(column1).replace("c.","").replace(">", "~")
        value = global_dic["prefix"]+value
    return value

################################################################################################################
############################################## new functions for GenoKGC #######################################
################################################################################################################

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

def match_gdna():
    combinedValue = global_dic["combinedValue"]
    if bool(combinedValue):
        expressionsList = combinedValue.split(":")
        gdna = ""
        for j in range(0,len(expressionsList)):
            if "g." in expressionsList[j]:
                gdna = expressionsList[j]
    else:
        gdna = ""
    return(gdna)

def match_cdna():
    combinedValue = global_dic["combinedValue"]
    if bool(combinedValue):
        expressionsList = combinedValue.split(":")
        cdna = ""
        for j in range(0,len(expressionsList)):
            if "c." in expressionsList[j]: 
                cdna = expressionsList[j] 
    else:
        cdna = ""                
    return(cdna)

def match_aa():
    combinedValue = global_dic["combinedValue"]
    if bool(combinedValue):
        expressionsList = combinedValue.split(":")
        aa = ""
        for j in range(0,len(expressionsList)):
            if "p." in expressionsList[j]:
                aa = expressionsList[j]
    else:
        aa = ""             
    return (aa)  

def match_exon():
    combinedValue = global_dic["func_par"]["combinedValue"]
    if bool(combinedValue):
        expressionsList = combinedValue.split(":")
        exon = ""
        for j in range(0,len(expressionsList)):
            if "exon" in expressionsList[j]:  
                exon = expressionsList[j]  
    else:
        exon = ""                
    return(exon)
           
def match_pFormat():    
    threeLetters = global_dic["threeLetters"]
    gene = global_dic["gene"]       
    aminoAcidsDic = {
    "ala":"A", "arg":"R", "asn":"N", "asp":"D", "asx":"B", "cys":"C", "glu":"E", "gln":"Q", "glx":"Z", "gly":"G", "his":"H", "ile":"I", "leu":"L", "lys":"K", 
    "met":"M", "phe":"F", "pro":"P", "ser":"S", "thr":"T", "trp":"W", "tyr":"Y", "val":"V"}               
    if threeLetters != "":
        first = threeLetters.split(".")[1][0:3]
        second = threeLetters.split(".")[1][-3:]
        middle = threeLetters.split(".")[1][3:-3]
        if first in aminoAcidsDic.keys() and second in aminoAcidsDic.keys():
            pFormat = gene + "~p." + aminoAcidsDic[first] + middle + aminoAcidsDic[second]
        else:
            pFormat = ""
    else:
        pFormat = ""    
    return(pFormat)

def rearrange_cds():
    cds = global_dic["cds"]
    if bool(cds):
        if "del" not in cds and "ins" not in cds:
            if not cds.split(".")[1][0].isdigit():
                firstN = cds.split(".")[1][0]
                gp = cds.split(".")[1][1:-1]
                secondN = cds.split(".")[1][-1]  
                new_cds = gp + firstN + "~" + secondN                                                          
            else:
                new_cds = cds.split(".")[1][:-1] + "~" + cds.split(".")[1][-1]
        else:
            new_cds = cds.split(".")[1]    
    else:
        new_cds = ""                
    return(new_cds)


# returns the regex match with the replvalue in the column
def replaceRegex():
    return re.sub(global_dic["regex"],str(global_dic["replvalue"]),str(global_dic["value"]))

# returns the index-th string obtained by splitting the string of the column at the first aprearance of the separator
def split():
    return str(global_dic["column"]).split(global_dic["separator"])[int(global_dic["index"])]

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
        for value in dic["func_par"]:
            if dic["func_par"][value] in inputs:
                if "constant" not in inputs: 
                    if isinstance(row,dict):
                        output[value] = row[inputs[0]]
                    elif isinstance(global_row,list):
                        output[value] = row[header.index(global_dic["func_par"][value])]
                else:
                    output[value] = inputs[0]
    return output
