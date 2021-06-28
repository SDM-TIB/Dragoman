import re
import csv
import sys
import os
import pandas as pd

global global_row
global_row = []
global global_header
global_header = []
global global_dic
global_dic = {}

## row: the current record of the datasource that is being provided to the function including all the columns/attributes
## dic["func_par"]: is a dictionary itself; the keys are the name of the input parameter and the value of each key is the column/attribute of the datasource which is the input value of the input parameter(key)
## header: is a list of the column names in the RDB that is provided as the input data source of the function 

# returns a string in lower case
def tolower():
    ## The definition of function to be executed over a csv file: 
    if isinstance(global_row,dict): ## 
        return global_row[global_dic["func_par"]["value"]].lower()
    ## The definition of function to be executed over a RDB file:
    elif isinstance(global_row,list): 
        return global_row[global_header.index(global_dic["func_par"]["value"])].lower()

# return a string in upper case
def toupper():
    if isinstance(global_row,dict):
        return global_row[global_dic["func_par"]["value"]].upper()
    elif isinstance(global_row,list): 
        return global_row[global_header.index(global_dic["func_par"]["value"])].upper()


# return a string in title case
def totitle():
    if isinstance(global_row,dict):
        return global_row[global_dic["func_par"]["value"]].title()
    elif isinstance(global_row,list): 
        return global_row[global_header.index(global_dic["func_par"]["value"])].title()


# return a string after removing leading and trailing whitespaces
def trim():
    if isinstance(global_row,dict):
        return global_row[global_dic["func_par"]["value"]].strip()
    elif isinstance(global_row,list): 
        return global_row[global_header.index(global_dic["func_par"]["value"])].strip()


# return a string without s2
def chomp():
    if isinstance(global_row,dict):
        return global_row[global_dic["func_par"]["value"]].replace(global_dic["func_par"]["toremove"], '')
    elif isinstance(global_row,list): 
        return global_row[global_header.index(global_dic["func_par"]["value"])].replace(global_dic["func_par"]["toremove"], '')


#return the substring (index2 can be null, index2 can be negative value)
def substring():
    if isinstance(global_row,dict):
        value = global_row[global_dic["func_par"]["value"]]
    elif isinstance(global_row,list):
        value = global_row[global_header.index(global_dic["func_par"]["value"])]
    if "index2" is global_dic["func_par"]:
        return value[int(global_dic["func_par"]["index1"]):]
    else:
        return value[int(global_dic["func_par"]["index1"]):int(global_dic["func_par"]["index2"])]


#replace value2 by value3
def replaceValue():
    if isinstance(global_row,dict):
        value = global_row[global_dic["func_par"]["value"]]
    elif isinstance(global_row,list):
        value = global_row[global_header.index(global_dic["func_par"]["value"])]
    return str(value).replace(global_dic["func_par"]["value2"], global_dic["func_par"]["value3"])


#returns the first appearance of the regex in value
def match():
    if isinstance(global_row,dict):
        value = global_row[global_dic["func_par"]["value"]]
    elif isinstance(global_row,list):
        value = global_row[global_header.index(global_dic["func_par"]["value"])]
    return re.match(global_dic["func_par"]["regex"], value)[0]


def variantIdentifier():
    value = ""
    if isinstance(global_row,dict):
        column1 = global_row[global_dic["func_par"]["column1"]]
        column2 = global_row[global_dic["func_par"]["column2"]]
    elif isinstance(global_row,list):
        column1 = global_row[global_header.index(global_dic["func_par"]["column1"])]
        column2 = global_row[global_header.index(global_dic["func_par"]["column2"])]
    if (str(column1) != "nan"):
        value = re.sub('_.*','',str(column2))+"_"+str(column1).replace("c.","").replace(">", "~")
        value = global_dic["func_par"]["prefix"]+value
    return value

################################################################################################################
############################################## new functions for GenoKGC #######################################
################################################################################################################

def concat2():
    for inputs in global_dic["func_par"]["inputs"]:
        if global_dic["func_par"]["value1"] in inputs:
            if "constant" in inputs:
                value1 = global_dic["func_par"]["value1"]
            else:
                if isinstance(global_row,dict):
                    value1 = global_row[global_dic["func_par"]["value1"]]
                elif isinstance(global_row,list):
                    value1 = global_row[global_header.index(global_dic["func_par"]["value1"])]
        elif global_dic["func_par"]["value2"] in inputs:
            if "constant" in inputs:
                value2 = global_dic["func_par"]["value2"]
            else:
                if isinstance(global_row,dict):
                    value2 = global_row[global_dic["func_par"]["value2"]]
                elif isinstance(global_row,list):
                    value2 = global_row[global_header.index(global_dic["func_par"]["value2"])]
    if bool(value1) and bool(value2):
        result = str(str(value1)+str(value2))
    else:
        result = ""  
    return(result)

def concat3():
    for inputs in global_dic["func_par"]["inputs"]:
        if global_dic["func_par"]["value1"] in inputs:
            if "constant" in inputs:
                value1 = global_dic["func_par"]["value1"]
            else:
                if isinstance(global_row,dict):
                    value1 = global_row[global_dic["func_par"]["value1"]]
                elif isinstance(global_row,list):
                    value1 = global_row[global_header.index(global_dic["func_par"]["value1"])]
        elif global_dic["func_par"]["value2"] in inputs:
            if "constant" in inputs:
                value2 = global_dic["func_par"]["value2"]
            else:
                if isinstance(global_row,dict):
                    value2 = global_row[global_dic["func_par"]["value2"]]
                elif isinstance(global_row,list):
                    value2 = global_row[global_header.index(global_dic["func_par"]["value2"])]
        elif global_dic["func_par"]["value3"] in inputs:
            if "constant" in inputs:
                value3 = global_dic["func_par"]["value3"]
            else:
                if isinstance(global_row,dict):
                    value3 = global_row[global_dic["func_par"]["value3"]]
                elif isinstance(global_row,list):
                    value3 = global_row[global_header.index(global_dic["func_par"]["value3"])]
    if bool(value1) and bool(value2) and bool(value3):
        result = str(str(value1)+str(value2)+str(value3))
    else:
        result = ""  
    return(result)

def concat4():
    for inputs in global_dic["func_par"]["inputs"]:
        if global_dic["func_par"]["value1"] in inputs:
            if "constant" in inputs:
                value1 = global_dic["func_par"]["value1"]
            else:
                if isinstance(row,dict):
                    value1 = global_row[global_dic["func_par"]["value1"]]
                elif isinstance(row,list):
                    value1 = global_row[global_header.index(global_dic["func_par"]["value1"])]
        elif global_dic["func_par"]["value2"] in inputs:
            if "constant" in inputs:
                value2 = global_dic["func_par"]["value2"]
            else:
                if isinstance(global_row,dict):
                    value2 = global_row[global_dic["func_par"]["value2"]]
                elif isinstance(global_row,list):
                    value2 = global_row[global_header.index(global_dic["func_par"]["value2"])]
        elif global_dic["func_par"]["value3"] in inputs:
            if "constant" in inputs:
                value3 = global_dic["func_par"]["value3"]
            else:
                if isinstance(global_row,dict):
                    value3 = global_row[global_dic["func_par"]["value3"]]
                elif isinstance(global_row,list):
                    value3 = global_row[global_header.index(global_dic["func_par"]["value3"])]
        elif global_dic["func_par"]["value4"] in inputs:
            if "constant" in inputs:
                value4 = global_dic["func_par"]["value4"]
            else:
                if isinstance(global_row,dict):
                    value4 = global_row[global_dic["func_par"]["value4"]]
                elif isinstance(global_row,list):
                    value4 = global_row[global_header.index(global_dic["func_par"]["value4"])]
    if bool(value1) and bool(value2) and bool(value3) and bool(value4):
        result = str(str(value1)+str(value2)+str(value3)+str(value4))
    else:
        result = ""  
    return(result)

def concat5():
    for inputs in global_dic["func_par"]["inputs"]:
        if global_dic["func_par"]["value1"] in inputs:
            if "constant" in inputs:
                value1 = global_dic["func_par"]["value1"]
            else:
                if isinstance(global_row,dict):
                    value1 = global_row[dic["func_par"]["value1"]]
                elif isinstance(global_row,list):
                    value1 = global_row[global_header.index(global_dic["func_par"]["value1"])]
        elif global_dic["func_par"]["value2"] in inputs:
            if "constant" in inputs:
                value2 = global_dic["func_par"]["value2"]
            else:
                if isinstance(global_row,dict):
                    value2 = global_row[global_dic["func_par"]["value2"]]
                elif isinstance(global_row,list):
                    value2 = global_row[global_header.index(global_dic["func_par"]["value2"])]
        elif global_dic["func_par"]["value3"] in inputs:
            if "constant" in inputs:
                value3 = global_dic["func_par"]["value3"]
            else:
                if isinstance(global_row,dict):
                    value3 = global_row[global_dic["func_par"]["value3"]]
                elif isinstance(global_row,list):
                    value3 = global_row[global_header.index(global_dic["func_par"]["value3"])]
        elif global_dic["func_par"]["value4"] in inputs:
            if "constant" in inputs:
                value4 = global_dic["func_par"]["value4"]
            else:
                if isinstance(global_row,dict):
                    value4 = global_row[global_dic["func_par"]["value4"]]
                elif isinstance(global_row,list):
                    value4 = global_row[global_header.index(global_dic["func_par"]["value4"])]
        elif global_dic["func_par"]["value5"] in inputs:
            if "constant" in inputs:
                value5 = global_dic["func_par"]["value5"]
            else:
                if isinstance(global_row,dict):
                    value5 = global_row[global_dic["func_par"]["value5"]]
                elif isinstance(global_row,list):
                    value5 = global_row[global_header.index(global_dic["func_par"]["value5"])]
    if bool(value1) and bool(value2) and bool(value3) and\
       bool(value4) and bool(value5):
        result = str(str(value1)+str(value2)+str(value3)+\
                 str(value4)+str(value5))
    else:
        result = ""  
    return(result)

def concat6():
    for inputs in global_dic["func_par"]["inputs"]:
        if global_dic["func_par"]["value1"] in inputs:
            if "constant" in inputs:
                value1 = global_dic["func_par"]["value1"]
            else:
                if isinstance(global_row,dict):
                    value1 = global_row[global_dic["func_par"]["value1"]]
                elif isinstance(global_row,list):
                    value1 = global_row[global_header.index(global_dic["func_par"]["value1"])]
        elif global_dic["func_par"]["value2"] in inputs:
            if "constant" in inputs:
                value2 = global_dic["func_par"]["value2"]
            else:
                if isinstance(global_row,dict):
                    value2 = global_row[global_dic["func_par"]["value2"]]
                elif isinstance(global_row,list):
                    value2 = global_row[global_header.index(global_dic["func_par"]["value2"])]
        elif global_dic["func_par"]["value3"] in inputs:
            if "constant" in inputs:
                value3 = global_dic["func_par"]["value3"]
            else:
                if isinstance(global_row,dict):
                    value3 = global_row[global_dic["func_par"]["value3"]]
                elif isinstance(global_row,list):
                    value3 = global_row[global_header.index(global_dic["func_par"]["value3"])]
        elif global_dic["func_par"]["value4"] in inputs:
            if "constant" in inputs:
                value4 = global_dic["func_par"]["value4"]
            else:
                if isinstance(global_row,dict):
                    value4 = global_row[global_dic["func_par"]["value4"]]
                elif isinstance(global_row,list):
                    value4 = global_row[global_header.index(global_dic["func_par"]["value4"])]
        elif global_dic["func_par"]["value5"] in inputs:
            if "constant" in inputs:
                value5 = global_dic["func_par"]["value5"]
            else:
                if isinstance(global_row,dict):
                    value5 = global_row[global_dic["func_par"]["value5"]]
                elif isinstance(global_row,list):
                    value5 = global_row[global_header.index(global_dic["func_par"]["value5"])]
        elif global_dic["func_par"]["value6"] in inputs:
            if "constant" in inputs:
                value6 = global_dic["func_par"]["value6"]
            else:
                if isinstance(global_row,dict):
                    value6 = global_row[global_dic["func_par"]["value6"]]
                elif isinstance(global_row,list):
                    value6 = global_row[global_header.index(global_dic["func_par"]["value6"])]
    if bool(value1) and bool(value2) and bool(value3) and\
       bool(value4) and bool(value5) and bool(value6):
        result = str(str(value1)+str(value2)+str(value3)+\
                 str(value4)+str(value5)+str(value6))
    else:
        result = "" 
    return(result)

def match_gdna():
    if isinstance(global_row,dict):
        combinedValue = global_row[global_dic["func_par"]["combinedValue"]]
    elif isinstance(global_row,list):
        combinedValue = global_row[global_header.index(global_dic["func_par"]["combinedValue"])]
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
    if isinstance(global_row,dict):
        combinedValue = global_row[global_dic["func_par"]["combinedValue"]]
    elif isinstance(global_row,list):
        combinedValue = global_row[global_header.index(global_dic["func_par"]["combinedValue"])]
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
    if isinstance(global_row,dict):
        combinedValue = global_row[global_dic["func_par"]["combinedValue"]]
    elif isinstance(global_row,list):
        combinedValue = global_row[global_header.index(global_dic["func_par"]["combinedValue"])]
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
    if isinstance(global_row,dict):
        combinedValue = global_row[global_dic["func_par"]["combinedValue"]]
    elif isinstance(global_row,list):
        combinedValue = global_row[global_header.index(global_dic["func_par"]["combinedValue"])]
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
    if isinstance(global_row,dict):
        threeLetters = global_row[global_dic["func_par"]["threeLetters"]]
        gene = global_row[global_dic["func_par"]["gene"]]
    elif isinstance(global_row,list):
        threeLetters = global_row[global_header.index(global_dic["func_par"]["threeLetters"])]
        gene = global_row[global_header.index(global_dic["func_par"]["gene"])]        
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
    if isinstance(global_row,dict):
        cds = global_row[global_dic["func_par"]["cds"]]
    elif isinstance(global_row,list):
        cds = global_row[global_header.index(global_dic["func_par"]["cds"])]
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
    if isinstance(global_row,dict):
        value = global_row[global_dic["func_par"]["value"]]
    elif isinstance(global_row,list):
        value = global_row[global_header.index(global_dic["func_par"]["value"])]
    return re.sub(global_dic["func_par"]["regex"],str(global_dic["func_par"]["replvalue"]),str(value))

# returns the index-th string obtained by splitting the string of the column at the first aprearance of the separator
def split():
    if isinstance(global_row,dict):
        column = global_row[global_dic["func_par"]["column"]]
    elif isinstance(global_row,list):
        column = global_row[global_header.index(global_dic["func_par"]["column"])]
    return str(column).split(global_dic["func_par"]["separator"])[int(global_dic["func_par"]["index"])]

def execute_function(row,header,dic):
    functions = {"tolower":"","toupper":"","totitle":"","trim":"","chomp":"","substring":"","replaceValue":"","variantIdentifier":"",
                "concat2":"","concat3":"","concat4":"","concat5":"","concat6":"","match_gdna":"","match_aa":"",
                "match_exon":"","rearrange_cds":"","match_pFormat":"","match":"","replaceRegex":"","split":""}
    func = dic["function"].split("/")[len(dic["function"].split("/"))-1]
    if func in functions:
        global global_row
        global_row = row
        global global_header
        global_header = header
        global global_dic
        global_dic = dic
        return eval(func + "()")             
    else:
        print("Invalid function")
        print("Aborting...")
        sys.exit(1)

def inner_function(row,dic,triples_map_list):

    functions = []
    keys = []
    for attr in dic["inputs"]:
        if ("reference function" in attr[1]):
            functions.append(attr[0])
        elif "constant" not in attr[1]:
            keys.append(attr[0])
    if functions:
        temp_dics = {}
        for function in functions:
            for tp in triples_map_list:
                if tp.triples_map_id == function:
                    temp_dic = create_dictionary(tp)
                    current_func = {"inputs":temp_dic["inputs"], 
                                    "function":temp_dic["executes"],
                                    "func_par":temp_dic,
                                    "termType":True}
                    temp_dics[function] = current_func
        temp_row = {}
        for dics in temp_dics:
            value = inner_function(row,temp_dics[dics],triples_map_list)
            temp_row[dics] = value
        for key in keys:
            temp_row[key] = row[key]
        return execute_function(temp_row,None,dic)
    else:
        return execute_function(row,None,dic)

def create_dictionary(triple_map):
    dic = {}
    inputs = []
    for tp in triple_map.predicate_object_maps_list:
        if "#" in tp.predicate_map.value:
            key = tp.predicate_map.value.split("#")[1]
            tp_type = tp.predicate_map.mapping_type
        elif "/" in tp.predicate_map.value:
            key = tp.predicate_map.value.split("/")[len(tp.predicate_map.value.split("/"))-1]
            tp_type = tp.predicate_map.mapping_type
        if "constant" in tp.object_map.mapping_type:
            value = tp.object_map.value
            tp_type = tp.object_map.mapping_type
        elif "#" in tp.object_map.value:
            value = tp.object_map.value.split("#")[1]
            tp_type = tp.object_map.mapping_type
        elif "/" in tp.object_map.value:
            value = tp.object_map.value.split("/")[len(tp.object_map.value.split("/"))-1]
            tp_type = tp.object_map.mapping_type
        else:
            value = tp.object_map.value
            tp_type = tp.object_map.mapping_type

        dic.update({key : value})
        if (key != "executes") and ([value,tp_type] not in inputs):
            inputs.append([value,tp_type])

    dic["inputs"] = inputs
    return dic
