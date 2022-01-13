import re
import csv
import sys
import os
import pandas as pd
from pathlib import Path
import requests
import unidecode

global global_dic
global_dic = {}
global functions_pool
functions_pool = {"findBiomarkerTestResult":"","findSemantic_HUPHM":"","findSemantic":"","findComorbidity":"","findFamilyRelationDegree":"","findFamilyRelationDegreeNewCategory":"","concat2":"","falcon_UMLS_CUI_function":""}
global semantic_dict
semantic_dict = dict()
global comprbidity_dict
comprbidity_dict = dict()
global familyDegree_dict
familyDegree_dict = dict()
global family_newCategory_dict
family_newCategory_dict = dict()

#######################################################################
############### *****SLCG***** Pre-preprocessing Functions ############
#######################################################################

def semanticDictionaryCreation():
    directory = Path(os.path.abspath(os.path.join(os.getcwd(), os.path.dirname(__file__)))).parent.absolute()
    semantic_df = pd.read_csv(str(directory)+"/Sources/CLARIFY-Project/SLCG_all_tables_except_comorbidity.csv", low_memory=False)
    for i in semantic_df.index:
        #print (str(semantic_df["value"][i]))
        key_name = str(semantic_df["table_name"][i]) + "_" + str(semantic_df["column_name"][i]) \
                                                + "_" + unidecode.unidecode(str(semantic_df["value"][i])).lower()
        replacedValue = semantic_df["replacement"][i]
        if type(replacedValue) == float:
            semantic_dict.update({key_name:replacedValue})
        else:
            semantic_dict.update({key_name:str(replacedValue)})        
            
semanticDictionaryCreation()


def findSemantic():
    #directory = Path(os.path.abspath(os.path.join(os.getcwd(), os.path.dirname(__file__)))).parent.absolute()
    #semantic_df = pd.read_csv(str(directory)+"/Sources/CLARIFY-Project/family_antecedents_treatment_line.csv", low_memory=False)
    #for i in range(0, len(semantic_df["family_member"])):
    #    if "Tío" in str(semantic_df["family_member"][i]):
    #        #print (semantic_dict.keys())
    #        print (semantic_dict["family_antecedents_treatment_line_family_member_Tío"])
    tableName = str(global_dic["tableName"])
    columnName = str(global_dic["columnName"])
    resource = str(global_dic["resource"])
    columnValue = unidecode.unidecode(str(global_dic["columnValue"]).replace(".0","")).lower()
    result = str()
    if bool(tableName) and bool(columnName) and bool(columnValue) and bool(columnValue):
        key = tableName + "_" + columnName + "_" + columnValue
        if key in semantic_dict:
            if str(semantic_dict[key]) != "nan":
                result = str(resource + str(semantic_dict[key]).replace(" ","_")) 
            else:
                result = ""
        else:
            result = ""
    return result

def comorbidityDictionaryCreation():
    directory = Path(os.path.abspath(os.path.join(os.getcwd(), os.path.dirname(__file__)))).parent.absolute()
    com_df = pd.read_csv(str(directory)+"/Sources/CLARIFY-Project/comorbidity_id_text.csv", low_memory=False)
    for i in com_df.index:
        key_name = str(com_df["TEXT"][i])
        replacedValue = com_df["TEXT_ID"][i]
        if type(replacedValue) == float:
            comprbidity_dict.update({key_name:replacedValue})
        else:
            comprbidity_dict.update({key_name:str(replacedValue)})            
            
comorbidityDictionaryCreation()


def findComorbidity():
    resource = global_dic["resource"]
    columnValue = global_dic["columnValue"]
    result = str()
    if bool(columnValue) and type(columnValue) != str:
        key = str(round(columnValue))
        if key in comprbidity_dict.keys():
            if str(comprbidity_dict[key]) != "nan":
                result = str(resource + str(comprbidity_dict[key]).replace(" ","_")) 
            else:
                result = ""
        else:
            result = ""
    elif bool(columnValue) and type(columnValue) == str:
        key = str(columnValue)
        if key in comprbidity_dict.keys():
            result = str(resource + str(comprbidity_dict[key]).replace(" ","_"))
        else:
            result = ""
    else:
        result = ""
    return result

def familyRelationDegreeDictionaryCreation():
    directory = Path(os.path.abspath(os.path.join(os.getcwd(), os.path.dirname(__file__)))).parent.absolute()
    family_df = pd.read_csv(str(directory)+"/Sources/CLARIFY-Project/family_antecedents_degree.csv", low_memory=False)
    for i in family_df.index:
        key_name = str(family_df["table_name"][i]) + "_" + str(family_df["column_name"][i]) \
                                                + "_" + unidecode.unidecode(str(family_df["value"][i])).lower()
        replacedValue = family_df["replacement"][i]
        if type(replacedValue) == float:
            familyDegree_dict.update({key_name:replacedValue})
        else:
            familyDegree_dict.update({key_name:str(replacedValue)})        
            
familyRelationDegreeDictionaryCreation()

def findFamilyRelationDegree():
    tableName = str(global_dic["tableName"])
    columnName = str(global_dic["columnName"])
    resource = str(global_dic["resource"])
    columnValue = unidecode.unidecode(str(global_dic["columnValue"]).replace(".0","")).lower()
    result = str()
    if bool(tableName) and bool(columnName) and bool(columnValue) and bool(columnValue):
        key = tableName + "_" + columnName + "_" + columnValue
        if key in familyDegree_dict:
            if str(familyDegree_dict[key]) != "nan":
                result = str(resource + str(familyDegree_dict[key]).replace(" ","_")) 
            else:
                result = ""
        else:
            result = ""
    return result

def semanticDictionaryCreation_HUPHM():
    directory = Path(os.path.abspath(os.path.join(os.getcwd(), os.path.dirname(__file__)))).parent.absolute()
    semantic_df = pd.read_csv(str(directory)+"/Sources/CLARIFY-Project/HUPHM_specific_dictionary.csv", low_memory=False)
    for i in semantic_df.index:
        key_name = str(semantic_df["table_name"][i]) + "_" + str(semantic_df["column_name"][i]) \
                                                + "_" + str(semantic_df["value"][i]).lower()
        replacedValue = semantic_df["replacement"][i]
        if type(replacedValue) == float:
            semantic_dict.update({key_name:replacedValue})
        else:
            semantic_dict.update({key_name:str(replacedValue)})        
            
semanticDictionaryCreation_HUPHM()

def findSemantic_HUPHM():
    tableName = str(global_dic["tableName"])
    columnName = str(global_dic["columnName"])
    resource = str(global_dic["resource"])
    columnValue = str(global_dic["columnValue"]).replace(".0","").lower()
    result = str()
    if bool(tableName) and bool(columnName) and bool(columnValue) and bool(columnValue):
        key = tableName + "_" + columnName + "_" + columnValue
        if key in semantic_dict:
            if str(semantic_dict[key]) != "nan":
                result = str(resource + str(semantic_dict[key]).replace(" ","_")) 
                print (result)
            else:
                result = ""
        else:
            result = ""
    return result

def familyRelationDegreeNewCategoryDictionaryCreation():
    directory = Path(os.path.abspath(os.path.join(os.getcwd(), os.path.dirname(__file__)))).parent.absolute()
    family_newCategory_df = pd.read_csv(str(directory)+"/Sources/CLARIFY-Project/family_antecedents_degree_newCategory.csv", low_memory=False)
    for i in family_newCategory_df.index:
        key_name = str(family_newCategory_df["table_name"][i]) + "_" + str(family_newCategory_df["column_name"][i]) \
                                                + "_" + unidecode.unidecode(str(family_newCategory_df["value"][i])).lower()
        replacedValue = family_newCategory_df["replacement"][i]
        if type(replacedValue) == float:
            family_newCategory_dict.update({key_name:replacedValue})
        else:
            family_newCategory_dict.update({key_name:str(replacedValue)})        
            
familyRelationDegreeNewCategoryDictionaryCreation()

def findFamilyRelationDegreeNewCategory():
    tableName = str(global_dic["tableName"])
    columnName = str(global_dic["columnName"])
    resource = str(global_dic["resource"])
    columnValue = unidecode.unidecode(str(global_dic["columnValue"]).replace(".0","")).lower()
    result = str()
    if bool(tableName) and bool(columnName) and bool(columnValue) and bool(columnValue):
        key = tableName + "_" + columnName + "_" + columnValue
        if key in family_newCategory_dict:
            if str(family_newCategory_dict[key]) != "nan":
                result = str(resource + str(family_newCategory_dict[key]).replace(" ","_")) 
            else:
                result = ""
        else:
            result = ""
    return result

def biomarkerDictionaryCreation():
    directory = Path(os.path.abspath(os.path.join(os.getcwd(), os.path.dirname(__file__)))).parent.absolute()
    semantic_df = pd.read_csv(str(directory)+"/Sources/CLARIFY-Project/SLCG_biomarkers.csv", low_memory=False)
    for i in semantic_df.index:
        #print (str(semantic_df["value"][i]))
        key_name = str(semantic_df["table_name"][i]) + "_" + str(semantic_df["column_name"][i]) \
                                                + "_" + str(semantic_df["biomarker"][i]) \
                                                + "_" + str(semantic_df["value"][i])
        replacedValue = semantic_df["replacement"][i]
        if type(replacedValue) == float:
            semantic_dict.update({key_name:replacedValue})
        else:
            semantic_dict.update({key_name:str(replacedValue)})        
            
biomarkerDictionaryCreation()


def findBiomarkerTestResult():
    #directory = Path(os.path.abspath(os.path.join(os.getcwd(), os.path.dirname(__file__)))).parent.absolute()
    #semantic_df = pd.read_csv(str(directory)+"/Sources/CLARIFY-Project/family_antecedents_treatment_line.csv", low_memory=False)
    #for i in range(0, len(semantic_df["family_member"])):
    #    if "Tío" in str(semantic_df["family_member"][i]):
    #        #print (semantic_dict.keys())
    #        print (semantic_dict["family_antecedents_treatment_line_family_member_Tío"])
    tableName = str(global_dic["tableName"])
    columnName = str(global_dic["columnName"])
    biomarkerName = str(global_dic["conditionColumn"])
    resource = str(global_dic["resource"])
    columnValue = str(global_dic["columnValue"]).replace(".0","")
    result = str()
    if bool(tableName) and bool(columnName) and bool(columnValue) and bool(columnValue):
        key = tableName + "_" + columnName + "_" + biomarkerName + "_" + columnValue
        if key in semantic_dict:
            if str(semantic_dict[key]) != "nan":
                result = str(resource + str(semantic_dict[key]).replace(" ","_")) 
            else:
                result = ""
        else:
            result = ""
    return result






################################################################################
############### *****Breast Cancer***** Pre-preprocessing Functions ############
################################################################################



############################################################
################ Entity-Linking Functions ##################
############################################################

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
        output = response['cui']
    else:
        output = ""
    return output

def concat2():
    value1 = global_dic["value1"]
    value2 = global_dic["value2"]
    if bool(value1) and bool(value2):
        result = str(str(value1)+str(value2))
    else:
        result = ""  
    return(result)


###############################################################################################
################################## Static (Do NOT change) #####################################

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
