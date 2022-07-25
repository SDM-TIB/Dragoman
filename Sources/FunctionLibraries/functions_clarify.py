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
functions_pool = {
"findSemantic":"","findComorbidity":"",
"findFamilyRelationDegree":"","findSemantic_HUPHM":"",
"findFamilyRelationDegreeNewCategory":"",
"findBiomarkerTestResult":"","concat2":"",
"findTreatmentType":"",
"findSemanticStringOutput":"","concat6":"",
"findSemantic_DrugMixture_HUPHM_BreastCancer":"",
"findSemantic_OralDrugType_HUPHM_BreastCancer":"",
"findSemantic_smokinghabit_HUPHM_BreastCancer":"",
"findSemantic_HUPHM_BreastCancer":"",
"findDrug_LC":"","findDrug_BC":"",
"findDrugSchema_LC":"","findDrugSchema_BC":"",
"replace_unwanted_characters":"","toLower":"",
"falcon_UMLS_CUI_function":""
}

global semantic_dict
semantic_dict = dict()
global comprbidity_dict
comprbidity_dict = dict()
global familyDegree_dict
familyDegree_dict = dict()
global family_newCategory_dict
family_newCategory_dict = dict()
global semantic_HUPHM_dict
semantic_HUPHM_dict = dict()
global semantic_bio_dict
semantic_bio_dict = dict()
global semantic_oralDrug_dict
semantic_oralDrug_dict = dict()
global semantic_tnm_dict
semantic_tnm_dict = dict()
global semantic_smoking_dict
semantic_smoking_dict = dict()
global semantic_bc_dict
semantic_bc_dict = dict()
global semantic_drug_dict
semantic_drug_dict = dict()
global treatmentType_dict
treatmentType_dict = dict()


#######################################################################
############### *****SLCG***** Pre-preprocessing Functions ############
#######################################################################

def semanticDictionaryCreation():
    directory = Path(os.path.abspath(os.path.join(os.getcwd(), os.path.dirname(__file__)))).parent.absolute()
    semantic_df = pd.read_csv(str(directory)+"/Sources/CLARIFY-Project/SLCG_all_tables_except_comorbidity.csv", low_memory=False)
    for i in semantic_df.index:
        key_name = str(semantic_df["table_name"][i]) + "_" + str(semantic_df["column_name"][i]) \
                                                + "_" + unidecode.unidecode(str(semantic_df["value"][i])).lower()
        replacedValue = semantic_df["replacement"][i]
        if type(replacedValue) == float:
            semantic_dict.update({key_name:replacedValue})
        else:
            semantic_dict.update({key_name:str(replacedValue)})        
            
semanticDictionaryCreation()


def findSemantic():
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
    semantic_HUPHM_df = pd.read_csv(str(directory)+"/Sources/CLARIFY-Project/HUPHM_specific_dictionary.csv", low_memory=False)
    for i in semantic_HUPHM_df.index:
        key_name = str(semantic_HUPHM_df["table_name"][i]) + "_" + str(semantic_HUPHM_df["column_name"][i]) \
                                                + "_" + str(semantic_HUPHM_df["value"][i]).lower()
        replacedValue = semantic_HUPHM_df["replacement"][i]
        if type(replacedValue) == float:
            semantic_HUPHM_dict.update({key_name:replacedValue})
        else:
            semantic_HUPHM_dict.update({key_name:str(replacedValue)})        
            
semanticDictionaryCreation_HUPHM()

def findSemantic_HUPHM():
    tableName = str(global_dic["tableName"])
    columnName = str(global_dic["columnName"])
    resource = str(global_dic["resource"])
    columnValue = str(global_dic["columnValue"]).replace(".0","").lower()
    result = str()
    if bool(tableName) and bool(columnName) and bool(columnValue) and bool(columnValue):
        key = tableName + "_" + columnName + "_" + columnValue
        if key in semantic_HUPHM_dict:
            if str(semantic_HUPHM_dict[key]) != "nan":
                result = str(resource + str(semantic_HUPHM_dict[key]).replace(" ","_")) 
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
    semantic_bio_df = pd.read_csv(str(directory)+"/Sources/CLARIFY-Project/SLCG_biomarkers.csv", low_memory=False)
    for i in semantic_bio_df.index:
        #print (str(semantic_df["value"][i]))
        key_name = str(semantic_bio_df["table_name"][i]) + "_" + str(semantic_bio_df["column_name"][i]) \
                                                + "_" + str(semantic_bio_df["biomarker"][i]) \
                                                + "_" + str(semantic_bio_df["value"][i])
        replacedValue = semantic_bio_df["replacement"][i]
        if type(replacedValue) == float:
            semantic_bio_dict.update({key_name:replacedValue})
        else:
            semantic_bio_dict.update({key_name:str(replacedValue)})        
            
biomarkerDictionaryCreation()


def findBiomarkerTestResult():
    tableName = str(global_dic["tableName"])
    columnName = str(global_dic["columnName"])
    biomarkerName = str(global_dic["conditionColumn"])
    resource = str(global_dic["resource"])
    columnValue = str(global_dic["columnValue"]).replace(".0","")
    result = str()
    if bool(tableName) and bool(columnName) and bool(columnValue) and bool(columnValue):
        key = tableName + "_" + columnName + "_" + biomarkerName + "_" + columnValue
        if key in semantic_bio_dict:
            if str(semantic_bio_dict[key]) != "nan":
                result = str(resource + str(semantic_bio_dict[key]).replace(" ","_")) 
            else:
                result = ""
        else:
            result = ""
    return result

def TreatmentTypeDictionaryCreation():
    directory = Path(os.path.abspath(os.path.join(os.getcwd(), os.path.dirname(__file__)))).parent.absolute()
    treatmentType_df = pd.read_csv(str(directory)+"/Sources/CLARIFY-Project/family_antecedents_degree_newCategory.csv", low_memory=False)
    for i in treatmentType_df.index:
        key_name = str(treatmentType_df["table_name"][i]) + "_" + str(treatmentType_df["column_name"][i]) \
                                                + "_" + unidecode.unidecode(str(treatmentType_df["value"][i])).lower()
        replacedValue = treatmentType_df["replacement"][i]
        if type(replacedValue) == float:
            treatmentType_dict.update({key_name:replacedValue})
        else:
            treatmentType_dict.update({key_name:str(replacedValue)})        
            
TreatmentTypeDictionaryCreation()

def findTreatmentType():
    tableName = str(global_dic["tableName"])
    columnName = str(global_dic["columnName"])
    resource = str(global_dic["resource"])
    columnValue = unidecode.unidecode(str(global_dic["columnValue"]).replace(".0","")).lower()
    result = str()
    if bool(tableName) and bool(columnName) and bool(columnValue) and bool(columnValue):
        key = tableName + "_" + columnName + "_" + columnValue
        if key in treatmentType_dict:
            if str(treatmentType_dict[key]) != "nan":
                result = str(resource + str(treatmentType_dict[key]).replace(" ","_")) 
            else:
                result = ""
        else:
            result = ""
    return result

def findSemanticStringOutput():
    tableName = str(global_dic["tableName"])
    columnName = str(global_dic["columnName"])
    columnValue = unidecode.unidecode(str(global_dic["columnValue"]).replace(".0","")).lower()
    result = str()
    if bool(tableName) and bool(columnName) and bool(columnValue) and bool(columnValue):
        key = tableName + "_" + columnName + "_" + columnValue
        if key in semantic_dict:
            if str(semantic_dict[key]) != "nan":
                result = str(semantic_dict[key]).replace(" ","_") 
            else:
                result = ""
        else:
            result = ""
    return result


################################################################################
############### *****Breast Cancer***** Pre-preprocessing Functions ############
################################################################################

def semanticDictionaryCreation_OralDrugType_HUPHM_BreastCancer():
    directory = Path(os.path.abspath(os.path.join(os.getcwd(), os.path.dirname(__file__)))).parent.absolute()
    semantic_oralDrug_df = pd.read_csv(str(directory)+"/Sources/CLARIFY-Project/OralDrugType_BreastCancer_dictionary.csv", low_memory=False)
    for i in semantic_oralDrug_df.index:
        key_name = str(semantic_oralDrug_df["OralDrug"][i]).lower()
        replacedValue = semantic_oralDrug_df["TherapyType"][i]
        if type(replacedValue) == float:
            semantic_oralDrug_dict.update({key_name:replacedValue})
        else:
            semantic_oralDrug_dict.update({key_name:str(replacedValue)})        
            
semanticDictionaryCreation_OralDrugType_HUPHM_BreastCancer()

def findSemantic_OralDrugType_HUPHM_BreastCancer():
    resource = str(global_dic["resource"])
    columnValue = str(global_dic["OralDrug"]).lower()
    result = str()
    if bool(tableName) and bool(columnName) and bool(columnValue):
        key = tableName + "_" + columnName + "_" + columnValue
        if key in semantic_oralDrug_dict:
            if str(semanti_oralDrugc_dict[key]) != "nan":
                result = str(resource + str(semantic_oralDrug_dict[key]).replace(" ","_")) 
            else:
                result = ""
        else:
            result = ""
    return result  


def semanticDictionaryCreation_smokingHabit_HUPHM_BreastCancer():
    directory = Path(os.path.abspath(os.path.join(os.getcwd(), os.path.dirname(__file__)))).parent.absolute()
    semantic_smoking_df = pd.read_csv(str(directory)+"/Sources/CLARIFY-Project/HUPHM_BreastCancer_dictionary.csv", low_memory=False)
    for i in semantic_smoking_df.index:
        key_name = str(semantic_smoking_df["value"][i]).lower()
        replacedValue = semantic_smoking_df["replacement"][i]
        if type(replacedValue) == float:
            semantic_smoking_dict.update({key_name:replacedValue})
        else:
            semantic_smoking_dict.update({key_name:str(replacedValue)})        
            
semanticDictionaryCreation_smokingHabit_HUPHM_BreastCancer()

def findSemantic_smokinghabit_HUPHM_BreastCancer():
    resource = str(global_dic["resource"])
    columnValue = str(global_dic["columnValue"]).replace(".0","").lower()
    result = str()
    if bool(columnValue) and bool(columnValue):
        key = tableName + "_" + columnName + "_" + columnValue
        if key in semantic_smoking_dict:
            if str(semantic_semantic_smoking_dictdict[key]) != "nan":
                result = str(resource + str(semantic_smoking_dict[key]).replace(" ","_")) 
            else:
                result = ""
        else:
            result = ""
    return result

def semanticDictionaryCreation_HUPHM_BreastCancer():
    directory = Path(os.path.abspath(os.path.join(os.getcwd(), os.path.dirname(__file__)))).parent.absolute()
    semantic_bc_df = pd.read_csv(str(directory)+"/Sources/CLARIFY-Project/HUPHM_BreastCancer_dictionary.csv", low_memory=False)
    for i in semantic_bc_df.index:
        key_name = str(semantic_bc_df["table_name"][i]) + "_" + str(semantic_bc_df["column_name"][i]) \
                                                + "_" + str(semantic_bc_df["value"][i]).lower()
        replacedValue = semantic_bc_df["replacement"][i]
        if type(replacedValue) == float:
            semantic_bc_dict.update({key_name:replacedValue})
        else:
            semantic_bc_dict.update({key_name:str(replacedValue)})        
            
semanticDictionaryCreation_HUPHM_BreastCancer()

def findSemantic_HUPHM_BreastCancer():
    tableName = str(global_dic["tableName"])
    columnName = str(global_dic["columnName"])
    resource = str(global_dic["resource"])
    columnValue = str(global_dic["columnValue"]).replace(".0","").lower()
    result = str()
    if bool(tableName) and bool(columnName) and bool(columnValue) and bool(columnValue):
        key = tableName + "_" + columnName + "_" + columnValue
        if key in semantic_bc_dict:
            if str(semantic_bc_dict[key]) != "nan":
                result = str(resource + str(semantic_bc_dict[key]).replace(" ","_")) 
            else:
                result = ""
        else:
            result = ""
    return result

def calculate_surgeryDate_HUPHM_BreastCancer():
    yearValue = str(global_dic["year"])
    monthValue = str(global_dic["month"])
    dayValue = str(global_dic["day"])
    result = str()
    if bool(yearValue) and bool(monthValue) and bool(dayValue):
        result = yearValue + "-" + monthValue + "-" + dayValue
    else:
        result = ""
    return result

################################################################################
##################### Drug Schema ############## Lung Cancer ###################
################################################################################
def semanticDictionaryCreation_Drug():
    directory = Path(os.path.abspath(os.path.join(os.getcwd(), os.path.dirname(__file__)))).parent.absolute()
    semantic_drug_df = pd.read_csv(str(directory)+"/Sources/CLARIFY-Project/schema_drugs_LC_and_BC.csv", low_memory=False)
    for i in semantic_drug_df.index:
        key_name = str(semantic_drug_df["table_name"][i]) + "_" + str(semantic_drug_df["column_name"][i]) \
                                                + "_" + str(semantic_drug_df["value"][i]).lower()
        replacedValue = semantic_drug_df["replacement"][i]
        if type(replacedValue) == float:
            semantic_drug_dict.update({key_name:replacedValue})
        else:
            semantic_drug_dict.update({key_name:str(replacedValue)})        
            
semanticDictionaryCreation_Drug()

def findDrug_LC():
    tableName = str(global_dic["tableName"])
    columnName = str(global_dic["columnName"])
    resource = str(global_dic["resource"])
    columnValue = str(global_dic["columnValue"]).replace(".0","").lower()
    result = str()
    if bool(tableName) and bool(columnName) and bool(columnValue) and bool(columnValue):
        key = tableName + "_" + columnName + "_" + columnValue
        if key in semantic_drug_dict:
            if str(semantic_drug_dict[key]) != "nan":
                result = str(resource + str(semantic_drug_dict[key]).replace(" ","_")) 
                print (result)
            else:
                result = ""
        else:
            result = ""
    return result

def findDrug_BC():
    tableName = str(global_dic["tableName"])
    columnName = str(global_dic["columnName"])
    resource = str(global_dic["resource"])
    columnValue = str(global_dic["columnValue"]).replace(".0","").lower()
    result = str()
    if bool(tableName) and bool(columnName) and bool(columnValue) and bool(columnValue):
        key = tableName + "_" + columnName + "_" + columnValue
        if key in semantic_drug_dict:
            if str(semantic_drug_dict[key]) != "nan":
                valueList = str(semantic_drug_dict[key]).split("_")
                result = list(str(resource + valueList[0]),str(resource + valueList[1]))
            else:
                result = ""
        else:
            result = ""
    return result

def findDrug(DrugName):
    tableName = str("chemotherapy_treatment_line")
    columnName = str("f1_schema")
    resource = str("http://clarify2020.eu/entity/")
    columnValue = str(DrugName).replace(".0","").lower()
    result = str()
    if bool(tableName) and bool(columnName) and bool(columnValue) and bool(columnValue):
        key = tableName + "_" + columnName + "_" + columnValue
        if key in semantic_drug_dict:
            if str(semantic_drug_dict[key]) != "nan":
                result = str(resource + str(semantic_drug_dict[key]).replace(" ","_")) 
                print (result)
            else:
                result = ""
        else:
            result = ""
    return result   

def findDrugSchema_LC():
    result = ""
    drug1 = str(global_dic["drug1"]).replace(".0","").lower()
    drug2 = str(global_dic["drug2"]).replace(".0","").lower()
    drug3 = str(global_dic["drug3"]).replace(".0","").lower()
    result = str()
    key1 = "chemotherapy_treatment_line_f1_schema_" + str(drug1).replace(".0","").lower()
    if key1 in semantic_drug_dict:
        if str(semantic_drug_dict[key1]) != "nan":
                drugName1 = str("http://clarify2020.eu/entity/" + str(semantic_drug_dict[key1]).replace(" ","_")) 
                print (drugName1)
        else:
            drugName1 = ""
    else:
        drugName1 = ""
    key2 = "chemotherapy_treatment_line_f2_schema_" + str(drug2).replace(".0","").lower()
    if key2 in semantic_drug_dict:
        if str(semantic_drug_dict[key2]) != "nan":
                drugName2 = str("http://clarify2020.eu/entity/" + str(semantic_drug_dict[key2]).replace(" ","_")) 
                print (drugName2)
        else:
            drugName2 = ""
    else:
        drugName2 = ""
    key3 = "chemotherapy_treatment_line_f3_schema_" + str(drug3).replace(".0","").lower()
    if key3 in semantic_drug_dict:
        if str(semantic_drug_dict[key3]) != "nan":
                drugName3 = str("http://clarify2020.eu/entity/" + str(semantic_drug_dict[key3]).replace(" ","_")) 
                print (drugName3)
        else:
            drugName3 = ""
    else:
        drugName3 = ""
    result = drugName1 + "_" + drugName2 + "_" + drugName3
    return result

def findDrugSchema_BC():
    tableName = str(global_dic["tableName"])
    columnName = str(global_dic["columnName"])
    resource = str(global_dic["resource"])
    columnValue = str(global_dic["columnValue"]).replace(".0","").lower()
    result = str()
    if bool(tableName) and bool(columnName) and bool(columnValue) and bool(columnValue):
        key = tableName + "_" + columnName + "_" + columnValue
        if key in semantic_drug_dict:
            if str(semantic_drug_dict[key]) != "nan":
                result = str(resource + str(semantic_drug_dict[key]).replace(" ","_"))
            else:
                result = ""
        else:
            result = ""
    return result
############################################################
######### General String processing Functions ##############
############################################################

def concat2():
    value1 = global_dic["value1"]
    value2 = global_dic["value2"]
    if bool(value1) and bool(value2):
        result = str(str(value1)+str(value2))
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

def replace_unwanted_characters():  ## this functions falls in replace() function subcategory; developed specifically for UMLS dictionary
    value1 = global_dic["value1"]
    if bool(value1):
        result = str(str(value1).replace(", ","_").replace(",","_").replace(" ","_"))
    else:
        result = ""  
    return(result)

def toLower(): 
    return global_dic["value"].lower()

############################################################
################ Entity-Linking Functions ##################
############################################################

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