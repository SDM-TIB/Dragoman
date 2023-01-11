import re
import csv
import sys
import os
from .string_subs import *
import pandas as pd

global global_dic
global_dic = {}
global functions_pool

#####################################################################################################
########### ADD THE IMPLEMENTATION OF YOUR FUNCTIONS HERE FOLLOWING THE EXAMPLES ####################
#####################################################################################################

functions_pool = {"credibilityScore":""}

def credibilityScore(): 
    cScore = global_dic["value"]
    if bool(cScore) and cScore == 0.5:
        credibilityClass = "Undecided"
    elif bool(cScore) and cScore > 0.5:
        credibilityClass = "Credible"
    elif bool(cScore) and cScore < 0.5:
        credibilityClass = "NotCredible"
    else:
        credibilityClass = ""
    return credibilityClass



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
