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
