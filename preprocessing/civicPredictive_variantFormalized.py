'''
@auther: Samaneh
'''
import pandas as pd
import numpy as np
import math
import csv
import re
import math

#################################################################################

#def formalizeMFormat(df, mFormat):
#    mFormat = list(str(df["gene.clinical"][z])+"_"+str(df["variant.clinical"][z]) for z in range (0, len(df)))
#    return(mFormat)

def formalize(df, gFormat, cFormat, pFormat):
    for i in range (0, len(df)):           
        if type(df["hgvs_expressions"][i])==str: # type(df["hgvs_expressions"][i])==float ----> the value is always "nan" if type == float
            fList = df["hgvs_expressions"][i].split(":")
            for j in range(0,len(fList)):
                if "g." in fList[j]:
                    gFormat[i] = "chr" + df["chromosome.clinical"][i] + "\\" + fList[j].split(",")[0]
                if "c." in fList[j]:    
                    cFormat[i] = df["gene.clinical"][i] + "\\" + fList[j].split(",")[0]
                if "p." in fList[j]:  
                    pFormat[i] = fList[j].split(",")[0].lower()     

    pFormat = aminoAcidsConversion(df, pFormat)                 
    return(gFormat, cFormat, pFormat)

def aminoAcidsConversion(df, pFormat):
    aminoAcidsDic = {
    "ala":"A", "arg":"R", "asn":"N", "asp":"D", "asx":"B", "cys":"C", "glu":"E", "gln":"Q", "glx":"Z", "gly":"G", "his":"H", "ile":"I", "leu":"L", "lys":"K", 
    "met":"M", "phe":"F", "pro":"P", "ser":"S", "thr":"T", "trp":"W", "tyr":"Y", "val":"V"  
    }
    for i in range(0, len(pFormat)):
        if pFormat[i] != "":
            first = pFormat[i].split(".")[1][0:3]
            second = pFormat[i].split(".")[1][-3:]
            middle = pFormat[i].split(".")[1][3:-3]
            if first in aminoAcidsDic.keys() and second in aminoAcidsDic.keys():
                pFormat[i] = df["gene.clinical"][i] + "\\" + aminoAcidsDic[first] + middle + aminoAcidsDic[second]
    return(pFormat)

def handler():

    df = pd.read_csv("/mnt/e/Backup/Deliverable/genomic/civic_Predictive.csv", low_memory=False)
    gFormat = [""]* len(df)
    pFormat = [""]* len(df)
    cFormat = [""]* len(df)
    aminoAcidsDic = {}

    gFormat,cFormat, pFormat = formalize(df, gFormat, cFormat, pFormat)

    gFormatSeries = pd.Series(gFormat, name="gFormat")
    cFormatSeries = pd.Series(cFormat, name="cFormat")
    pFormatSeries = pd.Series(pFormat, name="pFormat")
    
    df_result = pd.concat([df, gFormatSeries, cFormatSeries, pFormatSeries], axis=1)
    df_result.to_csv("/mnt/e/Backup/Deliverable/genomic/civic_Predictive_formalized.csv")

if __name__ == "__main__":
        handler()