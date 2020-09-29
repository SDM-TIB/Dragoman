'''
@auther: Samaneh
'''
import pandas as pd
import numpy as np
import math
import csv
import re

#################################################################################

def formalize_cFormat(df):
    cFormat = list(re.sub('_.*','',str(df["Gene name"][j]))+"_"+str(df["Mutation CDS"][j]).replace(">", "~") for j in range (0, len(df)))
    for j in range (0, len(df)):
        if (str(df["Mutation CDS"][j]) != "nan"):
            print (str(df["Mutation CDS"][j]).split("c.")[0])
    #pFormat = list(str(df["Gene name"][i])+"_"+str(df["Mutation AA"][i]).split(".")[1].replace(">", "~") for i in range (0, len(df)))
    return(cFormat)

def formalize_pFormat(df):  
    pFormat = [""]* len(df)
    for i in range (0, len(df)):
        if str(df["Mutation AA"][i]) != "nan" and "?" not in str(df["Mutation AA"][i]):
            pFormat[i] = re.sub('_.*','',str(df["Gene name"][i]))+"_"+str(df["Mutation AA"][i]).split(".")[1].replace(">", "~") 
    return (pFormat)


def handler():

    #df = pd.read_csv("/mnt/e/Backup/COSMIC/GRCh37/mutations/COSMIC_mutation.csv", low_memory=False)
    df = pd.read_csv("/mnt/e/Backup/COSMIC/veracity25.csv", low_memory=False)
    cFormat = formalize_cFormat(df)
    #pFormat = formalize_pFormat(df)
    #cFormatSeries = pd.Series(cFormat, name="cFormat")
    #pFormatSeries = pd.Series(pFormat, name="pFormat")
    #df_result = pd.concat([df, cFormatSeries, pFormatSeries], axis=1)
    #df_result.to_csv("/mnt/e/Backup/COSMIC/GRCh37/mutations/processed/COSMIC_mutation_formalized_expTest.csv")
    #df_result.to_csv("/mnt/e/Backup/COSMIC/GRCh37/expTest.csv")

if __name__ == "__main__":
        handler()
