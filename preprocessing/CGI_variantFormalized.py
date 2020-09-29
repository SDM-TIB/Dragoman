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

def handler():

    df = pd.read_csv("/mnt/e/Backup/CGI/processed/cgi_biomarkers_per_variant_processed.csv", low_memory=False)
    gFormat = df['gDNA']
    cFormat = [""]* len(df)
    pFormat = [""]* len(df)
    for i in range (0, len(df)):  
    	gene = str(df['Gene'][i])
    	gFormat[i] = str(gFormat[i]).replace(":","_").replace(">","~")   
    	cFormat[i] = str(gene) + "_" + str(df['cDNA'][i]).replace(">","~")
    gSeries = pd.Series(gFormat, name="gFormat")
    cSeries = pd.Series(cFormat, name="cFormat")
    df_result = pd.concat([df, gSeries, cSeries], axis=1)

    df_result.to_csv("/mnt/e/Backup/CGI/processed/cgi_biomarkers_per_variant_processed_formalized.csv")

if __name__ == "__main__":
        handler()