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

    df = pd.read_csv("/mnt/e/Backup/OncoKB/processed/allAnnotatedVariants_cleaned.csv", low_memory=False)
    pFormat = [""]* len(df)
    for i in range (0, len(df)):       
        pFormat[i] =  str(df['Hugo Symbol'][i]) + "_" + str(df['Protein Change'][i])
    pSeries = pd.Series(pFormat, name="pFormat")
    df_result = pd.concat([df, pSeries], axis=1)

    df_result.to_csv("/mnt/e/Backup/OncoKB/processed/allAnnotatedVariants_cleaned_formalized.csv")

if __name__ == "__main__":
        handler()