'''
@auther: Samaneh
'''
import pandas as pd
import numpy as np
import math
import csv
import re

#################################################################################

def formalize(df):
    pFormatList = [""]* len(df)
    cFormatList = [""]* len(df)
    gFormatList = [""]* len(df)
    vIDList = [""]* len(df)
    for i in range(0,len(df)):
        allList = str(df["AAChange.refGene"][i]).split(":")
        geneName = str(df["AAChange.refGene"][i]).split(":")[0]
        for j in range(0,len(allList)):
            if "p." in allList[j] and geneName not in allList[j]:
                if "del" not in allList[j]:
                    pFormatValue = geneName + "_" + allList[j].split(".")[1]
                    pFormatList[i] = pFormatValue
            if "c." in allList[j] and geneName not in allList[j]:
                if "del" not in allList[j] and "ins" not in allList[j]:
                    firstAA = allList[j].split(".")[1][0]
                    if allList[j].split(".")[1][1:-1].isdigit():
                        gp = allList[j].split(".")[1][1:-1]
                        secondAA = allList[j].split(".")[1][-1]
                    else:
                        gp = allList[j].split(".")[1][1:-2] 
                        secondAA = allList[j].split(".")[1][-2:]
                        #print (allList[j].split("."),"-------",secondAA,"----",gp)
                    cFormatValue = geneName + "_c." + gp + firstAA + "~" + secondAA
                    cFormatList[i] = cFormatValue
                else:
                    cFormatValue = geneName + "_" + allList[j]
                    cFormatList[i] = cFormatValue
        if df["Start"][i] == df["End"][i]:
            gFormatValue = df["Chr"][i] + "_g." + str(df["Start"][i]).split(".")[0] + df["Ref"][i] + "~" + df["Alt"][i]
            gFormatList[i] = gFormatValue
        vIDvalue = df["Gene.refGene"][i] + "_" + df["Chr"][i] +  "_" +  str(df["Start"][i]).split(".")[0] + "_" + df["Ref"][i] + "~" + df["Alt"][i]
        vIDList[i] = vIDvalue     

    return(gFormatList, cFormatList, pFormatList, vIDList)

def variantType(df):
    rowNumber = len(df)
    variantList = []
    for j in range(0,rowNumber):
        if df["AF"][j] < 0.4:
            variantList.append("somatic")
        else:
            variantList.append("germline")
    return (variantList)

def variantDescription(df):
    variantsDict = {"frameshift_deletion":"4", "frameshift_insertion":"4", "nonframeshift_deletion":"3", "nonframeshift_insertion":"3", "nonframeshift_substitution":"2", "nonsynonymous_SNV":"2", "stopgain":"3", "synonymous_SNV":"1","":"","nan":"----"}
    rowNumber = len(df)
    variantScoreList = [""]* len(df)
    for j in range(0,rowNumber):
        value = df["ExonicFunc.refGene"][j]
        if type(value) == str and value != "": 
            variantScoreList[j] = variantsDict[df["ExonicFunc.refGene"][j]]
        else:
            variantScoreList[j] = ""

    return (variantScoreList)

def handler():

    df = pd.read_csv("/mnt/e/Backup/Deliverable/genomic/annovarVersionJune2019_txt/liquid/selected/mergedVariants_withDates.csv", low_memory=False)
    cols = list(df.columns)
    gFormat, cFormat, pFormat, vIDList = formalize(df)
    variantList = variantType(df)
    variantScoreList = variantDescription(df)
    df['gFormat'] = gFormat
    cols.append('gFormat')
    df['cFormat'] = cFormat
    cols.append('cFormat')
    df['pFormat'] = pFormat
    cols.append('pFormat')
    df['variantType_basedOnAF'] = variantList
    cols.append('variantType_basedOnAF')
    df['variantScore_basedOnExonicFunc'] = variantScoreList
    cols.append('variantScore_basedOnExonicFunc')
    df['variantID'] = vIDList
    cols.append('variantID')
    df.columns = cols 
    df.to_csv("/mnt/e/Backup/Deliverable/genomic/annovarVersionJune2019_txt/liquid/selected/mergedVariants_withDates_withVariantAnnotations.csv")

if __name__ == "__main__":
        handler()