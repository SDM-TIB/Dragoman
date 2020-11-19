#! /bin/bash


## mappings to be applied in the experiments
declare -a mappings=("cosmic_mutation.ttl")
## datasets to be applied in the experiments
declare -a dataArray=("veracity100.csv")


## Running FunMap:
python3 /FunMap/run_translator.py config-FunMap.ini
## Running SDM-RDFizer over the results:
python3 /mnt/e/TOOLS/SDM-RDFizer-master/rdfizer/run_rdfizer.py config-SDM.ini

