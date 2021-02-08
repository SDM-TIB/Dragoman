#! /bin/bash

## Running FunMap:
python3 /mnt/e/GitHub/GenoKGC/FunMap/run_translator.py /mnt/e/GitHub/GenoKGC/FunMap/config-FunMap.ini
## Running SDM-RDFizer over the results:
python3 /mnt/e/TOOLS/SDM-RDFizer-master/rdfizer/run_rdfizer.py /mnt/e/GitHub/GenoKGC/config-SDM.ini


