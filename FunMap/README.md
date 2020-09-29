# FunMap: Functional Mappings for Scaled-Up Knowledge Graph Creation

We present FunMap, an interpreter of RML+FnO,that converts a data integration system defined using RML+FnO into an equivalent data integration system where RML mappings are function-free. FunMap resembles existing mapping translation proposals and empowers the  knowledge  graph  creation  process  with  optimization  techniques  to  reduce execution  time.  Transformations  of  data  sources  include  the  projection  of  the attributes used in the RML+FnO mappings. They are supported on well-known properties of the relational algebra, e.g., the pushing down of projections and selections into the data sources, and enable not only the reduction of the size of  data  sources  but  also  the  elimination  of  duplicates.  Additionally,  FunMap materializes  functions  –expressed  in  FnO–  and  represents  the  results  as  data sources of the generated data integration system; the translation of RML+FnO into RML mappings that integrate the materialization of functions is performed by means of joins between the generated RML mappings. 

![FunMap-workflow](images/architecture.png?raw=true "FunMap-workflow")


## How to run FunMap?

### Configuration file
```
[default]
main_directory: / 

[datasets]
number_of_datasets: 1
# name of dataset
name: funmap 
# path of results
output_folder: ${default:main_directory}results/ 
# yes executes FunMap, no executes FunMap- (without projections)
enrichment: yes
# only for RDB instance
dbType: mysql 

[dataset1]
name: funmap
# mapping path
mapping: ${default:main_directory}mappings/mapping.ttl 
# only for RDB
user: user 
password: pass
host: 127.0.0.1
port: 3306 
db: dbName
```

### Run with Docker 
```
# Preparation
docker build -t funmap .

# For CSV files
docker-compose up -d
# The path of the files in the mappings has to be: /data/nameOfTheFile.csv.
cp csvFiles.csv data/
cp mapping.ttl mappings/
cp config.ini funmap/

# For RDB instance
mkdir sql
cp sqlScript.sql sql/
docker-compose up -d 
cp mapping.ttl mappings/
cp config.ini funmap/

# Execution
docker exec -it funmap python3 /funmap/run_translator.py /funmap/config[_rdb].ini
```

### Run with Python3
```
pip install -r requirements.txt
python3 run_translator.py config[_rdb].ini
```

## Authors

- Samaneh Jozashoori (samaneh.jozashoori@tib.eu)
- David Chaves-Fraga (dchaves@fi.upm.es)
- Enrique Iglesias ( s6enigle@uni-bonn.de)
- Oscar Corcho (ocorcho@fi.upm.es)
- Maria-Esther Vidal (maria.vidal@tib.eu)
