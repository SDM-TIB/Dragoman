import os
import re
import csv
import sys
import rdflib
from rdflib.plugins.sparql import prepareQuery
from configparser import ConfigParser, ExtendedInterpolation
import traceback
from concurrent.futures import ThreadPoolExecutor
from .functions import *
import pandas as pd
from mysql import connector

try:
	from triples_map import TriplesMap as tm
except:
	from .triples_map import TriplesMap as tm


global jdbcDriver
jdbcDriver = ""
global jdbcDSN
jdbcDSN = ""

def string_separetion(string):
	if ("{" in string) and ("[" in string):
		prefix = string.split("{")[0]
		condition = string.split("{")[1].split("}")[0]
		postfix = string.split("{")[1].split("}")[1]
		field = prefix + "*" + postfix
	elif "[" in string:
		return string, string
	else:
		return string, ""
	return string, condition

def mapping_parser(mapping_file):

	"""
	(Private function, not accessible from outside this package)

	Takes a mapping file in Turtle (.ttl) or Notation3 (.n3) format and parses it into a list of
	TriplesMap objects (refer to TriplesMap.py file)

	Parameters
	----------
	mapping_file : string
		Path to the mapping file

	Returns
	-------
	A list of TriplesMap objects containing all the parsed rules from the original mapping file
	"""

	mapping_graph = rdflib.Graph()

	try:
		mapping_graph.load(mapping_file, format='n3')
	except Exception as n3_mapping_parse_exception:
		print(n3_mapping_parse_exception)
		print('Could not parse {} as a mapping file'.format(mapping_file))
		print('Aborting...')
		sys.exit(1)

	mapping_query = """
		prefix rr: <http://www.w3.org/ns/r2rml#> 
		prefix rml: <http://semweb.mmlab.be/ns/rml#> 
		prefix ql: <http://semweb.mmlab.be/ns/ql#> 
		prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#>
		prefix fnml: <http://semweb.mmlab.be/ns/fnml#> 
		SELECT DISTINCT *
		WHERE {

	# Subject -------------------------------------------------------------------------
		
			?triples_map_id rml:logicalSource ?_source .
			OPTIONAL{?_source rml:source ?data_source .}
			OPTIONAL {?_source rml:referenceFormulation ?ref_form .}
			OPTIONAL { ?_source rml:iterator ?iterator . }
			OPTIONAL { ?_source rr:tableName ?tablename .}
			OPTIONAL { ?_source rml:query ?query .}
			
			OPTIONAL {?triples_map_id rr:subjectMap ?_subject_map .}
			OPTIONAL {?_subject_map rr:template ?subject_template .}
			OPTIONAL {?_subject_map rml:reference ?subject_reference .}
			OPTIONAL {?_subject_map rr:constant ?subject_constant}
			OPTIONAL { ?_subject_map rr:class ?rdf_class . }
			OPTIONAL { ?_subject_map rr:termType ?termtype . }
			OPTIONAL { ?_subject_map rr:graph ?graph . }
			OPTIONAL { ?_subject_map rr:graphMap ?_graph_structure .
					   ?_graph_structure rr:constant ?graph . }
			OPTIONAL { ?_subject_map rr:graphMap ?_graph_structure .
					   ?_graph_structure rr:template ?graph . }
		   	OPTIONAL {?_subject_map fnml:functionValue ?subject_function .}		   

	# Predicate -----------------------------------------------------------------------
			OPTIONAL {
			?triples_map_id rr:predicateObjectMap ?_predicate_object_map .
			
			OPTIONAL {
				?triples_map_id rr:predicateObjectMap ?_predicate_object_map .
				?_predicate_object_map rr:predicateMap ?_predicate_map .
				?_predicate_map rr:constant ?predicate_constant .
			}
			OPTIONAL {
				?_predicate_object_map rr:predicateMap ?_predicate_map .
				?_predicate_map rr:template ?predicate_template .
			}
			OPTIONAL {
				?_predicate_object_map rr:predicateMap ?_predicate_map .
				?_predicate_map rml:reference ?predicate_reference .
			}
			OPTIONAL {
				?_predicate_object_map rr:predicate ?predicate_constant_shortcut .
			 }
			

	# Object --------------------------------------------------------------------------
			OPTIONAL {
				?_predicate_object_map rr:objectMap ?_object_map .
				?_object_map rr:constant ?object_constant .
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
			}
			OPTIONAL {
				?_predicate_object_map rr:objectMap ?_object_map .
				?_object_map rr:template ?object_template .
				OPTIONAL {?_object_map rr:termType ?term .}
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
			}
			OPTIONAL {
				?_predicate_object_map rr:objectMap ?_object_map .
				?_object_map rml:reference ?object_reference .
				OPTIONAL { ?_object_map rr:language ?language .}
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
			}
			OPTIONAL {
				?_predicate_object_map rr:objectMap ?_object_map .
				?_object_map rr:parentTriplesMap ?object_parent_triples_map .
				OPTIONAL {
					?_object_map rr:joinCondition ?join_condition .
					?join_condition rr:child ?child_value;
								 rr:parent ?parent_value.
				 	OPTIONAL{?parent_value fnml:functionValue ?parent_function.}
				 	OPTIONAL{?child_value fnml:functionValue ?child_function.}
				}
				OPTIONAL {
					?_object_map rr:joinCondition ?join_condition .
					?join_condition rr:child ?child_value;
								 rr:parent ?parent_value;
				}
			}
			OPTIONAL {
				?_predicate_object_map rr:object ?object_constant_shortcut .
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
			}
			OPTIONAL{
				?_predicate_object_map rr:objectMap ?_object_map .
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
				?_object_map fnml:functionValue ?function .
				OPTIONAL {?_object_map rr:termType ?term .}
				
			}
			}
			OPTIONAL {
				?_source a d2rq:Database;
  				d2rq:jdbcDSN ?jdbcDSN; 
  				d2rq:jdbcDriver ?jdbcDriver; 
			    d2rq:username ?user;
			    d2rq:password ?password .
			}
			
		} """

	mapping_query_results = mapping_graph.query(mapping_query)
	triples_map_list = []


	for result_triples_map in mapping_query_results:
		triples_map_exists = False
		for triples_map in triples_map_list:
			triples_map_exists = triples_map_exists or (str(triples_map.triples_map_id) == str(result_triples_map.triples_map_id))
		
		subject_map = None
		if result_triples_map.jdbcDSN is not None:
			jdbcDSN = result_triples_map.jdbcDSN
			jdbcDriver = result_triples_map.jdbcDriver
		if not triples_map_exists:
			if result_triples_map.subject_template is not None:
				if result_triples_map.rdf_class is None:
					reference, condition = string_separetion(str(result_triples_map.subject_template))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_template), condition, "template", result_triples_map.rdf_class, result_triples_map.termtype, result_triples_map.graph)
				else:
					reference, condition = string_separetion(str(result_triples_map.subject_template))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_template), condition, "template", str(result_triples_map.rdf_class), result_triples_map.termtype, result_triples_map.graph)
			elif result_triples_map.subject_reference is not None:
				if result_triples_map.rdf_class is None:
					reference, condition = string_separetion(str(result_triples_map.subject_reference))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_reference), condition, "reference", result_triples_map.rdf_class, result_triples_map.termtype, result_triples_map.graph)
				else:
					reference, condition = string_separetion(str(result_triples_map.subject_reference))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_reference), condition, "reference", str(result_triples_map.rdf_class), result_triples_map.termtype, result_triples_map.graph)
			elif result_triples_map.subject_constant is not None:
				if result_triples_map.rdf_class is None:
					reference, condition = string_separetion(str(result_triples_map.subject_constant))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_constant), condition, "constant", result_triples_map.rdf_class, result_triples_map.termtype, result_triples_map.graph)
				else:
					reference, condition = string_separetion(str(result_triples_map.subject_constant))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_constant), condition, "constant", str(result_triples_map.rdf_class), result_triples_map.termtype, result_triples_map.graph)
			elif result_triples_map.subject_function is not None:
				if result_triples_map.rdf_class is None:
					reference, condition = string_separetion(str(result_triples_map.subject_constant))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_function), condition, "function", result_triples_map.rdf_class, result_triples_map.termtype, result_triples_map.graph)
				else:
					reference, condition = string_separetion(str(result_triples_map.subject_constant))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_function), condition, "function", str(result_triples_map.rdf_class), result_triples_map.termtype, result_triples_map.graph)
				
			mapping_query_prepared = prepareQuery(mapping_query)


			mapping_query_prepared_results = mapping_graph.query(mapping_query_prepared, initBindings={'triples_map_id': result_triples_map.triples_map_id})




			predicate_object_maps_list = []

			function = False
			for result_predicate_object_map in mapping_query_prepared_results:

				if result_predicate_object_map.predicate_constant is not None:
					predicate_map = tm.PredicateMap("constant", str(result_predicate_object_map.predicate_constant), "")
				elif result_predicate_object_map.predicate_constant_shortcut is not None:
					predicate_map = tm.PredicateMap("constant shortcut", str(result_predicate_object_map.predicate_constant_shortcut), "")
				elif result_predicate_object_map.predicate_template is not None:
					template, condition = string_separetion(str(result_predicate_object_map.predicate_template))
					predicate_map = tm.PredicateMap("template", template, condition)
				elif result_predicate_object_map.predicate_reference is not None:
					reference, condition = string_separetion(str(result_predicate_object_map.predicate_reference))
					predicate_map = tm.PredicateMap("reference", reference, condition)
				else:
					predicate_map = tm.PredicateMap("None", "None", "None")

				if "execute" in predicate_map.value:
					function = True

				if result_predicate_object_map.object_constant is not None:
					object_map = tm.ObjectMap("constant", str(result_predicate_object_map.object_constant), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
				elif result_predicate_object_map.object_template is not None:
					object_map = tm.ObjectMap("template", str(result_predicate_object_map.object_template), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
				elif result_predicate_object_map.object_reference is not None:
					object_map = tm.ObjectMap("reference", str(result_predicate_object_map.object_reference), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
				elif result_predicate_object_map.object_parent_triples_map is not None:
					if (result_predicate_object_map.child_function is not None) and (result_predicate_object_map.parent_function is not None):
						object_map = tm.ObjectMap("parent triples map function", str(result_predicate_object_map.object_parent_triples_map), str(result_predicate_object_map.object_datatype), str(result_predicate_object_map.child_function), str(result_predicate_object_map.parent_function), result_predicate_object_map.term, result_predicate_object_map.language)
					elif (result_predicate_object_map.child_function is None) and (result_predicate_object_map.parent_function is not None):
						object_map = tm.ObjectMap("parent triples map parent function", str(result_predicate_object_map.object_parent_triples_map), str(result_predicate_object_map.object_datatype), str(result_predicate_object_map.child_function), str(result_predicate_object_map.parent_value), result_predicate_object_map.term, result_predicate_object_map.language)
					elif (result_predicate_object_map.child_function is not None) and (result_predicate_object_map.parent_function is None):
						object_map = tm.ObjectMap("parent triples map child function", str(result_predicate_object_map.object_parent_triples_map), str(result_predicate_object_map.object_datatype), str(result_predicate_object_map.child_value), str(result_predicate_object_map.parent_function), result_predicate_object_map.term, result_predicate_object_map.language)
					else:
						object_map = tm.ObjectMap("parent triples map", str(result_predicate_object_map.object_parent_triples_map), str(result_predicate_object_map.object_datatype), str(result_predicate_object_map.child_value), str(result_predicate_object_map.parent_value), result_predicate_object_map.term, result_predicate_object_map.language)
				elif result_predicate_object_map.object_constant_shortcut is not None:
					object_map = tm.ObjectMap("constant shortcut", str(result_predicate_object_map.object_constant_shortcut), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
				elif result_predicate_object_map.function is not None:
					object_map = tm.ObjectMap("reference function", str(result_predicate_object_map.function),str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
				else:
					object_map = tm.ObjectMap("None", "None", "None", "None", "None", "None", "None")

				predicate_object_maps_list += [tm.PredicateObjectMap(predicate_map, object_map)]

			if function:
				current_triples_map = tm.TriplesMap(str(result_triples_map.triples_map_id), str(result_triples_map.data_source), None, predicate_object_maps_list, ref_form=str(result_triples_map.ref_form), iterator=str(result_triples_map.iterator), tablename=str(result_triples_map.tablename), query=str(result_triples_map.query),function=True)
			else:
				current_triples_map = tm.TriplesMap(str(result_triples_map.triples_map_id), str(result_triples_map.data_source), subject_map, predicate_object_maps_list, ref_form=str(result_triples_map.ref_form), iterator=str(result_triples_map.iterator), tablename=str(result_triples_map.tablename), query=str(result_triples_map.query),function=False)
			triples_map_list += [current_triples_map]

	return triples_map_list


def translate(config_path):

	config = ConfigParser(interpolation=ExtendedInterpolation())
	config.read(config_path)

	if not os.path.exists(config["datasets"]["output_folder"]):
		os.mkdir(config["datasets"]["output_folder"])

	with ThreadPoolExecutor(max_workers=10) as executor:
		for dataset_number in range(int(config["datasets"]["number_of_datasets"])):
			dataset_i = "dataset" + str(int(dataset_number) + 1)
			triples_map_list = mapping_parser(config[dataset_i]["mapping"])

			print("Executing {}...".format(config[dataset_i]["name"]))
			function_dic = {}
			file_projection = {}
			i = 1
			j = 1
			for triples_map in triples_map_list:
				fields = {}
				if str(triples_map.file_format).lower() == "csv" and triples_map.query == "None":
					if triples_map.function:
						pass
					else:
						subject_field = triples_map.subject_map.value.split("{")
						if len(subject_field) == 2:
							fields[subject_field[1].split("}")[0]] = "subject"
						elif len(subject_field) == 1:
							fields[subject_field[0]] = "subject"
						else:
							for sf in subject_field:
								if "}" in sf:
									fields[sf.split("}")[0]] = "subject"
						for po in triples_map.predicate_object_maps_list:
							if po.object_map.mapping_type == "reference function":
								for triples_map_element in triples_map_list:
									if triples_map_element.triples_map_id == po.object_map.value:
										if triples_map_element.triples_map_id not in function_dic:
											dic = create_dictionary(triples_map_element)
											if po.object_map.term is not None:
												if "IRI" in po.object_map.term:
													current_func = {"output_name": dic["executes"].split("/")[len(dic["executes"].split("/"))-1] + "_output" + str(i),
																	"output_file": config["datasets"]["output_folder"] + "/OUTPUT" + str(i) + ".csv", 
																	"inputs":dic["inputs"], 
																	"function":dic["executes"],
																	"func_par":dic,
																	"termType":True}
													function_dic[triples_map_element.triples_map_id] = current_func
													join_csv_URI(triples_map.data_source, current_func, config["datasets"]["output_folder"])
											else:
												current_func = {"output_name": dic["executes"].split("/")[len(dic["executes"].split("/"))-1] + "_output"+ str(i),
																"output_file": config["datasets"]["output_folder"] + "/OUTPUT" + str(i) + ".csv", 
																"inputs":dic["inputs"], 
																"function":dic["executes"],
																"func_par":dic,
																"termType":False}
												function_dic[triples_map_element.triples_map_id] = current_func
												join_csv(triples_map.data_source, current_func, config["datasets"]["output_folder"], triples_map_list)
											i += 1
										if "variantIdentifier" in current_func["function"]:
											fields[current_func["func_par"]["column1"]] = "object"
											fields[current_func["func_par"]["column2"]] = "object"
										elif "concat3" in current_func["function"]:
											pass
										elif "concat4" in current_func["function"]:
											fields[current_func["func_par"]["value2"]] = "object"
										elif "match_pFormat" in current_func["function"]:
											fields[current_func["func_par"]["gene"]] = "object" 
										else:
											fields[current_func["func_par"]["value"]] = "object"
							else:
								if po.object_map.mapping_type != "None":
									if po.object_map.mapping_type == "parent triples map":
										if po.object_map.child is not None:
											fields[po.object_map.child] = "object"
										else:
											for tp in triples_map_list:
												if tp.triples_map_id == po.object_map.value:
													if "{" in tp.subject_map.value:
														object_field = tp.subject_map.value.split("{")
														if len(object_field) == 2:
															fields[object_field[1].split("}")[0]] = "object"
														else:
															for of in object_field:
																if "}" in of:
																	fields[of.split("}")[0]] = "object"
													else:
														fields[tp.subject_map.value] = "object"
									else:
											if "{" in po.object_map.value:
												object_field = po.object_map.value.split("{")
												if len(object_field) == 2:
													fields[object_field[1].split("}")[0]] = "object"
												else:
													for of in object_field:
														if "}" in of:
															fields[of.split("}")[0]] = "object"
											else:
												fields[po.object_map.value] = "object"

						if config["datasets"]["enrichment"].lower() == "yes":
							with open(config["datasets"]["output_folder"] + "/PROJECT" + str(j) + ".csv", "w") as temp_csv:
								writer = csv.writer(temp_csv, quoting=csv.QUOTE_ALL) 
								
								inner_func = {}
								for po in triples_map.predicate_object_maps_list:
									if po.object_map.mapping_type == "reference function":
										for triples_map_element in triples_map_list:
											if triples_map_element.triples_map_id == po.object_map.value:
												dic = create_dictionary(triples_map_element)
												for inputs in dic["inputs"]:
													if "reference function" in inputs:
														inner_func = {"inputs":dic["inputs"], 
																		"function":dic["executes"],
																		"func_par":dic}
								if inner_func:
									for attr in inner_func["inputs"]:
										if ("reference function" in attr[1]):
											for triples_map_element in triples_map_list:
												if triples_map_element.triples_map_id == attr[0]:
													temp_dic = create_dictionary(triples_map_element)
													break
											break
									reader = pd.read_csv(triples_map.data_source)
									reader = reader.where(pd.notnull(reader), None)
									reader = reader.drop_duplicates(keep='first')
									reader = reader.to_dict(orient='records')
									projection_keys = []
									for pk in fields:
										projection_keys.append(pk)
									projection_keys.append(temp_dic["executes"].split("/")[len(temp_dic["executes"].split("/"))-1])
									writer.writerow(projection_keys)
									line_values = {}
									for row in reader:
										line = []
										string_values = ""
										non_none = True
										for key in fields:
											line.append(row[key])
											if row[key] is None:
												non_none = False
											else:
												string_values += str(row[key])
										if non_none and string_values not in line_values:
											line.append(inner_function(row,inner_func,triples_map_list))
											writer.writerow(line)
											line_values[string_values] = line	
									file_projection[triples_map.triples_map_id] = config["datasets"]["output_folder"] + "/PROJECT" + str(j) + ".csv"
								else:
									reader = pd.read_csv(triples_map.data_source, usecols=fields.keys())
									reader = reader.where(pd.notnull(reader), None)
									reader = reader.drop_duplicates(keep='first')
									reader = reader.to_dict(orient='records')							
									writer.writerow(reader[0].keys())
									for row in reader:
										writer.writerow(row.values())	
									file_projection[triples_map.triples_map_id] = config["datasets"]["output_folder"] + "/PROJECT" + str(j) + ".csv"
								
							j += 1

						else:
							inner_func = {}
							for po in triples_map.predicate_object_maps_list:
								if po.object_map.mapping_type == "reference function":
									for triples_map_element in triples_map_list:
										if triples_map_element.triples_map_id == po.object_map.value:
											dic = create_dictionary(triples_map_element)
											for inputs in dic["inputs"]:
												if "reference function" in inputs:
													inner_func = {"inputs":dic["inputs"], 
																	"function":dic["executes"],
																	"func_par":dic,
																	"id":triples_map_element.triples_map_id}
							if inner_func:
								print("There is a function that uses another function as an input.")
								print("Please use the yes enrichment option.")
								print("Aborting...")
								sys.exit(1)

				elif config["datasets"]["dbType"] == "mysql":
					if triples_map.function:
						pass
					else:
						database, query_list = translate_sql(triples_map)
						db = connector.connect(host = config[dataset_i]["host"], port = int(config[dataset_i]["port"]), user = config[dataset_i]["user"], password = config[dataset_i]["password"])
						cursor = db.cursor(buffered=True)

						if database != "None":
							cursor.execute("use " + database)
						else:
							if config[dataset_i]["db"].lower() != "none":
								cursor.execute("use " + config[dataset_i]["db"])
						
						for po in triples_map.predicate_object_maps_list:
							if po.object_map.mapping_type == "reference function":
								for triples_map_element in triples_map_list:
									if triples_map_element.triples_map_id == po.object_map.value:
										dic = create_dictionary(triples_map_element)
										current_func = {"output_name": "OUTPUT" + str(i),
														"output_file": triples_map_element.triples_map_id + "_OUTPUT" + str(i), 
														"inputs":dic["inputs"], 
														"function":dic["executes"],
														"func_par":dic,
														"termType":False}
										if config["datasets"]["enrichment"].lower() == "yes":
											if triples_map.query == "None":
												for query in query_list:

													if "variantIdentifier" in current_func["function"]:
														if current_func["func_par"]["column1"] not in query and current_func["func_par"]["column2"] in query:
															query = query.replace("`" + po.object_map.value + "`","`" + current_func["func_par"]["column1"] + "`")
														elif current_func["func_par"]["column1"] in query and current_func["func_par"]["column2"] not in query:
															query = query.replace("`" + po.object_map.value + "`","`" + current_func["func_par"]["column2"] + "`")
														elif current_func["func_par"]["column1"] not in query and current_func["func_par"]["column2"] not in query:
															query = query.replace("`" + po.object_map.value + "`","`" + current_func["func_par"]["column1"]+"`, `"+current_func["func_par"]["column2"]+ "`")
													else:
														query = query.replace("`" + po.object_map.value + "`","`" + current_func["func_par"]["value"] + "`")

													create_table = "CREATE TABLE PROJECT" + str(j) + " ("
													insert = "INSERT INTO PROJECT" + str(j) + " SELECT DISTINCT "
													fields = query.split("SELECT DISTINCT")[1].split("FROM")[0].split(",")
													for f in fields:
														create_table += f + " varchar(300),\n"
														insert += f + ", "
													create_table = create_table[:-2] + ");"
													insert = insert[:-2] + "FROM " + query.split("FROM")[1]
													cursor.execute("DROP TABLE IF EXISTS PROJECT" + str(j) + ";")
													cursor.execute(create_table)
													cursor.execute(insert)
													file_projection[triples_map.triples_map_id] = "PROJECT" + str(j)
													if "variantIdentifier" in current_func["function"]:
														index = "CREATE index p" + str(j) + " on PROJECT" + str(j)
														index += " (`" + current_func["func_par"]["column1"] + "` , `"
														index += current_func["func_par"]["column2"] + "`);" 
													else:
														index = "CREATE index p" + str(j) + " on PROJECT" + str(j)
														index += " (`" + current_func["func_par"]["column1"] + "`);"
													cursor.execute(index)
													j += 1
											else:
												cursor.execute("DROP TABLE IF EXISTS PROJECT" + str(j) + ";")
												if "DISTINCT" in triples_map.query:
													fields = triples_map.query.split("SELECT DISTINCT")[1].split("FROM")[0].split(",")
												else:
													fields = triples_map.query.split("SELECT")[1].split("FROM")[0].split(",")
												create_table = "CREATE TABLE PROJECT" + str(j) + " ( "
												insert = "INSERT INTO PROJECT" + str(j) + " SELECT DISTINCT "
												for f in fields:
													create_table += f + " varchar(300),\n"
													insert += f + ", "
												create_table = create_table[:-2] + ");"
												insert = insert[:-2] + "FROM " + triples_map.query.split("FROM")[1]
												cursor.execute(create_table)
												cursor.execute(insert)
												file_projection[triples_map.triples_map_id] = "PROJECT" + str(j)
												if "variantIdentifier" in current_func["function"]:
													index = "CREATE index p" + str(j) + " on PROJECT" + str(j)
													index += " (`" + current_func["func_par"]["column1"] + "` , `"
													index += current_func["func_par"]["column2"] + "`);" 
												else:
													index = "CREATE index p" + str(j) + " on PROJECT" + str(j)
													index += " (`" + current_func["func_par"]["column1"] + "`);"
												cursor.execute(index)
											j += 1

										if triples_map_element.triples_map_id not in function_dic:
											if triples_map.query == "None":	
												for query in query_list:
													if "variantIdentifier" in current_func["function"]:
														if current_func["func_par"]["column1"] not in query and current_func["func_par"]["column2"] in query:
															query = query.replace("`" + po.object_map.value + "`","`" + current_func["func_par"]["column1"] + "`")
														elif current_func["func_par"]["column1"] in query and current_func["func_par"]["column2"] not in query:
															query = query.replace("`" + po.object_map.value + "`","`" + current_func["func_par"]["column2"] + "`")
														elif current_func["func_par"]["column1"] not in query and current_func["func_par"]["column2"] not in query:
															query = query.replace("`" + po.object_map.value + "`","`" + current_func["func_par"]["column1"]+"`, `"+current_func["func_par"]["column2"]+ "`")
													else:
														query = query.replace("`" + po.object_map.value + "`","`" + current_func["func_par"]["value"] + "`")


													cursor.execute("DROP TABLE IF EXISTS " + current_func["output_file"] + ";")
													cursor.execute(query)
													row_headers=[x[0] for x in cursor.description]
													function_dic[triples_map_element.triples_map_id] = current_func
													join_mysql(cursor, row_headers, current_func, db)
		
											else:
												cursor.execute("DROP TABLE IF EXISTS " + current_func["output_file"] + ";")
												cursor.execute(triples_map.query)
												row_headers=[x[0] for x in cursor.description]
												function_dic[triples_map_element.triples_map_id] = current_func
												join_mysql(cursor, row_headers, current_func, db)

											i += 1
				else:
					print("Invalid reference formulation or format")
					print("Aborting...")
					sys.exit(1)
					
			if config["datasets"]["enrichment"].lower() == "yes":
				if str(triples_map.file_format).lower() == "csv" and triples_map.query == "None":
					update_mapping(triples_map_list, function_dic, config["datasets"]["output_folder"], config[dataset_i]["mapping"],True,file_projection)
				else:
					update_mapping_rdb(triples_map_list, function_dic, config["datasets"]["output_folder"], config[dataset_i]["mapping"],True,file_projection)
			else:
				if str(triples_map.file_format).lower() == "csv" and triples_map.query == "None":
					update_mapping(triples_map_list, function_dic, config["datasets"]["output_folder"], config[dataset_i]["mapping"],True,{})
				else:
					update_mapping_rdb(triples_map_list, function_dic, config["datasets"]["output_folder"], config[dataset_i]["mapping"],True,{})


			print("Successfully executed the functions in {}\n".format(config[dataset_i]["name"]))

		

def main():

	argv = sys.argv[1:]
	try:
		opts, args = getopt.getopt(argv, 'hc:', 'config_file=')
	except getopt.GetoptError:
		print('python3 translate.py -c <config_file>')
		sys.exit(1)
	for opt, arg in opts:
		if opt == '-h':
			print('python3 translate.py -c <config_file>')
			sys.exit()
		elif opt == '-c' or opt == '--config_file':
			config_path = arg

	translate(config_path)

if __name__ == "__main__":
	main()
