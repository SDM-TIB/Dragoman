import os
import re
import csv
import sys
import rdflib
from rdflib.plugins.sparql import prepareQuery
from configparser import ConfigParser, ExtendedInterpolation
import traceback
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from mysql import connector
from .connection import *

try:
	from triples_map import TriplesMap as tm
except:
	from .triples_map import TriplesMap as tm


global jdbcDriver
jdbcDriver = ""
global jdbcDSN
jdbcDSN = ""
global temp_dics_values
temp_dics_values = {}

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
				 	OPTIONAL {?_object_map rr:termType ?term .}
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
			parent_child_list = parent_child_relation(triples_map_list)
			i = 1
			j = 1
			for triples_map in triples_map_list:
				fields = {}
				if str(triples_map.file_format).lower() == "csv" and triples_map.query == "None":
					if triples_map.function:
						pass
					else:
						subject_function = False
						if triples_map.subject_map.subject_mapping_type == "template":
							subject_field = triples_map.subject_map.value.split("{")
							if len(subject_field) == 2:
								fields[subject_field[1].split("}")[0]] = "subject"
							elif len(subject_field) == 1:
								fields[subject_field[0]] = "subject"
							else:
								for sf in subject_field:
									if "}" in sf:
										fields[sf.split("}")[0]] = "subject"
						elif triples_map.subject_map.subject_mapping_type == "function":
							subject_function = True
						for po in triples_map.predicate_object_maps_list:
							if po.object_map.mapping_type == "reference function":
								if po.object_map.term == None:
									for triples_map_element in triples_map_list:
										if triples_map_element.triples_map_id == po.object_map.value:
											if triples_map_element.triples_map_id not in function_dic:
												dic = create_dictionary(triples_map_element)
												current_func = {"output_name": dic["executes"].split("/")[len(dic["executes"].split("/"))-1] + "_output" + str(i), ## output_name: output column name in output file
																"output_file": config["datasets"]["output_folder"] + "/" + config[dataset_i]["mapping"].split("/")[len(config[dataset_i]["mapping"].split("/"))-1].split(".")[0] + "_OUTPUT" + str(i) + ".csv", ## output_file: path to the output file + file name
																"inputs":dic["inputs"], ## input parameters and cooresponding type (keys to be the parameter names and the values to be the type of the parameter)
																"function":dic["executes"], ## name of the function
																"func_par":dic, ## value of the input parameters (keys are the parameter names and the values are the input parameter value) 
																"termType":True} ## if needed or not
												no_inner_func = True
												for inputs in dic["inputs"]:
													if "reference function" in inputs:
														no_inner_func = False
												if no_inner_func:
													if po.object_map.term is not None:
														if "IRI" in po.object_map.term:
															function_dic[triples_map_element.triples_map_id] = current_func
															join_csv(triples_map.data_source, current_func, config["datasets"]["output_folder"],triples_map_list)
													else:
														current_func["termType"] = False
														function_dic[triples_map_element.triples_map_id] = current_func
														join_csv(triples_map.data_source, current_func, config["datasets"]["output_folder"],triples_map_list)
													i += 1
											for inputs in current_func["inputs"]:
												if "reference function" not in inputs and "constant" not in inputs: 
													fields[inputs[0]] = "object"
								else:
									if "Literal" not in po.object_map.term:
										for triples_map_element in triples_map_list:
											if triples_map_element.triples_map_id == po.object_map.value:
												if triples_map_element.triples_map_id not in function_dic:
													dic = create_dictionary(triples_map_element)
													current_func = {"output_name": dic["executes"].split("/")[len(dic["executes"].split("/"))-1] + "_output" + str(i), ## output_name: output column name in output file
																	"output_file": config["datasets"]["output_folder"] + "/" + config[dataset_i]["mapping"].split("/")[len(config[dataset_i]["mapping"].split("/"))-1].split(".")[0] + "_OUTPUT" + str(i) + ".csv", ## output_file: path to the output file + file name
																	"inputs":dic["inputs"], ## input parameters and cooresponding type (keys to be the parameter names and the values to be the type of the parameter)
																	"function":dic["executes"], ## name of the function
																	"func_par":dic, ## value of the input parameters (keys are the parameter names and the values are the input parameter value) 
																	"termType":True} ## if needed or not
													no_inner_func = True
													for inputs in dic["inputs"]:
														if "reference function" in inputs:
															no_inner_func = False
													if no_inner_func:
														if po.object_map.term is not None:
															if "IRI" in po.object_map.term:
																function_dic[triples_map_element.triples_map_id] = current_func
																join_csv(triples_map.data_source, current_func, config["datasets"]["output_folder"],triples_map_list)
														else:
															current_func["termType"] = False
															function_dic[triples_map_element.triples_map_id] = current_func
															join_csv(triples_map.data_source, current_func, config["datasets"]["output_folder"],triples_map_list)
														i += 1
												for inputs in current_func["inputs"]:
													if "reference function" not in inputs and "constant" not in inputs: 
														fields[inputs[0]] = "object"
							else:
								if po.object_map.mapping_type != "None" and po.object_map.mapping_type != "constant":
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
														if tp.subject_map.subject_mapping_type == "function":
															for func in triples_map_list:
																if tp.subject_map.value == func.triples_map_id:
																	temp = create_dictionary(func)
																	temp_dic = {"inputs":temp["inputs"], 
																				"function":temp["executes"],
																				"func_par":temp}
																	for attr in temp_dic["inputs"]:
																		if "reference function" not in attr[1] and "constant" not in attr[1]:
																			fields[attr[0]] = "object" 
														elif tp.subject_map.subject_mapping_type != "constant":
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
											if po.object_map.mapping_type != "template":
												fields[po.object_map.value] = "object"

						if (config["datasets"]["enrichment"].lower() == "yes" or triples_map.subject_map.subject_mapping_type == "function") and triples_map.triples_map_id not in file_projection:
							with open(config["datasets"]["output_folder"] + "/" + config[dataset_i]["mapping"].split("/")[len(config[dataset_i]["mapping"].split("/"))-1].split(".")[0] + "_PROJECT" + str(j) + ".csv", "w") as temp_csv:
								writer = csv.writer(temp_csv, quoting=csv.QUOTE_ALL) 
								temp_dics = []
								for po in triples_map.predicate_object_maps_list:
									temp_dic = {}
									if po.object_map.mapping_type == "reference function":
										for triples_map_element in triples_map_list:
											if triples_map_element.triples_map_id == po.object_map.value:
												dic = create_dictionary(triples_map_element)
												if po.object_map.term == None:
													for inputs in dic["inputs"]:
														if "reference function" in inputs:
															temp_dic = {"inputs":dic["inputs"], 
																			"function":dic["executes"],
																			"func_par":dic,
																			"id":triples_map_element.triples_map_id}
															if inner_function_exists(temp_dic, temp_dics):
																temp_dics.append(temp_dic)
												else:
													if "Literal" not in po.object_map.term:
														for inputs in dic["inputs"]:
															if "reference function" in inputs:
																temp_dic = {"inputs":dic["inputs"], 
																				"function":dic["executes"],
																				"func_par":dic,
																				"id":triples_map_element.triples_map_id}
																if inner_function_exists(temp_dic, temp_dics):
																	temp_dics.append(temp_dic)
													elif "Literal" in po.object_map.term:
														temp_dic = {"inputs":dic["inputs"], 
																	"function":dic["executes"],
																	"func_par":dic,
																	"id":triples_map_element.triples_map_id}
														if inner_function_exists(temp_dic, temp_dics):
															temp_dics.append(temp_dic)
									elif po.object_map.mapping_type == "parent triples map" and po.object_map.child == None:
										for triples_map_element in triples_map_list:
											if triples_map_element.triples_map_id == po.object_map.value:
												if triples_map_element.subject_map.subject_mapping_type == "function":
													for func in triples_map_list:
														if triples_map_element.subject_map.value == func.triples_map_id:
															dic = create_dictionary(func)
															for inputs in dic["inputs"]:
																temp_dic = {"inputs":dic["inputs"], 
																				"function":dic["executes"],
																				"func_par":dic,
																				"id":func.triples_map_id}
																if inner_function_exists(temp_dic, temp_dics):
																	temp_dics.append(temp_dic)
								if temp_dics or triples_map.subject_map.subject_mapping_type == "function":
									if triples_map.subject_map.subject_mapping_type == "function":
										for triples_map_element in triples_map_list:
											if triples_map_element.triples_map_id == triples_map.subject_map.value:
												temp = create_dictionary(triples_map_element)
												temp_dic = {"inputs":temp["inputs"], 
															"function":temp["executes"],
															"func_par":temp,
															"id":triples_map_element.triples_map_id}
												if inner_function_exists(temp_dic, temp_dics):
													temp_dics.append(temp_dic)
									reader = pd.read_csv(triples_map.data_source)
									reader = reader.where(pd.notnull(reader), None)
									reader = reader.drop_duplicates(keep='first')
									reader = reader.to_dict(orient='records')
									projection_keys = []
									for pk in fields:
										projection_keys.append(pk)
									if triples_map.triples_map_id in parent_child_list:
										for parent in parent_child_list[triples_map.triples_map_id]:
											projection_keys.append(parent)
									for temp in temp_dics:
										projection_keys.append(temp["function"].split("/")[len(temp["function"].split("/"))-1] + "_" + temp["id"])
									writer.writerow(projection_keys)
									line_values = {}
									for row in reader:
										temp_lines = {}
										k = 0
										line = []
										string_values = ""
										string_line_values = ""
										non_none = True
										for key in fields:
											line.append(row[key])
											if row[key] is None:
												pass
											else:
												string_values += str(row[key])
										if triples_map.triples_map_id in parent_child_list:
											for parent in parent_child_list[triples_map.triples_map_id]:
												line.append(row[parent])
												if row[parent] is None:
													pass
												else:
													string_values += str(row[parent])
										list_input = True
										for temp_dic in temp_dics:
											string_line_values = inner_values(row,temp_dic,triples_map_list)
											if temp_dic["func_par"]["executes"] + "_" + temp_dic["id"] not in temp_dics_values:
												value = inner_function(row,temp_dic,triples_map_list)
												if temp_lines:
													k = 0
													for temp in temp_lines:
														temp_temp_lines = {}
														if isinstance(value, list):
															for v in value:
																if non_none:
																	temp_temp_lines[k] = temp_lines[temp] + [v]
																	k += 1
														else:
															temp_temp_lines[k] =  temp_lines[temp] + [value]
															k += 1
													temp_lines = temp_temp_lines
												else:
													if isinstance(value, list):
														for v in value:
															if non_none:
																temp_lines[k] = line + [v]
																k += 1
														list_input = False
													else:
														line.append(value)
														string_values += value
												temp_dics_values[temp_dic["func_par"]["executes"] + "_" + temp_dic["id"]] = {string_line_values : value}
											else:
												if string_line_values not in temp_dics_values[temp_dic["func_par"]["executes"] + "_" + temp_dic["id"]]:
													value = inner_function(row,temp_dic,triples_map_list)
													if temp_lines:
														k = 0
														for temp in temp_lines:
															temp_temp_lines = {}
															if isinstance(value, list):
																for v in value:
																	if non_none:
																		temp_temp_lines[k] = temp_lines[temp] + [v]
																		i += 1
															else:
																temp_temp_lines[k] =  temp_lines[temp] + [value]
																k += 1
														temp_lines = temp_temp_lines
													else:
														if isinstance(value, list):
															for v in value:
																if non_none:
																	temp_lines[k] = line + [v]
																	k += 1
															list_input = False
														else:
															line.append(value)
															string_values += value
													temp_dics_values[temp_dic["func_par"]["executes"] + "_" + temp_dic["id"]][string_line_values] = value
												else:
													value = temp_dics_values[temp_dic["func_par"]["executes"] + "_" + temp_dic["id"]][string_line_values]
													if temp_lines:
														k = 0
														for temp in temp_lines:
															temp_temp_lines = {}
															if isinstance(value, list):
																for v in value:
																	if non_none:
																		temp_temp_lines[k] = temp_lines[temp] + [v]
																		k += 1
															else:
																temp_temp_lines[k] =  temp_lines[temp] + [value]
																k += 1
														temp_lines = temp_temp_lines
													else:
														if isinstance(value, list):
															for v in value:
																if non_none:
																	temp_lines[k] = line + [v]
																	k += 1
															list_input = False
														else:
															line.append(value)
															string_values += value
										if temp_lines:
											for temp in temp_lines:
												string_values = ""
												for string in temp_lines[temp]:
													string_values += string
												if string_values not in line_values:
													writer.writerow(temp_lines[temp])
													line_values[string_values] = temp_lines[temp]
										else:					
											if non_none and string_values not in line_values and list_input:
												writer.writerow(line)
												line_values[string_values] = line

									file_projection[triples_map.triples_map_id] = config["datasets"]["output_folder"] + "/" + config[dataset_i]["mapping"].split("/")[len(config[dataset_i]["mapping"].split("/"))-1].split(".")[0] + "_PROJECT" + str(j) + ".csv"
								else:
									reader = pd.read_csv(triples_map.data_source, usecols=fields.keys())
									reader = reader.where(pd.notnull(reader), None)
									reader = reader.drop_duplicates(keep='first')
									reader = reader.to_dict(orient='records')							
									writer.writerow(reader[0].keys())
									for row in reader:
										writer.writerow(row.values())	
									file_projection[triples_map.triples_map_id] = config["datasets"]["output_folder"] + "/" + config[dataset_i]["mapping"].split("/")[len(config[dataset_i]["mapping"].split("/"))-1].split(".")[0] + "_PROJECT" + str(j) + ".csv"
								
							j += 1

						else:
							if triples_map.triples_map_id not in file_projection:
								inner_functions = []
								for po in triples_map.predicate_object_maps_list:
									inner_func = {}
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
														inner_functions.append(inner_func)								
								if inner_functions:
									temp_dics = []
									for inner_func in inner_functions:
										for attr in inner_func["inputs"]:
											if ("reference function" in attr[1]):
												for triples_map_element in triples_map_list:
													if triples_map_element.triples_map_id == attr[0]:
														temp = create_dictionary(triples_map_element)
														temp_dic = {"inputs":temp["inputs"], 
																	"function":temp["executes"],
																	"func_par":temp}
														if inner_function_exists(temp_dic, temp_dics):
															temp_dics.append(temp_dic)
														

									with open(config["datasets"]["output_folder"] + "/" + config[dataset_i]["mapping"].split("/")[len(config[dataset_i]["mapping"].split("/"))-1].split(".")[0] + "_PROJECT" + str(j) + ".csv", "w") as temp_csv:
										writer = csv.writer(temp_csv, quoting=csv.QUOTE_ALL)
										reader = pd.read_csv(triples_map.data_source, usecols=fields.keys())
										reader = reader.where(pd.notnull(reader), None)
										reader = reader.to_dict(orient='records')
										projection_keys = []
										for pk in fields:
											projection_keys.append(pk)
										for temp in temp_dics:
											projection_keys.append(temp["function"].split("/")[len(temp["function"].split("/"))-1])
										writer.writerow(projection_keys)
										for row in reader:
											line = []
											non_none = True
											for key in fields:
												line.append(row[key])
												if row[key] is None:
													non_none = False
											if non_none:
												list_input = True
												for temp_dic in temp_dics:
													string_line_values = inner_values(row,temp_dic,triples_map_list)
													if temp_dic["func_par"]["executes"] + "_" + temp_dic["id"] not in temp_dics_values:
														value = inner_function(row,temp_dic,triples_map_list)
														if isinstance(value, list):
															for v in value:
																if non_none and (string_values + v) not in line_values:
																	writer.writerow(line + [v])
																	line_values[string_values + v] = line + [v]
															list_input = False
														else:
															line.append(value)
															string_values += value
														temp_dics_values[temp_dic["func_par"]["executes"] + "_" + temp_dic["id"]] = {string_line_values : value}
													else:
														if string_line_values not in temp_dics_values[temp_dic["func_par"]["executes"] + "_" + temp_dic["id"]]:
															value = inner_function(row,temp_dic,triples_map_list)
															if isinstance(value, list):
																for v in value:
																	if non_none and (string_values + v) not in line_values:
																		writer.writerow(line + [v])
																		line_values[string_values + v] = line + [v]
																list_input = False
															else:
																line.append(value)
																string_values += value
															temp_dics_values[temp_dic["func_par"]["executes"] + "_" + temp_dic["id"]][string_line_values] = value
														else:
															value = temp_dics_values[temp_dic["func_par"]["executes"] + "_" + temp_dic["id"]][string_line_values]
															if isinstance(value, list):
																for v in value:
																	if non_none and (string_values + v) not in line_values:
																		writer.writerow(line + [v])
																		line_values[string_values + v] = line + [v]
																list_input = False
															else:
																line.append(value)
																string_values += value
												if list_input:
													writer.writerow(line)
									file_projection[triples_map.triples_map_id] = config["datasets"]["output_folder"] + "/" + config["datasets"]["output_folder"] + "/" + config[dataset_i]["mapping"].split("/")[len(config[dataset_i]["mapping"].split("/"))-1].split(".")[0] + "_PROJECT" + str(j) + ".csv"
									j += 1


				elif config["datasets"]["dbType"] == "mysql":
					if triples_map.function:
						pass
					else:
						database, query_list = translate_sql(triples_map,triples_map_list)
						db = connector.connect(host = config[dataset_i]["host"], port = int(config[dataset_i]["port"]), user = config[dataset_i]["user"], password = config[dataset_i]["password"])
						cursor = db.cursor(buffered=True)

						if database != "None":
							cursor.execute("use " + database)
						else:
							if config[dataset_i]["db"].lower() != "none":
								cursor.execute("use " + config[dataset_i]["db"])

						subject_function = True
						if triples_map.subject_map.subject_mapping_type == "function":
							for triples_map_element in triples_map_list:
								if triples_map_element.triples_map_id == triples_map.subject_map.value:
									dic = create_dictionary(triples_map_element)
									current_func = {"output_name": "OUTPUT" + str(i),
													"output_file": triples_map_element.triples_map_id + "_OUTPUT" + str(i), 
													"inputs":dic["inputs"], 
													"function":dic["executes"],
													"func_par":dic,
													"termType":False}
									if config["datasets"]["enrichment"].lower() == "yes" and triples_map.triples_map_id not in file_projection:
										if triples_map.query == "None":
											for query in query_list:
												attributes = ""
												for attr in current_func["inputs"]:
													if attr[1] != "constant" and "reference function" != attr[1] and attr[1] not in query :
														attributes += "`" + attr[2] + "`, "
												query = query.replace("`" + po.object_map.value + "`",attributes[:-2])
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
												index = "CREATE index p" + str(j) + " on PROJECT" + str(j)
												index += " ("
												for attr in current_func["inputs"]:
													if attr[1] != "constant" and "reference function" != attr[1]:
														index += "`" + attr[0] + "`,"
												index = index[:-2] + "`);"
												cursor.execute(index)
												file_projection[triples_map.triples_map_id] = "PROJECT" + str(j)
												j += 1
										else:
											cursor.execute("DROP TABLE IF EXISTS PROJECT" + str(j) + ";")
											if "DISTINCT" in triples_map.query:
												fields = triples_map.query.split("DISTINCT")[1].split("FROM")[0].split(",")
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
											index = "CREATE index p" + str(j) + " on PROJECT" + str(j)
											index += " ("
											for attr in current_func["inputs"]:
												if attr[1] != "constant" and "reference function" != attr[1]:
													index += "`" + attr[0] + "`,"
											index = index[:-2] + "`);"
											cursor.execute(index)
											file_projection[triples_map.triples_map_id] = "PROJECT" + str(j)
											j += 1

									if triples_map.query == "None":	
										for query in query_list:
											attributes = ""
											for attr in current_func["inputs"]:
												if attr[1] != "constant" and "reference function" != attr[1] and attr[1] not in query :
													attributes += "`" + attr[0] + "`, "
											temp_query = query.split("FROM")
											query = temp_query[0] + ", " + attributes[:-2] + " FROM " + temp_query[1]
											cursor.execute("DROP TABLE IF EXISTS " + current_func["output_file"] + ";")
											cursor.execute(query)
											row_headers=[x[0] for x in cursor.description]
											function_dic[triples_map_element.triples_map_id] = current_func
											join_mysql(cursor, row_headers, current_func, db, triples_map_list)

									else:
										cursor.execute("DROP TABLE IF EXISTS " + current_func["output_file"] + ";")
										cursor.execute(triples_map.query)
										row_headers=[x[0] for x in cursor.description]
										function_dic[triples_map_element.triples_map_id] = current_func
										join_mysql(cursor, row_headers, current_func, db, triples_map_list)
									i += 1
							subject_function = False

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
										no_inner_func = True
										for inputs in dic["inputs"]:
											if "reference function" in inputs:
												no_inner_func = False
										if config["datasets"]["enrichment"].lower() == "yes" and triples_map.triples_map_id not in file_projection:
											if triples_map.query == "None":
												for query in query_list:
													attributes = ""
													for attr in current_func["inputs"]:
														if attr[1] != "constant" and "reference function" != attr[1] and attr[1] not in query :
															attributes += "`" + attr[2] + "`, "
													query = query.replace("`" + po.object_map.value + "`",attributes[:-2])
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
													index = "CREATE index p" + str(j) + " on PROJECT" + str(j)
													index += " ("
													for attr in current_func["inputs"]:
														if attr[1] != "constant" and "reference function" != attr[1]:
															index += "`" + attr[0] + "`,"
													index = index[:-2] + "`);"
													cursor.execute(index)
													file_projection[triples_map.triples_map_id] = "PROJECT" + str(j)
													j += 1
											else:
												cursor.execute("DROP TABLE IF EXISTS PROJECT" + str(j) + ";")
												if "DISTINCT" in triples_map.query:
													fields = triples_map.query.split("DISTINCT")[1].split("FROM")[0].split(",")
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
												index = "CREATE index p" + str(j) + " on PROJECT" + str(j)
												index += " ("
												for attr in current_func["inputs"]:
													if attr[1] != "constant" and "reference function" != attr[1]:
														index += "`" + attr[0] + "`,"
												index = index[:-2] + "`);"
												cursor.execute(index)
												file_projection[triples_map.triples_map_id] = "PROJECT" + str(j)
												j += 1

										if triples_map_element.triples_map_id not in function_dic:
											if triples_map.query == "None":	
												for query in query_list:
													attributes = ""
													for attr in current_func["inputs"]:
														if attr[1] != "constant" and "reference function" != attr[1] and attr[1] not in query :
															attributes += "`" + attr[0] + "`, "
													temp_query = query.split("FROM")
													query = temp_query[0] + ", " + attributes[:-2] + " FROM " + temp_query[1]

													cursor.execute("DROP TABLE IF EXISTS " + current_func["output_file"] + ";")
													cursor.execute(query)
													row_headers=[x[0] for x in cursor.description]
													function_dic[triples_map_element.triples_map_id] = current_func
													join_mysql(cursor, row_headers, current_func, db, triples_map_list)
		
											else:
												cursor.execute("DROP TABLE IF EXISTS " + current_func["output_file"] + ";")
												cursor.execute(triples_map.query)
												row_headers=[x[0] for x in cursor.description]
												function_dic[triples_map_element.triples_map_id] = current_func
												join_mysql(cursor, row_headers, current_func, db, triples_map_list)

											i += 1
				else:
					print("Invalid reference formulation or format")
					print("Aborting...")
					sys.exit(1)
			update_mapping(triples_map_list, function_dic, config["datasets"]["output_folder"], config[dataset_i]["mapping"],True,file_projection)


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