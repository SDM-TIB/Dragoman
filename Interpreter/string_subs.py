import re
import csv
import sys
import os
import urllib

def encode_char(string):
	encoded = ""
	valid_char = ["~","#","/",":"]
	for s in string:
		if s in valid_char:
			encoded += s
		elif s == "/":
			encoded += "%2F"
		else:
			encoded += urllib.parse.quote(s)
	return encoded

def string_substitution(string, pattern, row, term, ignore, iterator):

	"""
	(Private function, not accessible from outside this package)

	Takes a string and a pattern, matches the pattern against the string and perform the substitution
	in the string from the respective value in the row.

	Parameters
	----------
	string : string
		String to be matched
	triples_map_list : string
		Pattern containing a regular expression to match
	row : dictionary
		Dictionary with CSV headers as keys and fields of the row as values

	Returns
	-------
	A string with the respective substitution if the element to be subtitued is not invalid
	(i.e.: empty string, string with just spaces, just tabs or a combination of both), otherwise
	returns None
	"""
	template_references = re.finditer(pattern, string)
	new_string = string
	offset_current_substitution = 0
	if iterator != "None":
		if iterator != "$.[*]":
			temp_keys = iterator.split(".")
			for tp in temp_keys:
				if "$" != tp and tp in row:
					if "[*]" in tp:
						row = row[tp.split("[*]")[0]]
					else:
						row = row[tp]
	for reference_match in template_references:
		start, end = reference_match.span()[0], reference_match.span()[1]
		if pattern == "{(.+?)}":
			no_match = True
			if "]." in reference_match.group(1):
				temp = reference_match.group(1).split("].")
				match = temp[1]
				condition = temp[0].split("[")
				temp_value = row[condition[0]]
				if "==" in condition[1]:
					temp_condition = condition[1][2:-1].split("==")
					iterators = temp_condition[0].split(".")
					if isinstance(temp_value,list):
						for tv in temp_value:
							t_v = tv
							for cond in iterators[:-1]:
								if cond != "@":
									t_v = t_v[cond]
							if temp_condition[1][1:-1] == t_v[iterators[-1]]:
								row = t_v
								no_match = False
					else:
						for cond in iterators[-1]:
							if cond != "@":
								temp_value = temp_value[cond]
						if temp_condition[1][1:-1] == temp_value[iterators[-1]]:
							row = temp_value
							no_match = False
				elif "!=" in condition[1]:
					temp_condition = condition[1][2:-1].split("!=")
					iterators = temp_condition[0].split(".")
					match = iterators[-1]
					if isinstance(temp_value,list):
						for tv in temp_value:
							for cond in iterators[-1]:
								if cond != "@":
									temp_value = temp_value[cond]
							if temp_condition[1][1:-1] != temp_value[iterators[-1]]:
								row = t_v
								no_match = False
					else:
						for cond in iterators[-1]:
							if cond != "@":
								temp_value = temp_value[cond]
						if temp_condition[1][1:-1] != temp_value[iterators[-1]]:
							row = temp_value
							no_match = False
				if no_match:
					return None
			else:
				match = reference_match.group(1).split("[")[0]
			if "\\" in match:
				temp = match.split("{")
				match = temp[len(temp)-1]
			if "." in match:
				if match not in row.keys():
					temp_keys = match.split(".")
					match = temp_keys[len(temp_keys) - 1]
					for tp in temp_keys[:-1]:
						if tp in row:
							row = row[tp]
						else:
							return None
			if row == None:
				return None
			if match in row.keys():
				if row[match] != None and row[match] != "nan":
					if (type(row[match]).__name__) != "str" and row[match] != None:
						if (type(row[match]).__name__) == "float":
							row[match] = repr(row[match])
						else:
							row[match] = str(row[match])
					else:
						if re.match(r'^-?\d+(?:\.\d+)$', row[match]) is not None:
							row[match] = repr(float(row[match]))
					if isinstance(row[match],dict):
						print("The key " + match + " has a Json structure as a value.\n")
						print("The index needs to be indicated.\n")
						return None
					else:
						if re.search("^[\s|\t]*$", row[match]) is None:
							value = row[match]
							if "http" not in value and "http" in new_string[:start + offset_current_substitution]:
								value = encode_char(value)
							new_string = new_string[:start + offset_current_substitution] + value.strip() + new_string[ end + offset_current_substitution:]
							offset_current_substitution = offset_current_substitution + len(value) - (end - start)
							if "\\" in new_string:
								new_string = new_string.replace("\\", "")
								count = new_string.count("}")
								i = 0
								while i < count:
									new_string = "{" + new_string
									i += 1
								new_string = new_string.replace(" ", "")

						else:
							return None
				else:
					return None
			else:
				print('The attribute ' + match + ' is missing.')
				if ignore == "yes":
					return None
				print('Aborting...')
				sys.exit(1)
		elif pattern == ".+":
			match = reference_match.group(0)
			if "." in match:
				if match not in row.keys():
					temp_keys = match.split(".")
					match = temp_keys[len(temp_keys) - 1]
					for tp in temp_keys[:-1]:
						if tp in row:
							row = row[tp]
						else:
							return None
			if row == None:
				return None
			if match in row.keys():
				if (type(row[match]).__name__) != "str" and row[match] != None:
					if (type(row[match]).__name__) == "float":
						row[match] = repr(row[match])
					else:
						row[match] = str(row[match])
				if isinstance(row[match],dict):
					print("The key " + match + " has a Json structure as a value.\n")
					print("The index needs to be indicated.\n")
					return None
				else:
					if row[match] != None and row[match] != "nan":
						if re.search("^[\s|\t]*$", row[match]) is None:
							new_string = new_string[:start] + row[match].strip().replace("\"", "'") + new_string[end:]
							new_string = "\"" + new_string + "\"" if new_string[0] != "\"" and new_string[-1] != "\"" else new_string
						else:
							return None
					else:
						return None
			else:
				print('The attribute ' + match + ' is missing.')
				if ignore == "yes":
					return None
				print('Aborting...')
				sys.exit(1)
		else:
			print("Invalid pattern")
			if ignore == "yes":
				return None
			print("Aborting...")
			sys.exit(1)
	return new_string

def string_substitution_array(string, pattern, row, row_headers, term, ignore):

	"""
	(Private function, not accessible from outside this package)
	Takes a string and a pattern, matches the pattern against the string and perform the substitution
	in the string from the respective value in the row.
	Parameters
	----------
	string : string
		String to be matched
	triples_map_list : string
		Pattern containing a regular expression to match
	row : dictionary
		Dictionary with CSV headers as keys and fields of the row as values
	Returns
	-------
	A string with the respective substitution if the element to be subtitued is not invalid
	(i.e.: empty string, string with just spaces, just tabs or a combination of both), otherwise
	returns None
	"""

	template_references = re.finditer(pattern, string)
	new_string = string
	offset_current_substitution = 0
	for reference_match in template_references:
		start, end = reference_match.span()[0], reference_match.span()[1]
		if pattern == "{(.+?)}":
			match = reference_match.group(1).split("[")[0]
			if "\\" in match:
				temp = match.split("{")
				match = temp[len(temp)-1]
			if match in row_headers:
				if row[row_headers.index(match)] != None:# or row[row_headers.index(match)] != "None":
					value = row[row_headers.index(match)]
					if (type(value).__name__) != "str":
						if (type(value).__name__) != "float":
							value = str(value)
						else:
							value = str(math.ceil(value))
					else:
						if re.match(r'^-?\d+(?:\.\d+)$', value) is not None:
							value = str(math.ceil(float(value)))
					if "b\'" == value[0:2] and "\'" == value[len(value)-1]:
						value = value.replace("b\'","")
						value = value.replace("\'","")
					if re.search("^[\s|\t]*$", value) is None:
						if "http" not in value:
							value = encode_char(value)
						new_string = new_string[:start + offset_current_substitution] + value.strip() + new_string[ end + offset_current_substitution:]
						offset_current_substitution = offset_current_substitution + len(value) - (end - start)
						if "\\" in new_string:
							new_string = new_string.replace("\\", "")
							count = new_string.count("}")
							i = 0
							new_string = " " + new_string
							while i < count:
								new_string = "{" + new_string
								i += 1
							#new_string = new_string.replace(" ", "")

					else:
						return None
				else:
						return None
			else:
				print('The attribute ' + match + ' is missing.')
				if ignore == "yes":
					return None
				print('Aborting...')
				sys.exit(1)
				return
				# To-do:
				# Generate blank node when subject in csv is not a valid string (empty string, just spaces, just tabs or a combination of the last two)
				#if term == "subject":
				#	new_string = new_string[:start + offset_current_substitution] + str(uuid.uuid4()) + new_string[end + offset_current_substitution:]
				#	offset_current_substitution = offset_current_substitution + len(row[match]) - (end - start)
				#else:
				#	return None
		elif pattern == ".+":
			match = reference_match.group(0)
			if match in row_headers:
				if row[row_headers.index(match)] is not None:
					value = row[row_headers.index(match)]
					if type(value).__name__ == "date":
						value = value.strftime("%Y-%m-%d")
					elif type(value).__name__ == "datetime":
						value = value.strftime("%Y-%m-%d T%H:%M:%S")
					elif type(value).__name__ != "str":
						value = str(value)
					if "b\'" == value[0:2] and "\'" == value[len(value)-1]:
						value = value.replace("b\'","")
						value = value.replace("\'","")
					if re.search("^[\s|\t]*$", str(value)) is None:
						new_string = new_string[:start] + str(value).strip().replace("\"", "'") + new_string[end:]
						new_string = "\"" + new_string + "\"" if new_string[0] != "\"" and new_string[-1] != "\"" else new_string
					else:
						return None
				else:
					return None
			else:
				print('The attribute ' + match + ' is missing.')
				if ignore == "yes":
					return None
				print('Aborting...')
				sys.exit(1)
				return
		else:
			print("Invalid pattern")
			if ignore == "yes":
				return None
			print("Aborting...")
			sys.exit(1)

	return new_string