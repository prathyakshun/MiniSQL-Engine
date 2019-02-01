import sys
import re

class engine():

	def __init__(self):
		''' Class Initializers '''

		self.is_star_present = False
		self.is_distinct_present = False
		self.aggregate_term = None
		self.no_of_tables = 0
		self.dictionary = {}
		self.prod_table = {}
		self.join_conditions = []
		self.output_table = {}
		
							
	def displayResults(self,table):
		''' Printing the Table on the Screen '''
		if table == None:
			print ("Error: Invalid Query")
			sys.exit()
		final_arr = []
		final_arr.append(','.join(table['info']))
		for row in table['table']:
			arr = [str(x) for x in row]
			final_arr.append(','.join(arr))
		for row in final_arr:
			print (row)

	def cartesian_product2(self,table1,table2):
		''' Cartesian Product of Table1 and Table2 '''

		cols_table1 = []
		cols_table2 = []
		ret_table = []
		# Getting column fields from both the tables
		for i in range(max(len(table1['info']), len(table2['info']))):
			if i < len(table1['info']):
				if table1['info'][i].find(".") != -1:
					cols_table1.append(table1['info'][i])
				else:
					cols_table1.append(table1['name'] + '.' + table1['info'][i])
			if i < len(table2['info']):
				if table2['info'][i].find(".") != -1:
					cols_table2.append(table2['info'][i])
				else:
					cols_table2.append(table2['name'] + '.' + table2['info'][i])

		# Combining the two rows of the table into one
		for i in range(len(table1['table'])):
			for j in range(len(table2['table'])):
				ret_table.append(table1['table'][i] + table2['table'][j])

		return {"info":cols_table1+cols_table2, "table":ret_table}
	
	def select(self,tables, condition_str):
		''' Make the final output table with necessary rows after evaluating the expression '''

		# Joined table initially contains just the first table and append table name to columns
		joined_table = self.dictionary[tables[0]]
		info_arr = []
		for col in joined_table["info"]:
			info_arr.append(joined_table["name"] + "." + col)
		joined_table["info"] = info_arr

		# Replace = with == for use in eval function
		condition_str = condition_str.replace("=", "==")
		condition_str = condition_str.replace("<==", "<=")
		condition_str = condition_str.replace(">==", ">=")
		# condition_str = re.sub('(?<=[\w ])(=)(?=[\w ])', '==', condition_str)

		# Get the cartesian product of all the tables
		if (len(tables) >= 2):
			i = 1
			while (i<len(tables)):
				joined_table = self.cartesian_product2(joined_table, self.dictionary[tables[i]])
				i += 1

		ret_table = []

		# Finding if there is a join condition between two tables
		conditions = condition_str.replace(" and ", ",").replace(" or ", ",")
		conditions = conditions.split(",")											# Get the conditions in a list
		# conditions = conditions.replace('(', '').replace(')', '').split(',')
		for condition in conditions:
			info_arr = joined_table["info"]
			ret_table = []
			if bool(re.match('.*==.*[a-zA-Z]+.*', condition.strip())):				# Join if there exists a character after ==
				self.join_conditions.append((condition.strip().split('==')[0].strip(), condition.strip().split('==')[1].strip()))

		for col in joined_table['info']:
			idx = joined_table["info"].index(col)
			condition_str = condition_str.replace(col, 'row_val[' + str(idx) + ']')
		# Evaluate the expression for each row to include into the output table
		for row_val in joined_table['table']:
			try:
				if eval(condition_str):
					ret_table.append(row_val)
			except:
				print ("Error in where clause")
				sys.exit()
		return {"info":joined_table["info"], "table":ret_table}

	def solve_aggreage(self, col):
		''' Solve the aggregate and return the value '''
		
		if (len(col) == 0):					# When no set of values exist due to where condition
			return ""
		col = [float(val) for val in col]
		return {
			'sum' : sum(col),
			'avg' : sum(col)/len(col),
			'max' : max(col),
			'min' : min(col),
		}[self.aggregate_term]

	def project_query(self,table, cols):
		''' Project the required columns from the given table '''
		info_list = []
		ret_table = []
		if self.aggregate_term != None:
			# If aggreagate term is present make a table with just the aggreagate term
			info_list.append(self.aggregate_term + "(" + cols[0] + ")")
			col_val = []
			# Get the array of the corresponding column field
			for row in table['table']:
				col_idx = table["info"].index(cols[0])
				col_val.append(row[col_idx])
			ret_table.append([self.solve_aggreage(col_val)])
			cols = info_list

		else:
			if self.is_star_present == True:
				# If * is present then select all the columns and remove one of the duplicate columns of the join
				cols = [col for col in table["info"]]
				for col_pair in self.join_conditions:
					temp = []
					for col in cols:
						if (col == col_pair[1]):
							pass
						else:
							temp.append(col)
					cols = temp
			
			# Get the indices of the corresponding column in each row
			col_indices = []
			for col in cols:
				col_indices.append(table['info'].index(col))

			# Build the result table adding the required column fields
			for row in table['table']:
				result_row = []
				for i in range(len(col_indices)):
					result_row.append(row[col_indices[i]])
				ret_table.append(result_row)

			# If distinct is true then add only rows which are distinct
			if self.is_distinct_present == True:
				temp = sorted(ret_table)
				ret_table[:] = []
				for i in range(len(temp)):
					if i == 0:
						ret_table.append(temp[i])	
					elif temp[i] != temp[i-1]:
						ret_table.append(temp[i])	

		return {"info":cols, "table":ret_table}

	def check_semicolon(self, query):
		''' Check if semicolon is present at the end of the query '''

		if query[-1] != ";":
			print ("Error: Semicolon missing")
			sys.exit()

		if query[-1] == ";":
			query = query[:-1]
		return query

	def make_case_insensitive(self, query):
		''' Make the keywords case insensitive '''

		query = re.sub('select', 'select', query, flags=re.IGNORECASE)
		query = re.sub('distinct', 'distinct', query, flags=re.IGNORECASE)
		query = re.sub('from', 'from', query, flags=re.IGNORECASE)
		query = re.sub('where', 'where', query, flags=re.IGNORECASE)
		query = re.sub('and', 'and', query, flags=re.IGNORECASE)
		query = re.sub('or', 'or', query, flags=re.IGNORECASE)
		query = re.sub('min', 'min', query, flags=re.IGNORECASE)
		query = re.sub('max', 'max', query, flags=re.IGNORECASE)
		query = re.sub('avg', 'avg', query, flags=re.IGNORECASE)
		query = re.sub('sum', 'sum', query, flags=re.IGNORECASE)
		return query

	def parse_query(self,query):
		''' Parses the query and gets the final output table'''
			
		# Check if the query is of the form select ... from ...
		matches = re.match('^select.*from.*', query)
		if not matches:
			print("Given Query is Invalid")
			sys.exit()

		# Get the columns to be printed
		queries = query.split('from')[0]					# Part to the left of from
		queries = queries.replace('select',' ').strip()		# Remove select keyword from the query

		# Check if the 'distinct' keyword is present
		distinct_pos = queries.find('distinct')
		if distinct_pos != -1:
			self.is_distinct_present = True
			queries = queries.replace('distinct', ' ').strip()	# Remove distinct keyword from the query

		# Check if any aggreagate functions are present
		if re.match('^(avg)\(.*\)',queries):
			self.aggregate_term = queries.split('(')[0].strip()
			queries = queries.replace(self.aggregate_term, ' ').strip().strip('()')
		elif re.match('^(max)\(.*\)',queries):
			self.aggregate_term = queries.split('(')[0].strip()
			queries = queries.replace(self.aggregate_term, ' ').strip().strip('()')
		elif re.match('^(min)\(.*\)',queries):
			self.aggregate_term = queries.split('(')[0].strip()
			queries = queries.replace(self.aggregate_term, ' ').strip().strip('()')
		elif re.match('^(avg)\(.*\)',queries):
			self.aggregate_term = queries.split('(')[0].strip()
			queries = queries.replace(self.aggregate_term, ' ').strip().strip('()')

		# Gets a list of all column fields
		col_list = [col.strip() for col in queries.split(',')]
		# Check for * being present
		if len(col_list) == 1 and col_list[0] == '*':
			self.is_star_present = True
		if len(col_list) >= 2 and col_list[0] == '*':
			print ("Invalid Syntax: * should not couple with other field values")
			sys.exit() 
		# Only one aggregate term is allowed
		if len(col_list) >= 2 and self.aggregate_term != None:
			print("Invalid Syntax: Too many arguments present")
			sys.exit()

		# Get the table names
		tables_list = query.split('from')[1].split('where')[0].strip()
		tables_list = tables_list.split(',')
		tables_list = [table.strip() for table in tables_list]
		self.no_of_tables = len(tables_list)

		# Check if the tables exist in the database
		for i in range(len(tables_list)):
			if tables_list[i] not in self.dictionary:
				print("Error: Table does not exist.")
				sys.exit()
		
		matches = re.match('^select.*from.*where.*', query)
		if not matches:
			# No where clause
			if self.is_star_present == False:
				# Checking column validity when * is not present
				# Counting number of times a column comes to check if column comes in query without table name
				count_dict = {}
				for table in tables_list:
					for col in self.dictionary[table]["info"]:
						if col in count_dict:
							count_dict[col] += 1
						else:
							count_dict[col] = 1
				for col in col_list:
					is_col_present = 0
					for table in tables_list:
						if col.split(".")[-1] in self.dictionary[table]["info"]:
							if col.find(".") != -1:
								if col.split(".")[0] == table:
									is_col_present = 1
							else:
								if count_dict[col] == 1:
									is_col_present = 1
								else:
									is_col_present = 2
					if (is_col_present == 0):
						print ("Error: Column doesn't Exist")
						sys.exit()
					if (is_col_present == 2):
						print ("Error: Ambiguity in column specified")
						sys.exit()

			# Appending table name to column fields
			if self.is_star_present == False:
				for i in range(len(col_list)):
					if col_list[i].find(".") != -1:
						continue
					else:
						for table in tables_list:
							if col_list[i] in self.dictionary[table]['info']:
								col_list[i] = table + '.' + col_list[i]
								break


			self.output_table = self.select(tables_list, "1")
			self.output_table = self.project_query(self.output_table, col_list)
			return self.output_table

		else:
			# Where clause present
			if self.is_star_present == False:
				# Checking column validity when * is not present
				# Counting number of times a column comes to check if column comes in query without table name
				count_dict = {}
				for table in tables_list:
					for col in self.dictionary[table]["info"]:
						if col in count_dict:
							count_dict[col] += 1
						else:
							count_dict[col] = 1
				for col in col_list:
					is_col_present = 0
					for table in tables_list:
						if col.split(".")[-1] in self.dictionary[table]["info"]:
							if col.find(".") != -1:
								if col.split(".")[0] == table:
									is_col_present = 1
							else:
								if count_dict[col] == 1:
									is_col_present = 1
								else:
									is_col_present = 2
					if (is_col_present == 0):
						print ("Error: Column doesn't Exist")
						sys.exit()
					if (is_col_present == 2):
						print ("Error: Ambiguity in column specified")
						sys.exit()


			# Getting the condition column fields
			condition_str = query.split('where')[1].strip()
			remove_and = condition_str.replace(' and ', ' ')
			remove_and_or = remove_and.replace(' or ',' ')
			# Getting the condition column fields only once without any dupicates
			cond_cols_list = set(re.findall(r"[A-Za-z][\w\.]*", remove_and_or))
			cond_cols_list = list(cond_cols_list)

			# Checking column validity of condition column fields
			for col in cond_cols_list:
				is_col_present = 0
				for table in tables_list:
					if col.split(".")[-1] in self.dictionary[table]["info"]:
						if col.find(".") != -1:
							if col.split(".")[0] == table:
								is_col_present = 1
						else:
							is_col_present = 1
				if (is_col_present == 0):
					print ("Error: Condition columns specified doesn't Exist")
					sys.exit()

			# Appending table name to column fields in case of joins
			if self.is_star_present == False:
				for i in range(len(col_list)):
					if col_list[i].find(".") != -1:
						continue
					else:
						for table in tables_list:
							if col_list[i] in self.dictionary[table]['info']:
								col_list[i] = table + '.' + col_list[i]
								break

			# Appending table name to the condition column fields
			for col in cond_cols_list:
				if col.find(".") == -1:
					for table in tables_list:
						if col in self.dictionary[table]['info']:
							condition_str = re.sub('(?<=[^a-zA-Z0-9])(' + col + ')(?=[\(\)=\W])', table+"."+col, ' '+condition_str+' ').strip()

			# Join the tables, get rows satisfying the condtion and project the required columns
			self.output_table = self.select(tables_list, condition_str)
			self.output_table = self.project_query(self.output_table, col_list)
			return self.output_table


	def read_meta_data(self):
		''' Prepare the database in the dictionary '''

		fobj = open("./metadata.txt","r")
		metadata_dict = {}
		lines_list = fobj.readlines()
		flag = "none"
		table_name = ""
		for line in lines_list:
			if line.strip() == "<begin_table>":
				flag = "table"
			elif line.strip() == "<end_table>":
				flag = "none"	
			elif flag == "table":
				flag = "cols"	
				table_name = line.strip()
				self.dictionary[table_name] = {}
				self.dictionary[table_name]["info"] = []
				self.dictionary[table_name]["table"] = []
			elif flag == "cols":
				self.dictionary[table_name]["info"].append(line.strip())
				self.dictionary[table_name][line.strip()] = []

		for table in self.dictionary:
			self.dictionary[table]["name"] = table
			tobj = open(table+".csv", "r")
			table_lines = tobj.readlines()
			for line in table_lines:
				arr = []
				table_elems = line.split(",")
				for tab_val in table_elems:
					arr.append(int(tab_val.strip('"')))
				self.dictionary[table]["table"].append(arr)


# Create Engine object
Engine = engine()

# Read the metadata file for checking the structure of the table and 
# store the table elements into a data structure
Engine.read_meta_data()

# Read the query, make it case insensitive
query = sys.argv[1].strip('"').strip()	
query = Engine.make_case_insensitive(query)
query = Engine.check_semicolon(query)

# Parse the Query
try:
	final_table = Engine.parse_query(query)
except:
	print ("Error")
	sys.exit()
	
# Display the Result on the screen
Engine.displayResults(final_table)
