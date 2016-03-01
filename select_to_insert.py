#Checking the data type of the columns is remaining
#if the data type matches CLOB then a dufferent functionality has to implemented

import cx_Oracle
import re

def get_table(query):
	"""Takes a SQL query as input and return the corresponding table name"""
	table_name = re.match(r'(.*) from (\w+).*', query, re.M|re.I)
	
	print "The table name is : \t", table_name.group(2)
	return table_name.group(2)
	
def get_column(cur):
	"""Input : cursor
	   Output : Column names and their data types in a tuple
	   Functioning :Reads the name and data type of the columns used in the select query from the describe attribute of the cursor"""
	return [ (i[0], i[1]) for i in cur.description ]
	
def gen_val(col_val):
	#item for item in col_val if "None" in item
	test_list = []
	for item in col_val:
		test_list = list(item)

		#if "None" in item:
		#get all the indices where "None" exists
		ind = [i for i, val in enumerate(test_list) if val == "None"]

		for i in ind:
			test_list[i] = "NULL"
		
	
def select_to_insert(list_of_dict_form_of_rows, table):
	"""The function takes list_of_dict_form_of_rows as input and generates the insert query by reading the 
	corresponding column name and column value pair"""
	
	
##############################################################################
#
#								MAIN
#
##############################################################################

def main():
	#Get the select query from the user
	query = raw_input("Enter the select query:\t")
	
	#get the table name from the query
	table = get_table(query)
	
	con=cx_Oracle.connect(connection-string)
	
	cur = con.cursor()
	
	cur.execute(query)
	
	#get the names of the columns and their data types
	#desc_col = describe columns
	#desc_col is a list of tuple (column_name, column_type)
	desc_col = get_column(cur)
	
	print "Total no of rows fetched = ", cur.rowcount
	
	#col_val is the list of values of each column returned from select query
	col_val = [row for row in cur]
	
	values = gen_val(col_val)
	
if __name__ == "__main__" :
    main()
