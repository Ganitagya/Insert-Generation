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
	   Output : Columns names and ther data types in a tuple
	   Functioning :Reads the name and data type of the columns used in the select query from the describe attribute of the cursor"""
	return [ (i[0], i[1]) for i in cur.description ]
	

def select_to_insert(list_of_dict_form_of_rows, table):
	"""The function takes list_of_dict_form_of_rows as input and generates the insert query by reading the 
	corresponding column name and column value pair"""
	i=1
	value = "("
	#for element in list_of_dict_form_of_rows:
	#	print "\n\nROW_NUM: ",i, "\n",element
	#	i += 1
		
		
	element = list_of_dict_form_of_rows[0]
	
	for key in element:
		if key != element.keys()[-1]:
			value = value + key + ", "
		else:
			value = value + key 

	value = value + ") VALUES ( "
	
	for key in element:
		if key !=element.keys()[-1]:
			value = value + str(element[key]) + ", "
		else:
			value = value + str(element[key]) 

	
	insert_query = "INSERT INTO " + table + value	+ " );"
	
	return insert_query
	
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
	
	con=cx_Oracle.connect("KOTDB20/KOTDB20@KOTABP1")
	
	cur = con.cursor()
	
	cur.execute(query)
	
	#get the names of the columns and their data types
	#desc_col = describe columns
	desc_col = get_column(cur)
	
	for element in desc_col:
		print element
		
	input("Enter")
	
	#generate the insert out of the select query
	insert_query = select_to_insert(result, table)
	
	print "Total no of rows fetched = ", cur.rowcount
	print insert_query
	
if __name__ == "__main__" :
    main()

