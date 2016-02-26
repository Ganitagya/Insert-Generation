#Checking the data type of the columns is remaining
#if the data type matches CLOB then a dufferent functionality has to implemented

import cx_Oracle
import re


def list_of_dict_form_of_rows(cur):
	"""create a list of dictionary such that 
		each item of the list is a dictionary of the form { "col1_name" : col1_data }
		each item of the list is actually a row of the table only thing that each cell comes in a col name value pair"""
		
	#get all the column names of the table in a list
	#the first element of the description attribute of the cursor is the column name
	
	column = [i[0] for i in cur.description ]
	
	list_of_dict_form_of_rows = []
	
	for each_row in cur:
		row_dict = dict()
				
		for each_col in column:
			row_dict[each_col] = each_row[column.index(each_col)]
			
		list_of_dict_form_of_rows.append(row_dict)
		
	return list_of_dict_form_of_rows

	
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
	
##################################################################################################################
#
#								MAIN
#
##################################################################################################################

def main():
	query = raw_input("Enter the select query:\t")
	table_name = re.match(r'(.*) from (\w+).*', query, re.M|re.I)

	print "The table name is : \t", table_name.group(2)

	con=cx_Oracle.connect("KOTDB20/KOTDB20@KOTABP1")

	cur = con.cursor()

	#cur.execute(__sel_from_table,)
	cur.execute(query)

	result = list_of_dict_form_of_rows(cur)

	table = table_name.group(2)

	insert_query = select_to_insert(result, table)

	print "Total no of rows fetched = ", cur.rowcount
	print insert_query

if __name__ == "__main__" :
	main()
