#!/bin/env python
#Checking the data type of the columns is remaining
#if the data type matches CLOB then a dufferent functionality has to implemented

import cx_Oracle
import re
from   datetime import datetime
import time

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
	
def gen_val(row):
	#item for item in row if "None" in item

	test_list = []
	test_list = list(row)

	#if "None" in item:
	#get all the indices where "None" exists
	ind = [i for i, val in enumerate(test_list) if val == None]
	
	#Oracle uses NULL instead of None
	#and Oracle treats '' as NULL while inserting
	for i in ind:
		test_list[i] = ''
		
	del ind[:]
	
	#if datetime object in item
	#get all the indices where the object exists
	ind = [i for i, val in enumerate(test_list) if type(val) is datetime]
			
	#convert into to_date() format of Oracle
	
	for i in ind:
		test_list[i] = time.strftime('%m/%d/%Y %H:%M:%S')
		test_list[i] = "TO_DATE(" + str(test_list[i]) + ", 'MM/DD/YYYY HH24:MI:SS')"
		
	del ind[:]
	
	return test_list
		
def gen_insert(table, desc_col, values):
	"""Input: The column description and the values to be inserted to each column
		OutPut: generates the insert query """
	#for items in values:
	query = "INSERT INTO " + table + "( "
	
	#for cols in desc_col:
	#	query += str(cols[0]) + ", " 
  
	#new_query=''
	query += ', '.join(col_name for col_name,col_data_type in desc_col)
	#print "query_new",query,
	#We dont want the last two characters ' and a space :)
	#query = query[:-2]
	
	query += ") VALUES ("
	
	for i in values:
		query += str(i) + ", "
	
	#We dont want the last two characters ' and a space :)
	query = query[:-2]
	
	query += ") ;\n"
	
	print query

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
	#"""desc_col is a list of tuple (column_name, column_type)"""
	desc_col = get_column(cur)
	
	print "Total no of rows fetched = ", cur.rowcount
	
	#col_val is the list of values of each column in form of tuples returned from select query
	#it is the list of rows
	col_val = [row for row in cur]
	
	ins_query = []
	
	for q,row in enumerate(col_val):
		values = gen_val(row)
		ins_query.append(gen_insert(table, desc_col, values))
	
	for query in ins_query:
		print ins_query
	
if __name__ == "__main__" :
    main()
