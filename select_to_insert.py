#!/bin/env python
#Checking the data type of the columns is remaining
#if the data type matches CLOB then a dufferent functionality has to implemented

import cx_Oracle
import re
from   datetime import datetime
import time
import os
import check_primary

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
	#flag is the switch for CLOB data
	#flag = True implies CLOB data exists in the row
	flag = False
	test_list = []
	test_list = list(row)

	
	for i, cell in enumerate(test_list):
		if cell == None:
			test_list[i] = "NULL"
	
		if type(cell) is datetime:
			test_list[i] = test_list[i].strftime('%m/%d/%Y %H:%M:%S')
			test_list[i] = "TO_DATE( '" + str(test_list[i]) + "' , 'MM/DD/YYYY HH24:MI:SS')"
			
		if type(cell) is str:
			test_list[i] = "'" +  cell + "'"
			
		if type(cell) is cx_Oracle.LOB:
			#if CLOB data exists then while inserting just insert a string of zero length
			#create a file CLOB_data where the data is stored
			#from this file create the update queries
			test_list[i] = "''"
			
			file = open("CLOB_data", "w")
			file.write(str(cell))
			#The delimiter to one CLOB data is EOCLOB
			#EOCLOB = End Of Clob
			file.write("\n\nEOCLOB\n\n")
			file.close()
			flag = True
			
	return test_list, flag
	
def create_statement(cur, table, desc_col, query, connect_string):
	#p_cols are the primary columns of table
	p_cols = check_primary.get_primary(connect_string, table)

	
	#check if the primary columns have been included in the column description.
	if check_primary.chk_pk_in(p_cols, desc_col):
		file = open('insert_statement.sql' , 'w')
		file.write("--The insert for table:\t" + table + "\n\n")
		
		for row in cur:
			values, flag = gen_val(row)
			gen_insert(table, desc_col, values, file)
			
			if flag:
				print "LOB data exists"
				file.write("--The update for table:\t" + table + "\n\n")
				gen_update(table, desc_col, file)
		
		file.close()
	
		
def gen_insert(table, desc_col, values, file):
	"""Input: The column description and the values to be inserted to each column
		OutPut: generates the insert query """
	#for items in values:
	ins_query = "INSERT INTO " + table + "( "

	ins_query += ', '.join(col_name for col_name,col_data_type in desc_col)
	
	ins_query += ") VALUES ("
	
	ins_query += ', '.join(str(val) for val in values)
	
	ins_query += ") ;\n\n\n"
		
	file.write(ins_query)
	
def gen_update(table, desc_col, file):
	"""Input:	table name in table
				(column_name, column_type) in desc_col
				file descriptor to out put file 
		Output:	writes the update queries for CLOB data to the out put file"""
		
	#open the file containing the CLOB data
	print "in gen_update"
				
	

##############################################################################
#
#								MAIN
#
##############################################################################

def main():
	connect_string = os.environ.get("USER") + '/' + os.environ.get("PASS") + '@' + os.environ.get("DB_INST")
	con = cx_Oracle.connect(connect_string)
	
	cur = con.cursor()
	
	#Get the select query from the user
	query = raw_input("Enter the select query:\t")
	
	#get the table name from the query
	table = get_table(query)
	
	cur.execute(query)
	
	#get the names of the columns and their data types
	#desc_col = describe columns
	#"""desc_col is a list of tuple (column_name, column_type)"""
	desc_col = get_column(cur)

	create_statement(cur, table.upper(), desc_col, query, connect_string)
	
if __name__ == "__main__" :
    main()
