#!/bin/env python

import cx_Oracle
import os
import select_to_insert


__query_to_primary = """SELECT cols.column_name
						FROM all_constraints cons, all_cons_columns cols
						WHERE cols.table_name = upper('{table_name}')
						AND cons.constraint_type = 'P'
						AND cons.constraint_name = cols.constraint_name
						AND cons.owner = cols.owner
						ORDER BY cols.table_name, cols.position"""
						
def get_primary(connect_string, table):
	con = cx_Oracle.connect(connect_string)
	cursor = con.cursor()
	return cursor.execute(__query_to_primary.format(table_name=table))
	
def chk_pk_in(p_cols, desc_col):
	"""Input: set of primary columns in p_cols
				p_cols is a oracle cursor object consisting of tuples having primary keys
				and
			  set of column description in desc_col
				desc_col is a list of tuples (column_name, column_type)
	   Output: returns True if primary keys in p_cols belongs to column_name in desc_col"""
	
	result = True
	
	columns = [cols for cols, data_type in desc_col]
	
	for key in p_cols:
		if key[0] in columns:
			result = result and True
		else:
			result = result and False

	return result

def main():
	connect_string = os.environ.get("REF_ORA_USER") + '/' + os.environ.get("REF_ORA_PASS") + '@' + os.environ.get("REFC_DB_INST")
	con = cx_Oracle.connect(connect_string)
	
	cur = con.cursor()
	
	#Get the select query from the user
	query = raw_input("Enter the select query:\t")
	
	#get the table name from the query
	table = select_to_insert.get_table(query)
	
	primary_cols = get_primary(cur, table)
	
	for cols in primary_cols:
		print cols
		print type(cols)				#tuple
		
	print type(primary_cols)			#Ora select * from logical_datecur object
	
if __name__ == "__main__" :
    main()
