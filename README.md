# Insert-Generation
Generate an insert query out of a select query

Some times it is required to copy the contents of a table in an environment to some other enviroment.
Normally there are tools to help you with that (TOAD) but the problem comes when the data type of a column is CLOB.
Normal inserts generated by any tool is not useful.
The solution proposed here is that as SQL supports only 4000 chars to be inserted at a time, create as many update statements as many as required to get the entire cell trsansferred.
