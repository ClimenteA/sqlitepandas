# sqlitepandas
The most simple SQLITE ORM for Data scientists!



### Installation

Just copy the sqlitepandas.py file where you need it.

### Usage

Importing and init the sqlite db
```
from sqlitepandas import DB
db = DB("mydata.db")
```

Create a table using a pandas dataframe
```
db.create_table(table_name, dataframe)
```
Get table from the database as a pandas dataframe or as a dict (df.to_dict("list"))
```
db.get_table(table_name, as_dict=False)
```
Append dataframe to an existing table in the database
```
db.append_table(table_name, dataframe)
```
Get row from table based on row index or column name and a row value (Similar to SELECT FROM X WHERE column = Y)  
```
db.get_row(table_name, row_index=None, column_name=None, row_value=None, as_dict=False) #checkout get_rows in the source file!
```
Add one row to the table using a one row dataframe (you can also use 'append_table' function for this)
```
db.add_row(table_name, one_row_df) #checkout add_rows in the source file!
```
Remove a row from the database based on one row dataframe or the row index
```
db.remove_row(table_name, one_row_df=None, row_index=None)
```
Update table cells giving it an dataframe (it will update cells based on ID column)
```
db.update_cells(table_name, df)
```
Delete table from database
```
db.drop_table(table_name)
```
Custom SQL queries
```
db.execute_query(sql_statement)
```

Up I listed the most commun functions I used.
Copy and run sqlitepandas_test.ipynb to see all the functions at work!
Works well with tables under 5000-10000 rows. 
