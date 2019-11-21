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
db.get_row(table_name, row_index=None, column_name=None, row_value=None, as_dict=False)
```




Checkout sqlitepandas_test.ipynb to see how you can use it! 
