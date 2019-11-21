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
db.create_table("table_name", dataframe)
```





Checkout sqlitepandas_test.ipynb to see how you can use it! 
