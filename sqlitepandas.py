import sqlite3
import pandas as pd
pd.options.mode.chained_assignment = None #SettingWithCopyWarning
pd.set_option("display.max_columns", None)



class DB:
    """
       The super simple ORM for Data Scientists!
       It performs basic CRUD operations on SQLITE3 database.
       
       Observations: 
           - Columns must be in this format "ColName" or "col_name" (NO SPACES ALLOWED)
           - Commas ',' are NOT accepted
           - The more data you have the more slower it will be (it uses pandas for working with sqlite)
           - It will set a column 'ID' as primary key when you add a table
           - All columns (except ID) will have datatype TEXT...
       
        TODO - Implement and test in chunks (chunk_size) feature for bigger tables 
        For now it's ok if table is under 10.000 rows, if more it will get slow..
        
        
    """

    def __init__(self, database_path):
        """Require path to database as parameter"""
        self.db_path = database_path

    def get_connection(self):
        """Connect to a db and if it not exists creates one with the name given"""
        connection = sqlite3.connect(self.db_path)
        return connection


    def execute_query(self, query, keep_connection=False):
        """Execute query, commit and close query"""
        
        print(query)
        
        try:#execute query in database
            conn = self.get_connection()
            if conn == False:
                raise Exception("Connection to database failed!")
            else:
                with conn:
                    conn.execute(query)
                if keep_connection:
                    return True
                else:
                    conn.close()
                    return True
        except Exception as e:#query was not executed, Try again
            raise Exception("Failed to execute query! Got {}".format(str(e)))


    def create_table(self, table_name, df):
        """"
            Create a sqlite table with and ID INTEGER PRIMARY KEY
            If a table with that name exists replace that table with the current one
        """
        if "ID" in df:
            df.drop("ID", axis=1, inplace=True)
        self.execute_query("DROP TABLE IF EXISTS {};".format(table_name))
        sqlcolumns = ["`ID` INTEGER PRIMARY KEY"] + ["`{}` TEXT".format(col.strip()) for col in df.columns.tolist()] 
        sql_statement = "CREATE TABLE" + " `{}` ".format(table_name) + "(" + ", ".join(sqlcolumns) + ");"
        self.execute_query(sql_statement)
        self.append_table(table_name, df)
        
    
    def append_table(self, table_name, df):
        """"
            Append df to an existing table in the database
        """
        df.columns = [col.strip() for col in df.columns.tolist()]
        conn = self.get_connection()
        try:
            df.to_sql(table_name, conn, if_exists="append", index=False)
            conn.commit()
            conn.close()
        except Exception as e:
            conn.close()
            raise Exception(str(e))


    def drop_table(self, table_name):
        """Delete/remove table from database"""
        sql_statement = "DROP TABLE " + table_name
        return self.execute_query(sql_statement)
      
        
    def get_table(self, table_name, as_dict=False, chunk_size=None):
        """
           Get table from the database as a pandas dataframe 
           table_name - name of the table
           as_dict=False - option to get the df as dict('list') pandas format ex: {"col1": ['a','b','c'], "col2": ['d','f','g']}
           (NOT TESTED)chunk_size - is df is to big you can get it in chunks, where chunk_size = number of rows, it will return also the connection
           
        """
        
        query = "SELECT * FROM {}".format(table_name)
        conn = self.get_connection()
        df = pd.read_sql_query(query, conn, chunksize=chunk_size)
        
        #Set column ID as index for the df given
        df["IDX"] = df["ID"]
        df.set_index("IDX", inplace=True)
        
        if as_dict:
            df = df.to_dict("list")
        
        if chunk_size == None:
            conn.close()
            return df
        else:
            return df, conn
        
        
    def get_tables(self, table_names, as_dict=False, chunk_size=None):
        """
           Get tables from database in a dict
           table_names - names of the table - LIST
           as_dict=False - option to get the df as dict('list') pandas format ex: {"col1": ['a','b','c'], "col2": ['d','f','g']}
           (NOT TESTED)chunk_size - is df is to big you can get it in chunks, where chunk_size = number of rows, it will return also the connection
           
        """
        df_dict = {}
        for table_name in table_names:
            df_dict[table_name] = self.get_table(table_name, as_dict=as_dict, chunk_size=chunk_size)
            
        return df_dict


    def add_row(self, table_name, one_row_df):
        """
        Insert one row in the database.
        
           table_name - name of the table 
           one_row_df - a pandas dataframe with just one row
           
        """
        row = one_row_df.to_dict("list")
        
        processedInfo = {}
        for k, val in row.items():
            val = val[0]
            if isinstance(val, str):
                templi = []
                templi.append(val)
                val = [str(v) for v in templi]
                processedInfo[k] = ','.join(list(set(val)))
            else:
                try:
                    val = [str(v) for v in val]
                    processedInfo[k] = ','.join(list(set(val)))
                except:
                    processedInfo[k] = str(val)

        columns = tuple(processedInfo.keys())
        values = tuple(processedInfo.values())

        sql_insert = """INSERT INTO {} {} VALUES {};"""
        sql_statement = sql_insert.format(table_name, columns, values)
    
        return self.execute_query(sql_statement)

    
    def add_rows(self, table_name, df):
        """
            Add multiple rows to an sqlite table one by one.
            table_name - name of the table
            df - a pandas dataframe (careful on the size may take a while)
            You can just use this instead of add_row def
        """
        
        for idx in df.index.tolist():
            one_row_df = df.loc[[idx]]
            if not self.add_row(table_name, one_row_df):
                raise Exception("This row {} cannot be inserted !".format(one_row_df))


    def get_row(self, table_name, row_index=None, column_name=None, row_value=None, as_dict=False, chunk_size=None):
        """
            get_row from 'table_name' where 'column_name' has this 'row_value'
            or get row where ID = row_index
        
        """
        
        df = self.get_table(table_name, as_dict=False, chunk_size=chunk_size)
    
        if isinstance(df, tuple):
            df = df[0]
            conn = df[1]
    
        if row_index == None:
            if [column_name, row_value] != [None, None]:
                df = df[df[column_name].map(str) == str(row_value)]
            else:
                raise Exception("Parameters row_index or column_name and row_value must pe completed!")
        else:
            df = df[df["ID"].map(str) == str(row_index)]
        
        if as_dict:
            df = df.to_dict("list")
            
        if chunk_size == None:
            return df
        else:
            return df, conn

    
    def get_rows(self, table_name, row_indexes=None, column_name=None, row_values=None, as_dict=False, chunk_size=None):
        """
            get_row from 'table_name' where 'column_name' has this 'row_values' (list)
            or get row where index = row_indexes (list)
            You can just use this instead of get_row def
        """
        if not isinstance(row_indexes, list):
            if isinstance(column_name, str) and isinstance(row_values, list):
                one_row_df_list = []
                for row_value in row_values:
                    one_row_df_list.append(self.get_row(table_name=table_name, column_name=column_name, row_value=row_value, as_dict=False, chunk_size=chunk_size))
                df = pd.concat(one_row_df_list, axis=0)
        else:
            one_row_df_list = []
            for row_index in row_indexes:
                one_row_df_list.append(self.get_row(table_name=table_name, row_index=row_index, as_dict=False, chunk_size=chunk_size))
            df = pd.concat(one_row_df_list, axis=0)
            
                
        return df
        
        
    def remove_row(self, table_name, one_row_df=None, id_value=None):
        """
            Delete row or rows by maching
        """
        
        if isinstance(one_row_df, pd.DataFrame):
            
            row = one_row_df.to_dict("list")
            
            conditions_list = []
            for col, val in row.items():
                val = val[0]
                condition = "`{}`='{}'".format(col, val)
                conditions_list.append(condition)
                
            conditions = " AND ".join(list(set(conditions_list)))
            
            sql_statement = "DELETE FROM `{}` WHERE {}".format(table_name, conditions)
            
            return self.execute_query(sql_statement)
        
        elif id_value != None:
            condition = "`ID`='{}'".format(str(id_value))
            sql_statement = "DELETE FROM `{}` WHERE {}".format(table_name, condition)
            return self.execute_query(sql_statement)
       
    def remove_rows(self, table_name, df=None, id_values=None):
        """
            Remove rows from table based on the dataframe or list of ID's given
            You can just use this instead of remove_row def
        """
        
        if isinstance(df, pd.DataFrame):
            for idx in df.index.tolist():
                one_row_df = df.loc[[idx]]
                self.remove_row(table_name, one_row_df, id_value=None)
        elif isinstance(id_values, list):
            for id_value in id_values:
                self.remove_row(table_name, one_row_df=None, id_value=id_value)
        else:
            raise Exception("You must have either a df or a list with id_values")
        
        

    def update_cells(self, table_name, df):
        """
            Update cells of the table in the database 

        """
        
        columns = df.columns.tolist()
        
        for idx in df.index.tolist():
            
            where_condition = "`ID`" + '=' + "'{}'".format(str(df.loc[idx, "ID"]))

            for col in columns:
                
                if col == "ID": continue
        
                cell_new_value = "`{}`".format(col) + '=' + "'{}'".format(str(df.loc[idx, col]))
                
                sql_statement = str("UPDATE " + "`{}`".format(table_name) + " SET "  + cell_new_value  +  " WHERE " + where_condition + ";")
                if not self.execute_query(sql_statement):
                    raise Exception("Rows where not updated!")
  




          