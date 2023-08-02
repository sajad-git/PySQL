import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, types, MetaData, Table, select, delete, and_, Column, PrimaryKeyConstraint
from sqlalchemy.exc import NoSuchTableError, NoSuchColumnError
from sqlalchemy import schema as sqlalchemy_schema
import numpy as np
import uuid
from ast import literal_eval
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

class Table_analyzer():
    def __init__(self):
        pass
    
    def analyze_column_dtype(self, df, column_name):
        """
         analyzing dtypes for storing data in SQL Server.

        Args:
            df (pandas dataframe): A dataframe which its column dtypes needs to be analyzed.
            columnName (str): the column you needs to be analyzed.
        Returns:
            str: column's dtype - BIT/INT/FLOAT/DATE/TEXT 
        """
        def date_has_time(column_data):
            for date in column_data:
                if date.hour != 0:
                    return True
            return False

        def can_convert_to_datetime(column_data):
            try:
                # Attempt to convert the cleaned column data to datetime
                column_data_len = len(column_data)
                column_data = column_data.sample(n=100) if column_data_len > 100 else column_data.sample(n=column_data_len)
                column_data = pd.to_datetime(column_data)
                if date_has_time(column_data):
                    return "DATETIME"
                return "DATE"
            except ValueError:
                return False
            
        column_data = df[column_name]
        non_null_values = column_data.dropna()

        # Check if the column contains only boolean values (including different representations)
        is_bool = non_null_values.apply(lambda x: str(x).lower() in ['true', 'false', '1', '0'])
        if is_bool.all():
            return "BIT"
        
        # Check if the column contains all integer values with null cells
        if non_null_values.dtype == np.int64:
            if column_data.isnull().any():
                return "INT"

        # Check if the column contains integers or floats that can be considered integers
        try:
            if non_null_values.apply(lambda x: x == int(x)).all():
                if max(non_null_values) > 2_000_000_000:
                    return "BIGINT"
                return "INT"
            return "FLOAT"
        except:
            pass

        # Check if the column contains datetime values
        can_be_dtime = can_convert_to_datetime(non_null_values)
        if can_be_dtime:
            return can_be_dtime

        # If none of the above, return TEXT
        return "TEXT"

    def calculate_n_value(self, df, column_name, buffer=20, max_length=4000):
        """
        Calculate the suitable n value for storing nvarchar data in SQL Server.

        Args:
            df (pandas dataframe): A dataframe with text column which the n value needs to be calculated.
            columnName (str): the column what its n needs to be calculated.
            buffer (int/float, optional): A buffer to account for future growth/ (under 1 --> percentage of growth) (default is 20).
            max_length (int, optional): The maximum length allowed for nvarchar columns in SQL Server (default is 4000).

        Returns:
            int: The calculated n value.
        """
        column_data = df[column_name]
        non_null_values = column_data.dropna()

        # Calculate the maximum length of the texts in the list
        max_text_length = max(len(text.encode()) for text in non_null_values)

        # Calculate the suitable n value considering the buffer and the max_length constraint
        if 0 <= buffer < 1:
            n_value = int(min(max_text_length + buffer*max_text_length, max_length))
            n_value = n_value if n_value < 4000 else 4000
        else:        
            n_value = min(max_text_length + buffer, max_length)
            n_value = n_value if n_value < 4000 else 4000
            
        return n_value
    
    def analyze(self, df, texts_buffer=20, texts_max_length=4000, pre_analysed_dict={}):
        """
        analyzing for optim dtypes to store data in SQL Server.

        Args:
            df (pandas dataframe): A dataframe needs to be analyzed.
            texts_buffer (int/float, optional): A buffer to account for future growth/ (under 1 --> percentage of growth) (default is 20).
            texts_max_length (int, optional): The maximum length allowed for nvarchar columns in SQL Server (default is 4000).
            pre_analysed_dict (dict): manually created columns dtype / sample --> {'column_name' : sqlalchemy.types.VARCHAR(length=25)}

        Returns:
            dict: analysed_dict (input param for pysql.to_sql)
        """
        for column in df.columns:
            if column not in pre_analysed_dict.keys():
                detected_type = self.analyze_column_dtype(df, column)
                if detected_type == 'TEXT':
                    n_value = self.calculate_n_value(df, column, buffer=texts_buffer, max_length=texts_max_length)
                    pre_analysed_dict[column] = types.NVARCHAR(length=n_value)
                elif detected_type == 'INT':
                    pre_analysed_dict[column] = types.INT()
                elif detected_type == 'BIGINT':
                    pre_analysed_dict[column] = types.BIGINT()
                elif detected_type == 'BIT':
                    pre_analysed_dict[column] = types.Boolean()
                elif detected_type == 'FLOAT':
                    pre_analysed_dict[column] = types.FLOAT()
                elif detected_type == 'DATE':
                    pre_analysed_dict[column] = types.DATE()
                elif detected_type == 'DATETIME':
                    pre_analysed_dict[column] = types.DATETIME()
        return pre_analysed_dict


class PySQL():
    def __init__(self):
        self.log_dtypes =  {'function':types.VARCHAR(50), 'state':types.VARCHAR(50), 'log':types.VARCHAR(2000), 'connection_user':types.VARCHAR(50), 'process_id':types.VARCHAR(15), 'datetime':types.DATETIME()}
        self.process_id = uuid.uuid4().hex[:15]
        self.dtypes = {}
        
    def _log_decorator(func):  
        def wrapper(self, *args, **kwargs):
            try:
                self.process_id = uuid.uuid4().hex[:15]
                self.logger(func.__name__ , 'start', 'success')
                func_result = func(self, *args, **kwargs)
                self.logger(func.__name__ , 'end', 'success')
                return func_result
            except Exception as Error:
                print(Error)
                self.logger(func.__name__ , 'progress', str(Error))
        return wrapper    
    
    def logger(self, func, state, log):
        """
        basic logging system by PySQL 
        stores in 'config' schema and 'log' table
        connection_user, process_id and datetime joins via your input params automaticly

        Args:
            func (function object): an function (it's need to have __name__ variable')
            state (str): any thing but start/stop/progress is suggested
            log (str): log string
            
        Returns:
            None
        """
        if len(log)> 2000:
            log = log[:1999]
        if 'config' not in self.engine.dialect.get_schema_names(self.engine):  # check for config schema
            self.engine.execute(sqlalchemy_schema.CreateSchema('config'))
            print('config schema not exist! \n new created!')
        now = datetime.now().isoformat().replace('T', ' ').split('.')[0]
        log_data = {'function':func, 'state':state, 'log':log,'connection_user':self.username ,'process_id':self.process_id, 'datetime':now}
        self.log_data = pd.DataFrame([log_data])
        self.log_data.to_sql('log', con=self.engine, schema='config', if_exists='append', dtype=self.log_dtypes, index=False)
        
    def create_connection(self, server, database, username='', password='', port=1433):
        """
        creates a pysql.engine connection to your target database
        in order to have best experience to use, your sql_user(username) must to have datawriter, datareader and ddladmin 
        Args:
            server (str): your host server (ip)
            database (str): target database
            username (str): username 
            log (str): log string
            log (str): log string
            
        Returns:
            None
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.connection_str = f"mssql+pyodbc://{self.username}:{self.password}@{self.server}:{self.port}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server"
        self.engine = create_engine(self.connection_str)
        self.logger('create_connection', 'success', 'connected')
    
    @_log_decorator 
    def to_sql(self, df, table_name, schema=None, if_exists='append', index=True, index_label=None, primary_key=None, chunksize=1, date_normalizer=True, text_cutter=True):
        """
        Write records stored in a DataFrame to a SQL database.
    
        Parameters
        ----------
        df : DataFrame, Series
        table_name : str
            Name of SQL table.
        schema : str, optional
            Name of SQL schema in database to write to (if database flavor
            supports this). If None, use default schema (default).
        if_exists : {'fail', 'replace', 'append'}, default 'fail'
            - fail: If table exists, do nothing.
            - replace: If table exists, drop it, recreate it, and insert data.
            - append: If table exists, insert data. Create if does not exist.
        index : bool, default True
            Write DataFrame index as a column.
        index_label : str or sequence, optional
            Column label for index column(s). If None is given (default) and
            `index` is True, then the index names are used.
            A sequence should be given if the DataFrame uses MultiIndex.
        primary_key : str , optional
            it uses pysql.set_primary_key so :
            just one time allowed (if PK exists error!)
            PK must be unique in every rows   
        chunksize : int, optional
            Specify the number of rows in each batch to be written at a time.
            By default, all rows will be written at once.
        date_normalizer : bool, default True
            its run PySQL.date_normalizer and make date formats storable foe sql server
    
    
            .. versionadded:: 1.3.0
    
        **engine_kwargs
            Any additional kwargs are passed to the engine.
    
        Returns
        -------
        None or int
            Number of rows affected by to_sql. None is returned if the callable
            passed into ``method`` does not return the number of rows.
        """
        if schema!= None:
            if schema not in self.engine.dialect.get_schema_names(self.engine):  # check for schema existance
                self.engine.execute(sqlalchemy_schema.CreateSchema(schema))
        df['process_id'] = [self.process_id]*len(df)
        self.dtypes = self.dtypes | {'process_id':types.VARCHAR(15)}
        if date_normalizer:
            df = self.date_normalizer(df)
        if text_cutter:
            df = self.text_cutter(df)
        
        self.df = df
        row_number = df.to_sql(name=table_name, con=self.engine, schema=schema, if_exists=if_exists, index=index, index_label=index_label, dtype=self.dtypes, chunksize=chunksize)

        if primary_key != None:
            self.set_primary_key(table_name=table_name, schema=schema, column_name=primary_key)
        return row_number
    
    @_log_decorator
    def tables_list(self, schema=None):
        """
        Returns table list of schemas
        
        Args:
            schema (str): target schema name default=None
            
        Returns:
            list: list of table names
        """
        return self.engine.table_names(schema=schema)
    
    @_log_decorator 
    def read_sql_table(self, table_name, schema=None, index_col=None, coerce_float=True, parse_dates=None, columns=None, chunksize=None):
        """
        Read SQL database table into a DataFrame.
    
        Given a table name , returns a DataFrame.
        This function does not support DBAPI connections.
    
        Parameters
        ----------
        table_name : str
            Name of SQL table in database.
        schema : str, default None
            Name of SQL schema in database to query (if database flavor
            supports this). Uses default schema if None (default).
        index_col : str or list of str, optional, default: None
            Column(s) to set as index(MultiIndex).
        coerce_float : bool, default True
            Attempts to convert values of non-string, non-numeric objects (like
            decimal.Decimal) to floating point. Can result in loss of Precision.
        parse_dates : list or dict, default None
            - List of column names to parse as dates.
            - Dict of ``{column_name: format string}`` where format string is
              strftime compatible in case of parsing string times or is one of
              (D, s, ns, ms, us) in case of parsing integer timestamps.
            - Dict of ``{column_name: arg dict}``, where the arg dict corresponds
              to the keyword arguments of :func:`pandas.to_datetime`
              Especially useful with databases without native Datetime support,
              such as SQLite.
        columns : list, default None
            List of column names to select from SQL table.
        chunksize : int, default None
            If specified, returns an iterator where `chunksize` is the number of
            rows to include in each chunk.
    
        Returns
        -------
        DataFrame or Iterator[DataFrame]
            A SQL table is returned as two-dimensional data structure with labeled
            axes.
    
        See Also
        --------
        read_sql_query : Read SQL query into a DataFrame.
    
        Notes
        -----
        Any datetime values with time zone information will be converted to UTC.
    
        Examples
        --------
        >>> pysql.read_sql_table('table_name')
        """
        return pd.read_sql_table(table_name, con=self.engine, schema=schema, index_col=index_col, coerce_float=coerce_float, parse_dates=parse_dates, columns=columns, chunksize=chunksize)
    
    @_log_decorator
    def read_sql_query(self, query, index_col=None, coerce_float=True, params=None, parse_dates=None, chunksize=None, dtype=None):
        """
        Read SQL query into a DataFrame.
    
        Returns a DataFrame corresponding to the result set of the query
        string. Optionally provide an `index_col` parameter to use one of the
        columns as the index, otherwise default integer index will be used.
    
        Parameters
        ----------
        query : str SQL query or SQLAlchemy Selectable (select or text object)
            SQL query to be executed.
        index_col : str or list of str, optional, default: None
            Column(s) to set as index(MultiIndex).
        coerce_float : bool, default True
            Attempts to convert values of non-string, non-numeric objects (like
            decimal.Decimal) to floating point. Useful for SQL result sets.
        params : list, tuple or dict, optional, default: None
            List of parameters to pass to execute method.  The syntax used
            to pass parameters is database driver dependent. Check your
            database driver documentation for which of the five syntax styles,
            described in PEP 249's paramstyle, is supported.
            Eg. for psycopg2, uses %(name)s so use params={'name' : 'value'}.
        parse_dates : list or dict, default: None
            - List of column names to parse as dates.
            - Dict of ``{column_name: format string}`` where format string is
              strftime compatible in case of parsing string times, or is one of
              (D, s, ns, ms, us) in case of parsing integer timestamps.
            - Dict of ``{column_name: arg dict}``, where the arg dict corresponds
              to the keyword arguments of :func:`pandas.to_datetime`
              Especially useful with databases without native Datetime support,
              such as SQLite.
        chunksize : int, default None
            If specified, return an iterator where `chunksize` is the number of
            rows to include in each chunk.
        dtype : Type name or dict of columns
            Data type for data or columns. E.g. np.float64 or
            {‘a’: np.float64, ‘b’: np.int32, ‘c’: ‘Int64’}.
    
        Returns
        -------
        DataFrame or Iterator[DataFrame]
    
        See Also
        --------
        read_sql_table : Read SQL database table into a DataFrame.
    
        Notes
        -----
        Any datetime values with time zone information parsed via the `parse_dates`
        parameter will be converted to UTC.
        Examples
        --------
        >>> pysql.read_sql_query('SELECT * FROM TABLE_NAME')
        """
        return pd.read_sql_query(query, con=self.engine, index_col=index_col, coerce_float=coerce_float, params=params, parse_dates=parse_dates, chunksize=chunksize, dtype=dtype)
    
    def date_has_time(self, column_data):
        non_null_values = column_data.dropna()
        for date in non_null_values:
            if date.hour != 0:
                return True
        return False
    
    def date_normalizer(self, df):
        for key, value in self.dtypes.items():
            if 'DATE' in str(value):
                df[key] = pd.to_datetime(df[key])
                has_time = self.date_has_time(df[key])
                if has_time:
                    df[key] = df[key].apply(lambda x:x.isoformat().split('+')[0].replace('T',' '))
                else:
                    df[key] = df[key].apply(lambda x:x.isoformat().split('+')[0].split('T')[0])
        return df
    
    def cutter_n_finder(self, dtype):
        n = str(dtype).split('(')[1][:-1]
        return int(n)
            
    def text_cutter(self, df):
        for key, value in self.dtypes.items():
            if 'VARCHAR' in str(value):
                N = self.cutter_n_finder(value)
                df[key] = df[key].apply(lambda x:str(x)[:N] if len(str(x))> N else str(x))
        return df
        
    @_log_decorator
    def set_primary_key(self, table_name, schema=None, column_name=None):
        """
         set the table primary key
             -just one time allowed (if PK exists error!)
             -PK must be unique in every rows 

        Args:
            table_name (str): target table name
            schema (str): target schema name default=None
            column_name (str): column name what you want to be primary key
            
        Returns:
            None
        """
        if schema != None:
            table_name = f'{schema}.{table_name}'
            
        primary_key_dtype = str(self.dtypes[column_name])
        QUERY = f"""ALTER TABLE {table_name} alter column {column_name} {primary_key_dtype} NOT NULL"""
        self.engine.execute(QUERY)   
        QUERY = f"""ALTER TABLE {table_name}
                    ADD PRIMARY KEY ({column_name});"""
        self.engine.execute(QUERY)
        print('primary key sets on {column_name} with out any error')
        
    @_log_decorator 
    def create_dtypes(self, dtype_dict, table_name):
        def dtype_to_string(dtype_dict):
            dtype_dict_stringed = {}
            for dtype in dtype_dict.keys():
                dtype_string = 'types.' + str(dtype_dict[dtype]) if '(' in str(dtype_dict[dtype]) else 'types.'+ str(dtype_dict[dtype]) +'()'
                dtype_string.replace('BOOLEAN', 'Boolean')
                dtype_dict_stringed = dtype_dict_stringed | {dtype:dtype_string}
            return dtype_dict_stringed
        
        dtype_dict = dtype_to_string(dtype_dict)
        dtype_df = pd.DataFrame([dtype_dict])
        dtype_df_dtypes = {i:types.VARCHAR(50) for i in dtype_dict.keys()}
        dtype_df.to_sql(table_name+'_dtypes', con=self.engine, schema='config', if_exists='replace', dtype=dtype_df_dtypes, index=False)
        
    @_log_decorator   
    def load_dtypes(self, table_name):
        try:
            self.dtypes = pd.read_sql_table(table_name=table_name+'_dtypes', con=self.connection_str, schema='config')
            self.dtypes = self.dtypes.to_dict('records')[0]
            self.dtypes = {i:self.dtypes[i].replace('BOOLEAN', 'Boolean') for i in self.dtypes.keys()}
            self.dtypes = {i:eval(self.dtypes[i]) for i in self.dtypes} 
        except:
            self.Error = "can't load dtypes table from database please try run PySQL.create_dtypes() first.  using Table_analyzer is suggested :) "
            raise Exception(self.Error)
       
        
