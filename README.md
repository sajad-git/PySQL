## PySQL

<p align="center">
PySQL is a Python library for interacting with SQL Server databases. It provides a simplified interface for common database operations.
</p>

<p align="center">
  <img src="https://github.com/sajad-git/PySQL/blob/crawler/readme/lugu.jpg?raw=true" alt="Sublime's custom image"/>
</p>
<br>
# Installation
pySQL requires the following packages:

- pandas
- numpy
- sqlalchemy
- pyodbc
  
```python
pip install pandas numpy sqlalchemy pyodbc
```

# Usage
Import PySQL and :

```python
from pySQL import PySQL, Table_analyzer
```
# Table_analyzer usage:
A helper class for analyzing Pandas DataFrames to determine optimal SQL datatypes.

```python
df = pd.read_csv('test.csv')
TA = Table_analyzer()
dtype_dict = TA.analyze(df,texts_buffer=0.2)
```
> + for texts_buffer > 1 :  N in nvarchar(N) sets to len_max_length + texts_buffer
> + for 0 < texts_buffer < 1 :  N in nvarchar(N) sets to len_max_length + texts_buffer*len_max_length (extra percentage)

# PySQL usage:

+ Create a connection
```python
pysql = PySQL()
pysql.create_connection(server='host_ip', database='mydb', username='myuser', password='mypassword')
```
<br>

+ at first use (for every tables) you must call create_dtypes()
```python
dtype_dict = {'col1':sql.sqltypes.INTEGER , 'col2':sql.sqltypes.NVARCHAR(100)} # it's suggested to use Table_analyzer to calculate optimal dtype_dict
# dtype_dict = TA.analyze(df,texts_buffer=0.2) 
pysql.create_dtypes(dtype_dict=dtype_dict, table_name='Test_table', schema='Test_schema')
```
<br>

+ at all next usages you must call load_dtypes and next you can use to_sql method to send data
```python
pysql.load_dtypes(table_name='Test_table', schema='Test_schema')    # created before
pysql.to_sql(df,'Test_table', schema='Test_schema', if_exists='append', text_cutter=True, date_normalizer=True)
```
> + you can use primary_key='column_name' to set tables primary_key
> + in next usages it's not allowed to use this
> + 'text_cutter' trys to cut new text if those length was taller than column capacity
> + 'date_normalizer' trys to make date format colums suitable for sql server

+ and there is some read data methods in order to read data from your database (returns pandas dataframe)
```python
pysql.tables_list(schema=None)
pysql.read_sql_table(table_name, schema=None)
pysql.read_sql_query(query='SELECT * FROM TABLE_NAME')
```

<br>

+ logger is a common method to use ( it logs datetime and process_id and actor_user in order to make your actions trackable )
> every to_sql calls can join with logs with 'process_id' column , so you can find who and when it's started to store data and how long takes it process
```python
pysql.logger('create_connection', 'success', 'connected')
```
+ auto log system logs all your method calls  like bellow sample:

> auto_log sample:
<img src="https://github.com/sajad-git/PySQL/blob/crawler/readme/log_sample.PNG?raw=true" alt="Sublime's custom image"/>

> data to_sql sample:
<img src="https://github.com/sajad-git/PySQL/blob/crawler/readme/to_sql_sqmple.PNG?raw=true" alt="Sublime's custom image"/>



