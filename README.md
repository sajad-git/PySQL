## PySQL

<p align="center">
PySQL is a Python library for interacting with SQL Server databases. It provides a simplified interface for common database operations.
</p>

<p align="center">
  <img src="https://github.com/sajad-git/PySQL/blob/crawler/readme/lugu.jpg?raw=true" alt="Sublime's custom image"/>
</p>

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
Import pySQL and :

```python
from pySQL import PySQL, Table_analyzer
```
# Table_analyzer usage:
A helper class for analyzing Pandas DataFrames to determine optimal SQL datatypes.

```python
df = pd.read_csv('test.csv')
TA = Table_analyzer()
my_dict = TA.analyze(df,texts_buffer=0.2)
```
> + for texts_buffer > 1 :  N in nvarchar(N) sets to len_max_length + texts_buffer
> + for 0 < texts_buffer < 1 :  N in nvarchar(N) sets to len_max_length + texts_buffer*len_max_length (extra percentage)

# PySQL usage:

1. Create a connection
```python
pysql = PySQL()
pysql.create_connection(server='host_ip', database='mydb', username='myuser', password='mypassword')
```

2. at first use (for every tables) you must call create_dtypes()
```python
dtype_dict = {'col1':sql.sqltypes.INTEGER , 'col2':sql.sqltypes.NVARCHAR(100)} # it's suggested to use Table_analyzer to calculate optimal dtype_dict
pysql.create_dtypes(dtype_dict=dtype_dict, table_name='Test_table', schema='Test_schema')
```

3. at all next usages you must call load_dtypes and next you can use to_sql method to send data
```python
pysql.load_dtypes(table_name='Test_table', schema='Test_schema')    # created before
pysql.to_sql(df,'Test_table', schema='Test_schema', if_exists='append', text_cutter=True, date_normalizer=True)
```


