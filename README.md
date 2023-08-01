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
+ for texts_buffer > 1 : output N for nvarchar(N) sets to len_max_length + texts_buffer
+ for 0 < texts_buffer < 1 : output N for nvarchar(N) sets to len_max_length + texts_buffer*len_max_length (extra percentage)
