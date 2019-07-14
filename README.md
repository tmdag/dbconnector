Python 3 MySQL 8 DB manager
==============
![VFX Pipeline](https://img.shields.io/badge/VFX%20Pipeline-2018-lightgrey.svg?style=flat)
![MySQL v8.0](https://img.shields.io/badge/MySQL-8.0-yellow.svg?style=flat)
![python v3.6](https://img.shields.io/badge/Python-3.6-blue.svg?style=flat)
![pylint Score](https://mperlet.github.io/pybadge/badges/9.58.svg)

GitHub: https://github.com/tmdag/dbconnector

## Overview
This python script is using official MySQL python connector from [MySQL connector website](https://dev.mysql.com/doc/connector-python/en/).
While official python module is using 'low level' MySQL querries, this dbmanager wraps them around to more pythonic functions.

## Installation:

```bash
$ pip install git+https://github.com/tmdag/dbconnector
```

## Config file :
dbconnector expects configuration file with your database details. Example of such config:
config.ini
```javascript
[mysql]
user = myusername
password = MySectetPassword
host = myServer
database = myDatabase
```
You can pass config location to the class as an argument.

## Connection : 

Each connection with database is made by creating class instance and requires database closure with 'close_connection()' method.

## Todo:

- [ ] Support for single file DB
- [ ] more sophisticated calls
- [ ] implement as_dict / as_list

## Example:

```python
from dbconnector import dbmanager
db_config = 'config/config.ini'
dbconnect = dbmanager.Connect(cfg=db_config)

db_tables = dbconnect.show_tables()
my_column_names = dbconnect.get_column_names("myTable")
primary_key_of_table = dbconnect.get_primary_key("myTable")
id_of_specific_entry = dbconnect.get_value_id("cameras", "cameraName", "GoPro")
raw_mysql_call = dbconnect.raw_call("SELECT shots.shotName FROM shots INNER JOIN shows ON shows.showID = shots.shows_showID WHERE shows.showName = 'MYSHOT'")

dbconnect.save() # save (commit) if there were any changes made in the database.
dbconnect.close_connection()
```