# Python 3 MySQL 8 DB manager
This python script is using official MySQL python connector from [MySQL connector website](https://dev.mysql.com/doc/connector-python/en/).
While official python module is using 'low level' MySQL querries, this dbmanager wraps them around to more pythonic functions.

## Installation:
Download [MySQL repo](https://dev.mysql.com/downloads/repo/yum/)
OR 
Download official python connector here: [MySQL connector download](https://dev.mysql.com/downloads/connector/python/).
Clone this repo and start using dbmanager.


## Config file :
dbconnector expects configuration file with your database details. Example of such config:
config.ini
```
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
- [x] Write Todo
- [ ] 
- [ ] 

## Example:
```
db_config = 'config/config.ini'

dbconnect = Connect(dbconfig)

db_tables = dbconnect.show_tables()
print(db_tables)

my_column_names = dbconnect.get_column_names("myTable")
print(my_column_names)

primary_key_of_table = dbconnect.get_primary_key("myTable")
print(primary_key_of_table)

dbconnect.close_connection()
```