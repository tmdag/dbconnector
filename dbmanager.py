"""
This python script is using official MySQL python connector from MySQL connector website.
While official python module is using 'low level' MySQL querries,
this dbmanager wraps them around to more pythonic functions.
"""
import os.path
from mysql.connector import MySQLConnection, Error, errorcode
from configparser import ConfigParser
##=====================================

class Connect:
    def __init__(self, cfg='config.ini'):
        """ Connect to MySQL database """
        db_config = self.read_db_config(cfg)
        try:
            # conn = mysql.connector.connect(**db_config)
            print('Connecting to MySQL database...')
            self.conn = MySQLConnection(**db_config)
            if self.conn.is_connected():
                print('Connected to MySQL database')
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print("Error: {}".format(err))

    def show_tables(self):
        ''' show tables in current database '''
        cursor = self.conn.cursor(buffered=True)
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        cursor.close()
        return tables

    def get_primary_key(self, tablename):
        ''' gets name of primary key in a table '''
        cursor = self.conn.cursor(buffered=True)
        query = "SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE (`TABLE_NAME` = {0!r}) AND (`COLUMN_KEY` = 'PRI')".format(tablename)
        print("EXECUTING: " + query)
        cursor.execute(query)
        primary_key_name = cursor.fetchone()[0]
        cursor.close()
        return primary_key_name

    def get_column_names(self, tablename):
        ''' list column names of a table '''
        cursor = self.conn.cursor(buffered=True)
        query = "SELECT column_name FROM information_schema.columns WHERE table_name={0!r} ORDER BY ORDINAL_POSITION".format(tablename)
        print("EXECUTING: " + query)
        cursor.execute(query)
        column_names = cursor.fetchall() #list(zip(*cursor.fetchall()))[0]
        cursor.close()
        return column_names

    def get_all_rows(self, tablename):
        ''' get all rows from selected table '''
        cursor = self.conn.cursor(buffered=True)
        query = "SELECT * FROM {0:s}".format(tablename)
        print("EXECUTING: " + query)
        cursor.execute(query)
        all_rows = cursor.fetchall()
        cursor.close()
        return all_rows

    def get_rows_from_columns(self, tablename, **cols):
        ''' get row data by by specific columns '''
        cursor = self.conn.cursor(buffered=True)
        columns = cols.get("columns")
        query = "SELECT {1:s} FROM {0:s}".format(tablename, ','.join(map(str, columns)))
        print("EXECUTING: " + query)
        cursor.execute(query)
        all_rows = cursor.fetchall()
        cursor.close()
        return all_rows

    def value_exists(self, tablename, column, value):
        ''' check whenever value exists in specified table under specified column '''
        cursor = self.conn.cursor(buffered=True)
        query = "SELECT {1:s}, COUNT(*) FROM {0:s} WHERE {1:s} = {2!r} GROUP BY {1:s}".format(tablename, column, value)
        print("EXECUTING: " + query)
        cursor.execute(query)
        number_of_rows_found = cursor.rowcount
        cursor.close()
        return number_of_rows_found

    def get_value_id(self, tablename, column, value):
        ''' check whenever value exists in specified table under specified column '''
        cursor = self.conn.cursor(buffered=True)
        query = "SELECT @id:={3:s} AS id FROM {0:s} WHERE {1:s} = {2!r}".format(tablename, column, value, self.get_primary_key(tablename))
        print("EXECUTING: " + query)
        cursor.execute(query)
        get_query = cursor.fetchone()
        value_id = get_query[0] if get_query != None else None
        cursor.close()
        return value_id

    def insert_single_row(self, tablename, **colvals):
        ''' insert single row/values into table.
        requires 'columns' and 'values' as an argument '''
        cursor = self.conn.cursor(buffered=True)
        columns = colvals.get("columns")
        values = colvals.get("values")
        query = "INSERT INTO {0:s} ({1:s}) VALUES ({2:s})".format(tablename, ','.join(map(str, columns)), ','.join(map(repr, values)))
        print("EXECUTING: " + query)
        cursor.execute(query)
        cursor.close()

    def update_single_row(self, tablename, key, **colvals):
        ''' insert single row/values into table.
        requires 'columns' and 'values' as an argument '''
        cursor = self.conn.cursor(buffered=True)

        primary_key_column = self.get_primary_key(tablename)
        columns = colvals.get("columns")
        values = colvals.get("values")

        for idx, value in enumerate(columns):
            single_column = value
            single_value = values[idx]
            query = "UPDATE {0:s} SET {1:s} = {2!r} WHERE {3:s} = {4:s}".format(tablename, single_column, single_value, primary_key_column, str(key))
            print("EXECUTING: " + query)
            cursor.execute(query)
        cursor.close()

    def insert_single_value(self, tablename, column, values):
        ''' insert single values into table '''
        cursor = self.conn.cursor(buffered=True)
        query = "INSERT INTO {0:s} ({1:s}) VALUES({2!r})".format(tablename, column, values)
        print("EXECUTING: " + query)
        cursor.execute(query)
        cursor.close()

    def update_single_value(self, tablename, key, column, value):
        ''' insert single row/values into table.
        requires 'columns' and 'values' as an argument '''
        cursor = self.conn.cursor(buffered=True)

        primary_key_column = self.get_primary_key(tablename)

        query = "UPDATE {0:s} SET {1:s} = {2!r} WHERE {3:s} = {4:s}".format(tablename, column, value, primary_key_column, str(key))
        print("EXECUTING: " + query)
        cursor.execute(query)
        cursor.close()

    def remove_by_value(self, tablename, column, values):
        ''' remove row by matching value '''
        cursor = self.conn.cursor(buffered=True)
        query = "DELETE FROM {0:s} WHERE {1:s} = {2!r}".format(tablename, column, values)
        print("EXECUTING: " + query)
        cursor.execute(query)
        cursor.close()

    def save(self):
        ''' commit to database '''
        self.conn.commit()
        print('Changes Saved')

    def read_db_config(self, cfgfilename, section='mysql'):
        """ Read database configuration file and return a dictionary object
        :param filename: name of the configuration file
        :param section: section of database configuration
        :return: a dictionary of database parameters
        """
        # create parser and read ini configuration file
        parser = ConfigParser()
        parser.read(cfgfilename)

        if not os.path.isfile(cfgfilename):
            raise Exception('file {0} not found'.format(cfgfilename))
        # get section, default to mysql
        db_config_dict = {}
        if parser.has_section(section):
            items = parser.items(section)
            for item in items:
                db_config_dict[item[0]] = item[1]
        else:
            raise Exception('{0} not found in the {1} file'.format(section, cfgfilename))
        return db_config_dict

    def close_connection(self):
        ''' close connection to database '''
        self.conn.close()
        print('Connection closed.')


if __name__ == '__main__':
    dbconnect = Connect('dbconfig.ini')
    data = dbconnect.get_column_names("assetType")
    # data = dbconnect.get_primary_key("cameras")
    # data = dbconnect.get_value_id("cameras", "cameraName", "GoPro")
    print(data)
    dbconnect.close_connection()
