"""
This python script is using official MySQL python connector from MySQL connector website.
While official python module is using 'low level' MySQL querries,
this dbmanager wraps them around to more pythonic functions.
"""
import sys
import os.path
import logging
from configparser import ConfigParser
from mysql.connector import MySQLConnection, Error, errorcode
##=====================================

class Connect:
    """ main connect class """
    def __init__(self, cfg='config.ini', debug=True, key=None, value=None):
        """ Connect to MySQL database """
        logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

        if not debug:
            logging.disable(logging.DEBUG)

        db_config = self.read_db_config(cfg)
        self.db_name = db_config.get("database")
        self.debug = debug
        try:
            # conn = mysql.connector.connect(**db_config)
            logging.debug('Connecting to MySQL database...')
            self.conn = MySQLConnection(**db_config)
            if self.conn.is_connected():
                logging.debug('Connected to MySQL database')
        except Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logging.debug("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logging.debug("Database does not exist")
            else:
                logging.debug("Error: %s", err)
            raise Exception(err)

        self.key = None
        self.value = None

    def show_tables(self):
        ''' show tables in current database '''
        cursor = self.conn.cursor(buffered=True)
        query = "SHOW TABLES"
        logging.debug("EXECUTING: " + query)
        cursor.execute(query)
        tables = cursor.fetchall()
        cursor.close()
        tables_out = tuple(zip(*tables))[0]
        self.key = self.db_name
        self.value = tuple(zip(*tables))[0]
        # return tables_out
        return self

    def get_primary_key(self, tablename):
        ''' gets name of primary key in a table '''
        cursor = self.conn.cursor(buffered=True)
        query = "SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE (`TABLE_NAME` = {0!r}) AND (`COLUMN_KEY` = 'PRI')".format(tablename)
        logging.debug("EXECUTING: " + query)
        cursor.execute(query)
        primary_key_name = cursor.fetchone()[0]
        cursor.close()
        return primary_key_name

    def get_column_names(self, tablename):
        ''' list column names of a table '''
        cursor = self.conn.cursor(buffered=True)
        query = "SELECT column_name FROM information_schema.columns  WHERE table_schema={0!r} AND table_name={1!r} ORDER BY ORDINAL_POSITION".format(self.db_name, tablename)
        logging.debug("EXECUTING: " + query)
        cursor.execute(query)
        column_names = list(zip(*cursor.fetchall()))[0]
        cursor.close()
        return column_names

    def get_all_rows(self, tablename):
        ''' get all rows from selected table '''
        cursor = self.conn.cursor(buffered=True)
        query = "SELECT * FROM {0:s}".format(tablename)
        logging.debug("EXECUTING: " + query)
        cursor.execute(query)
        all_rows = cursor.fetchall()
        cursor.close()
        return all_rows

    def get_rows_from_columns(self, tablename, **cols):
        ''' get row data by by specific columns '''
        cursor = self.conn.cursor(buffered=True)
        columns = cols.get("columns")
        query = "SELECT {1:s} FROM {0:s}".format(tablename, ','.join(map(str, columns)))
        logging.debug("EXECUTING: " + query)
        cursor.execute(query)
        all_rows = cursor.fetchall()
        cursor.close()
        return all_rows

    def get_row_by_id(self, tablename, idx):
        ''' get row data by by specific columns '''
        cursor = self.conn.cursor(buffered=True)
        query1 = "SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE (`TABLE_NAME` = {0!r}) AND (`COLUMN_KEY` = 'PRI');".format(tablename)
        logging.debug("EXECUTING: " + query1)
        cursor.execute(query1)
        columnID = cursor.fetchone()[0]
        query2 = "SELECT * FROM {0:s} WHERE {1:s} = {2!r};".format(tablename, columnID, str(idx))
        logging.debug("EXECUTING: " + query2)
        cursor.execute(query2)
        single_row = cursor.fetchone()

        cursor.close()
        return single_row

    def get_value_by_id(self, tablename, column, idx):
        ''' get row data by by specific columns '''
        cursor = self.conn.cursor(buffered=True)
        query1 = "SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE (`TABLE_NAME` = {0!r}) AND (`COLUMN_KEY` = 'PRI');".format(tablename)
        logging.debug("EXECUTING: " + query1)

        try:
            cursor.execute(query1)
            columnID = cursor.fetchone()[0]
        except Error as err:
            logging.debug("\n\nSomething went wrong: {}".format(err))
            return -1

        query2 = "SELECT {1:s} FROM {0:s} WHERE {2:s} = {3!r};".format(tablename, column, columnID, str(idx))
        logging.debug("EXECUTING: " + query2)

        try:
            cursor.execute(query2)
            single_row = cursor.fetchone()[0]
            return single_row
        except Error as err:
            logging.debug("\n\nSomething went wrong: {}".format(err))
            return -1
        except TypeError as err:
            logging.debug("No results found".format(err))
            return 0   
        else:
            return 1
        cursor.close()

    def get_rows_from_columns_by_foren_id(self, tablename, forencolumn, forenidx, **cols):
        ''' get rows from foren ids '''
        cursor = self.conn.cursor(buffered=True)
        if not isinstance(cols.get("columns"), str):
            columns = ','.join(map(str, cols.get("columns"))) 
        else:
            columns = cols.get("columns")
        query = "SELECT {1:s} FROM {0:s} WHERE {2:s} = {3!r};".format(tablename, columns, forencolumn, str(forenidx))
        
        try:
            cursor.execute(query)
            logging.debug("EXECUTING: " + query)
            if len(cols.get("columns"))==1 or isinstance(cols.get("columns"), str):
                all_rows = [i[0] for i in cursor.fetchall()]
            else:
 
                all_rows =cursor.fetchall()
            return all_rows
        except Error as err:
            logging.debug("\n\nSomething went wrong: {}".format(err))
            return -1
        except TypeError as err:
            logging.debug("No results found".format(err))
            return 0   
        else:
            return 1
        cursor.close()

    def value_exists(self, tablename, column, value):
        ''' check whenever value exists in specified table under specified column '''
        cursor = self.conn.cursor(buffered=True)
        query = "SELECT {1:s}, COUNT(*) FROM {0:s} WHERE {1:s} = {2!r} GROUP BY {1:s}".format(tablename, column, value)
        logging.debug("EXECUTING: " + query)
        cursor.execute(query)
        number_of_rows_found = cursor.rowcount
        cursor.close()
        return number_of_rows_found

    def get_value_id(self, tablename, column, value):
        ''' check whenever value exists in specified table under specified column '''
        cursor = self.conn.cursor(buffered=True)
        query = "SELECT @id:={3:s} AS id FROM {0:s} WHERE {1:s} = {2!r}".format(tablename, column, value, self.get_primary_key(tablename))
        logging.debug("EXECUTING: " + query)
        try:
            cursor.execute(query)
            get_query = cursor.fetchone()
            value_id = get_query[0] if get_query != None else None
        except Error as err:
            logging.debug("\n\nSomething went wrong: {}".format(err))
            return -1
        else:
            return value_id
        cursor.close()

    def insert_single_row(self, tablename, **colvals):
        ''' insert single row/values into table.
        requires 'columns' and 'values' as an argument '''
        cursor = self.conn.cursor(buffered=True)
        columns = colvals.get("columns")
        values = colvals.get("values")
        query = "INSERT INTO {0:s} ({1:s}) VALUES ({2:s})".format(tablename, ','.join(map(str, columns)), ','.join(map(repr, values)))
        logging.debug("EXECUTING: " + query)
        try:
            cursor.execute(query)
        except Error as err:
            logging.debug("\n\nSomething went wrong: {}".format(err))
            return -1
        else:
            return 1
        cursor.close()

    def update_single_row(self, tablename, key, **colvals):
        ''' insert single row/values into table.
        requires 'columns' and 'values' as an argument '''
        cursor = self.conn.cursor(buffered=True)

        primary_key_column = self.get_primary_key(tablename)
        columns = colvals.get("columns")
        values = colvals.get("values")

        # check if colvals is not just a single string
        if isinstance(columns, str):
            single_column = columns
            single_value = values
            query = "UPDATE {0:s} SET {1:s} = {2!r} WHERE {3:s} = {4:s}".format(tablename, single_column, single_value, primary_key_column, str(key))
            logging.debug("EXECUTING: " + query)
            try:
                cursor.execute(query)
            except Error as err:
                logging.debug("\n\nSomething went wrong: {}".format(err))
                return -1
            else:
                return 1
        else:
            if len(columns)!=len(values):
                logging.debug("\n\nNumber of columns and values missmatch")
                return -1
                raise ValueError('Number of columns and values missmatch')

            for idx, value in enumerate(columns):
                single_column = value
                single_value = values[idx]
                query = "UPDATE {0:s} SET {1:s} = {2!r} WHERE {3:s} = {4:s}".format(tablename, single_column, single_value, primary_key_column, str(key))
                logging.debug("EXECUTING: " + query)
                try:
                    cursor.execute(query)
                except Error as err:
                    logging.debug("\n\nSomething went wrong: {}".format(err))
                    return -1
                else:
                    return 1

        cursor.close()
        

    def insert_single_value(self, tablename, column, values):
        ''' insert single values into table '''
        cursor = self.conn.cursor(buffered=True)
        query = "INSERT INTO {0:s} ({1:s}) VALUES({2!r})".format(tablename, column, values)
        logging.debug("EXECUTING: " + query)
        cursor.execute(query)
        cursor.close()
        return 1

    def update_single_value(self, tablename, key, column, value):
        ''' insert single row/values into table.
        requires 'columns' and 'values' as an argument '''
        cursor = self.conn.cursor(buffered=True)

        primary_key_column = self.get_primary_key(tablename)

        query = "UPDATE {0:s} SET {1:s} = {2!r} WHERE {3:s} = {4:s}".format(tablename, column, value, primary_key_column, str(key))
        logging.debug("EXECUTING: " + query)
        cursor.execute(query)
        cursor.close()
        return 1

    def remove_by_value(self, tablename, column, values):
        ''' remove row by matching value '''
        cursor = self.conn.cursor(buffered=True)
        query = "DELETE FROM {0:s} WHERE {1:s} = {2!r}".format(tablename, column, values)
        logging.debug("EXECUTING: " + query)
        cursor.execute(query)
        cursor.close()
        return 1

    def raw_call(self, call):
        ''' allows to execture raw call to DB '''
        cursor = self.conn.cursor(buffered=True)
        query = call
        logging.debug("EXECUTING: " + query)
        cursor.execute(query)
        get_query = cursor.fetchall()
        cursor.close()
        return get_query   

    def save(self):
        ''' commit to database '''
        self.conn.commit()
        logging.debug('Changes Saved to DB')

    def as_dict(self):
        pass

    def get_data(self):
        pass

    def __repr__(self):
        return str(self.value)

    @staticmethod
    def read_db_config(cfgfilename, section='mysql'):
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
        logging.debug('Connection closed.')
        logging.shutdown()


if __name__ == '__main__':
    dbconnect = Connect('dbconfig.ini', debug="True")
    # data = dbconnect.get_column_names("hdrs")
    # data = dbconnect.get_primary_key("cameras")
    # data = dbconnect.get_value_id("cameras", "cameraName", "GoPro")

    # get_camera_name="Canon EOS 5D Mark II"
    # data = dbconnect.get_value_id("cameras", "cameraName", get_camera_name.strip())

    # "cameras_cameraID"
    # data = dbconnect.get_row_by_id("cameras", 2)
    # data = dbconnect.get_value_by_id("cameras", "cameraName", 2)
    # data = dbconnect.get_value_by_id("lenses", "lensMake", 1)
    # data = dbconnect.show_tables()
    # print(data)
    # print(type(data))
    # data = dbconnect.get_all_rows("showStructure")
    # print(data)
    # call = "SELECT s.structureName, s.structurePath, p.platformName FROM showStructure s LEFT JOIN platforms p ON s.platforms_platformID = p.platformID"
    # data = dbconnect.raw_call(call)
    # print(data)

    # columns = ["rangeStart", "rangeEnd", "handles"]
    seqdb_columns = ["seqId", "seqName"]
    data = dbconnect.get_rows_from_columns_by_foren_id("sequences", "shows_showID", 24, columns=seqdb_columns)
    print(data)
    columns = ["rangeStart", "rangeEnd", "handles"]
    data = dbconnect.get_rows_from_columns_by_foren_id("shots", "shotID", 28, columns=columns)
    # data = dbconnect.get_rows_from_columns("sequences", columns=columns)
    print(data)
    # info = dbconnect.get_column_names("shots")
    # print(info)


    dbconnect.close_connection()