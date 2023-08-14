"""
This python script is using official MySQL python connector from MySQL connector website.
While official python module is using 'low level' MySQL querries,
this dbmanager wraps them around to more pythonic functions.
"""
import sys
import os.path
import logging
from contextlib import contextmanager
from mysql.connector import MySQLConnection, Error, errorcode
if sys.version_info[0] < 3:
    from configparser import ConfigParser
else:
    from configparser import ConfigParser
##=====================================

APP_DIR = os.path.dirname(os.path.abspath(__file__))
LOGGING_CFG_FILE = os.path.join(APP_DIR, "config", "logging_config.ini")

class Connect:
    """ main connect class """
    def __init__(self, cfg='config.ini', debug=False, key=None, value=None):
        """ Connect to MySQL database """
        self.debug = debug
        self.cfgfile = cfg
        db_config = self.read_db_config(self.cfgfile)
        self.db_name = db_config.get("database")
        self.key = None
        self.value = None
        self.conn = None

    def __enter__(self):
        self.connect_to_db()
        return self  # Return the Connect object itself


    def __exit__(self, exc_type, exc_value, traceback):
        if self.conn:
            self.close_connection()
        logging.shutdown()

    def connect_to_db(self):
        self.init_logging()
        LOG.debug("log initialized")
        db_config = self.read_db_config(self.cfgfile)
        self.db_name = db_config.get("database")
        try:
            LOG.debug('Connecting to MySQL database...')
            self.conn = MySQLConnection(**db_config)
            if self.conn.is_connected():
                LOG.debug('Connected to MySQL database successfully')
        except Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                LOG.debug("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                LOG.debug("Database does not exist")
            else:
                LOG.debug("Error: %s", err)
            raise Exception(err)

    @contextmanager
    def cursor(self):
        cur = self.conn.cursor(buffered=True)
        try:
            yield cur
        finally:
            cur.close()

    @staticmethod
    def init_logging(log_file=None, append=False, console_loglevel=logging.DEBUG):
        """Set up logging to file and console."""
        # CRITICAL 50
        # ERROR 40
        # WARNING 30
        # INFO 20
        # DEBUG 10
        # NOTSET 0
        if log_file is not None:
            if append:
                filemode_val = 'a'
            else:
                filemode_val = 'w'
            logging.basicConfig(level=logging.DEBUG,
                                format="%(asctime)s %(levelname)s %(threadName)s %(name)s %(message)s",
                                # datefmt='%m-%d %H:%M',
                                filename=log_file,
                                filemode=filemode_val)

        # define a Handler which writes INFO messages or higher to the sys.stderr
        console = logging.StreamHandler()
        console.setLevel(console_loglevel)
        # set a format which is simpler for console use
        formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        console.setFormatter(formatter)

        global LOG
        LOG = logging.getLogger(__name__)
        if not LOG.hasHandlers():
            LOG.setLevel(logging.DEBUG)
            LOG.addHandler(console)

    def show_tables(self):
        ''' show tables in current database '''
        with self.cursor() as cursor:
            query = "SHOW TABLES"
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query)
            tables = cursor.fetchall()
        self.key = self.db_name
        self.value = tuple(zip(*tables))[0]
        return self

    def get_primary_key(self, tablename):
        ''' gets name of primary key in a table '''
        with self.cursor() as cursor:
            query = "SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE (`TABLE_NAME` = {0!r}) AND (`COLUMN_KEY` = 'PRI')".format(tablename)
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query)
            primary_key_name = cursor.fetchone()[0]
        return primary_key_name

    def get_column_names(self, tablename):
        ''' list column names of a given tablename '''
        with self.cursor() as cursor:
            query = "SELECT column_name FROM information_schema.columns  WHERE table_schema={0!r} AND table_name={1!r} ORDER BY ORDINAL_POSITION".format(self.db_name, tablename)
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query)
            column_names = list(list(zip(*cursor.fetchall()))[0])
        return column_names

    def get_all_rows(self, tablename):
        ''' get all rows from selected table '''
        with self.cursor() as cursor:
            query = "SELECT * FROM {0:s}".format(tablename)
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query)
            all_rows = cursor.fetchall()
        return all_rows

    def get_column(self, tablename, column):
        ''' get whole column from table '''
        with self.cursor() as cursor:
            query = "SELECT {0:s} FROM {1:s}".format(column, tablename)
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query)
            db_data = cursor.fetchall()
            all_rows = list(tuple(zip(*db_data))[0])
        return all_rows

    def get_rows_from_columns(self, tablename, **cols):
        ''' get row data by by specific columns '''
        with self.cursor() as cursor:
            columns = cols.get("columns")
            query = "SELECT {1:s} FROM {0:s}".format(tablename, ','.join(map(str, columns)))
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query)
            all_rows = cursor.fetchall()
        return all_rows

    def get_rowss_from_columns_by_key(self, tablename, key, value, **cols):
        ''' get row data by by specific columns '''
        with self.cursor() as cursor:
            columns = cols.get("columns")
            query = "SELECT {1:s} FROM {0:s} WHERE {2:s} = {3:s}".format(tablename, ','.join(map(str, columns)), key, str(value))
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query)
            all_rows = cursor.fetchall()
        return all_rows

    def get_rows_from_columns_by_key(self, tablename, key, value):
        ''' get row data by by specific columns '''
        with self.cursor() as cursor:
            query = "SELECT * FROM {0:s} WHERE {1:s} = {2:s}".format(tablename, key, str(value))
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query)
            all_rows = cursor.fetchall()
        return all_rows

    def get_rows_from_columns_by_foregin_id(self, tablename, foregincolumn, foreginidx, **cols):
        ''' get rows from foregin ids. For eg get a column by comparing values of different column like shot name where seqID = 3 '''
        with self.cursor() as cursor:
            if not isinstance(cols.get("columns"), str):
                columns = ','.join(map(str, cols.get("columns")))
            else:
                columns = cols.get("columns")
            query = "SELECT {1:s} FROM {0:s} WHERE {2:s} = {3!r};".format(tablename, columns, foregincolumn, str(foreginidx))

            try:
                cursor.execute(query)
                LOG.debug("EXECUTING: %s", query)
                if len(cols.get("columns"))==1 or isinstance(cols.get("columns"), str):
                    all_rows = [i[0] for i in cursor.fetchall()]
                else:

                    all_rows =cursor.fetchall()
                return all_rows
            except Error as err:
                LOG.debug("\n\nSomething went wrong: %s", err)
                return 0
            except TypeError as err:
                LOG.debug("No results found %s", err)
                return 0
            else:
                return 1

    def get_row_by_id(self, tablename, idx):
        ''' get row data by by specific columns '''
        with self.cursor() as cursor:
            query1 = "SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE (`TABLE_NAME` = {0!r}) AND (`COLUMN_KEY` = 'PRI');".format(tablename)
            LOG.debug("EXECUTING: %s", query1)
            cursor.execute(query1)
            columnID = cursor.fetchone()[0]
            query2 = "SELECT * FROM {0:s} WHERE {1:s} = {2!r};".format(tablename, columnID, str(idx))
            LOG.debug("EXECUTING: %s", query2)
            cursor.execute(query2)
            single_row = cursor.fetchone()
        return single_row

    def get_value_id(self, tablename, column, value):
        ''' check whenever value exists in specified table under specified column '''
        with self.cursor() as cursor:
            query = "SELECT @id:={3:s} AS id FROM {0:s} WHERE {1:s} = {2!r}".format(tablename, column, value, self.get_primary_key(tablename))
            LOG.debug("EXECUTING: %s", query)
            try:
                cursor.execute(query)
                get_query = cursor.fetchone()
                value_id = get_query[0] if get_query != None else None
            except Error as err:
                LOG.debug("\n\nSomething went wrong: %s", err)
                return 0
            else:
                return value_id

    def get_value_id_multiple(self, tablename, **colvals):
        ''' gets value id by multiple entries comparison '''
        with self.cursor() as cursor:
            columns = colvals.get("columns")
            values = colvals.get("values")
            keys = zip(columns, values)

            query = "SELECT @id:={0:s} AS id FROM {1:s} WHERE".format(self.get_primary_key(tablename), tablename)

            elements = (len(columns))
            for i, (key, value) in enumerate(keys):
                query += " {0:s} = {1!r} ".format(key, value)
                query += "AND" if i<elements-1 else ""
            LOG.debug("EXECUTING: %s", query)

            try:
                cursor.execute(query)
                get_query = cursor.fetchone()
                value_id = get_query[0] if get_query != None else None
            except Error as err:
                LOG.debug("\n\nSomething went wrong: %s", err)
                return -1
            else:
                return value_id

    def get_value_by_id(self, tablename, column, idx):
        ''' get row data by by checking its MAIN key '''
        with self.cursor() as cursor:
            query1 = "SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE (`TABLE_NAME` = {0!r}) AND (`COLUMN_KEY` = 'PRI');".format(tablename)
            LOG.debug("EXECUTING: %s", query1)

            try:
                cursor.execute(query1)
                columnID = cursor.fetchone()[0]
            except Error as err:
                LOG.debug("\n\nSomething went wrong: %s", err)
                return 0

            query2 = "SELECT {1:s} FROM {0:s} WHERE {2:s} = {3!r};".format(tablename, column, columnID, str(idx))
            LOG.debug("EXECUTING: %s", query2)

            try:
                cursor.execute(query2)
                single_row = cursor.fetchone()[0]
                return single_row
            except Error as err:
                LOG.debug("\n\nSomething went wrong: %s", err)
                return 0
            except TypeError as err:
                LOG.debug("No results found %s", err)
                return 0
            else:
                return 1


    def value_exists(self, tablename, column, value):
        ''' check whenever value exists in specified table under specified column '''
        with self.cursor() as cursor:
            query = "SELECT {1:s}, COUNT(*) FROM {0:s} WHERE {1:s} = {2!r} GROUP BY {1:s}".format(tablename, column, value)
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query)
            number_of_rows_found = cursor.rowcount
        return number_of_rows_found

    def value_exists_multiple(self, tablename, **colvals):
        ''' check whenever value exists in specified table under specified column '''
        with self.cursor() as cursor:

            columns = colvals.get("columns")
            values = colvals.get("values")
            keys = zip(columns, values)
            query = "SELECT {1:s}, COUNT(*) FROM {0:s} WHERE".format(tablename, columns[0])

            elements = (len(columns))
            if elements > 1:
                for i, (key, value) in enumerate(keys):
                    query += " {0:s} = {1!r} ".format(key, value)
                    query += "AND" if i<elements-1 else ""
            query += "GROUP BY {0:s}".format(columns[0])

            LOG.debug("EXECUTING: %s", query)
            try:
                cursor.execute(query)
                number_of_rows_found = cursor.rowcount
                return number_of_rows_found
            except Error as err:
                LOG.debug("\n\nSomething went wrong: %s", err)
                return 0
            except TypeError as err:
                LOG.debug("No results found %s", err)
                return 0



    def insert_single_row(self, tablename,**colvals):
        ''' insert single row/values into table.
        requires 'columns' and 'values' as an argument '''
        with self.cursor() as cursor:
            columns = colvals.get("columns")
            values = tuple(colvals.get("values"))
            query = "INSERT INTO {} ({}) VALUES ({})".format(tablename, ', '.join(columns), ','.join(['%s']*len(values)))
            LOG.debug("EXECUTING: %s", query%values)
            try:
                cursor.execute(query, values)
            except Error as err:
                LOG.debug("\n\nSomething went wrong: %s\n\n", err)
                return -1
            else:
                cursor.execute("SELECT LAST_INSERT_ID();")
                insert_id = cursor.fetchone()
                return insert_id[0]
                # return 1


    def insert_single_row2(self, tablename, dbdata):
        ''' insert single row by passing dictionary
        db_data={
            "uuid" : '00000',
            "columnA" : 51}
        '''
        with self.cursor() as cursor:
            columns = dbdata.keys()
            query = "INSERT INTO {} ({}) VALUES ({})".format(tablename, ', '.join(columns), ','.join(['%({})'.format(colname) for colname in columns]))
            LOG.debug("EXECUTING: %s", query)
            try:
                cursor.execute(query, dbdata)
            except Error as err:
                LOG.debug("\n\nSomething went wrong: %s\n\n", err)
                return -1
            else:
                cursor.execute("SELECT LAST_INSERT_ID();")
                insert_id = cursor.fetchone()
                return insert_id[0]
                # return 1


    def update_single_row(self, tablename, key, **colvals):
        ''' insert single row/values into table.
        requires 'columns' and 'values' as an argument '''
        with self.cursor() as cursor:
            primary_key_column = self.get_primary_key(tablename)
            columns = colvals.get("columns")
            values = tuple(colvals.get("values"))

            # check if colvals is not just a single string
            if isinstance(columns, str):
                single_column = columns
                single_value = values
                query = "UPDATE {0:s} SET {1:s} = {2!r} WHERE {3:s} = {4:s}".format(tablename, single_column, single_value, primary_key_column, str(key))
                LOG.debug("EXECUTING: %s", query)
                try:
                    cursor.execute(query)
                except Error as err:
                    LOG.debug("\n\nSomething went wrong: %s", err)
                    return 0
                else:
                    return 1
            else:
                LOG.debug("We have %s columns to update", len(columns))
                if len(columns)!=len(values):
                    LOG.debug("\n\nNumber of columns and values missmatch")
                    # raise ValueError('Number of columns and values missmatch')
                    return 0
                for idx, value in enumerate(columns):
                    LOG.debug("Updating column %s with value: %s", value, values[idx])
                    single_column = value
                    single_value = values[idx]
                    query = "UPDATE {0:s} SET {1:s} = {2!r} WHERE {3:s} = {4:s}".format(tablename, single_column, single_value, primary_key_column, str(key))
                    LOG.debug("EXECUTING: %s", query)
                    try:
                        cursor.execute(query)
                    except Error as err:
                        LOG.debug("\n\nSomething went wrong: %s", err)
                        return 0
                return 1



    def insert_single_value(self, tablename, column, values):
        ''' insert single values into table '''
        with self.cursor() as cursor:
            query = "INSERT INTO {0:s} ({1:s}) VALUES({2!r})".format(tablename, column, values)
            LOG.debug("EXECUTING: %s", query)
            try:
                cursor.execute(query)
            except Error as err:
                LOG.debug("\n\nSomething went wrong: %s\n\n", err)
                return -1
            else:
                cursor.execute("SELECT LAST_INSERT_ID();")
                insert_id = cursor.fetchone()
                return insert_id[0]


    def update_single_value(self, tablename, key, column, value):
        ''' insert single row/values into table.
        requires 'columns' and 'values' as an argument '''
        with self.cursor() as cursor:

            primary_key_column = self.get_primary_key(tablename)

            query = "UPDATE {0:s} SET {1:s} = {2!r} WHERE {3:s} = {4:s}".format(tablename, column, value, primary_key_column, str(key))
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query)

        return 1

    def remove_by_value(self, tablename, column, values):
        ''' remove row by matching value. Very dangerous method. Better to use remove by ID '''

        with self.cursor() as cursor:
            query = "DELETE FROM {0:s} WHERE {1:s} = {2!r}".format(tablename, column, values)
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query)
        return 1

    def remove_by_id(self, tablename, idx):
        ''' removes whole row from column by row ID '''
        with self.cursor() as cursor:
            query1 = "SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE (`TABLE_NAME` = {0!r}) AND (`COLUMN_KEY` = 'PRI');".format(tablename)
            LOG.debug("EXECUTING: %s", query1)

            try:
                cursor.execute(query1)
                columnID = cursor.fetchone()[0]
            except Error as err:
                LOG.debug("\n\nSomething went wrong: %s", err)
                return 0

            query2 = "DELETE FROM {0:s} WHERE {1:s} = {2!r}".format(tablename, columnID, str(idx))
            LOG.debug("EXECUTING: %s", query2)

            try:
                cursor.execute(query2)
                return 1
            except Error as err:
                LOG.debug("\n\nSomething went wrong: %s", err)
                return 0
            except TypeError as err:
                LOG.debug("No results found %s", err)
                return 0



    def raw_call(self, call):
        ''' allows to execture raw call to DB '''
        with self.cursor() as cursor:
            query = call
            LOG.debug("EXECUTING: %s", query)
            try:
                cursor.execute(query)
                get_query = cursor.fetchall()
                return get_query
            except Error as err:
                LOG.debug("\n\nSomething went wrong: %s", err)
                return 0



    def save(self):
        ''' commit to database '''
        self.conn.commit()
        LOG.debug('Changes Saved to DB')

    def as_dict(self):
        pass

    def as_list(self, db_data):
        db_data = list(tuple(zip(*db_data))[0])
        return db_data

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
        LOG.debug('Connection closed.------------------------------------------------------------------')
        logging.shutdown()


if __name__ == '__main__':

    cfgfile = APP_DIR +'/../dbconfig.ini';
    with Connect(cfgfile, debug=True) as dbconnect:
        # out = dbconnect.show_tables()
        # out = dbconnect.get_column("hdrs", "hdrID")
        out = dbconnect.get_value_by_id("shows", "showName", 22)

    # dbconnect = Connect(APP_DIR +'/../dbconfig.ini', debug="True")
    # # data = dbconnect.get_rows_from_columns("hdrs", columns=["hdrid"])
    # # ids = []
    # # ids.extend([x[0] for x in data]) # get second column for each row
    # # print(ids)
    # out = dbconnect.get_rows_from_columns_by_key("renders", "users_userID", "6")
    # # out = dbconnect.get_rows_from_columns_by_key("hdrs", "hdrID", "52")
    # #
    # structuredb_columns = ["softwareId", "version"]
    # testA = dbconnect.get_rowss_from_columns_by_key("software", "softwareName", "'Houdini'", columns=structuredb_columns)
    # print(testA)

    print(out)
    # dbconnect.save()
    # dbconnect.close_connection()