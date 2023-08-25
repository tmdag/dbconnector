"""
This python script is using official MySQL python connector from MySQL connector website.
While official python module is using 'low level' MySQL querries,
this dbmanager wraps them around to more pythonic functions.
"""
import sys
import os.path
import logging
from contextlib import contextmanager
from typing import List, Tuple, Any, Union
from mysql.connector import Error, errorcode, pooling
from functools import lru_cache
from configparser import ConfigParser
##=====================================


APP_DIR = os.path.dirname(os.path.abspath(__file__))
LOGGING_CFG_FILE = os.path.join(APP_DIR, "config", "logging_config.ini")

class Connect:
    """ main connect class """

    # Define the connection pool outside of the __init__ method
    cnxpool = None

    def __init__(self, cfg='config.ini', debug=False, pool_size=5):
        """
        Initializes the database connection manager.

        :param cfg: Path to the configuration file, default is 'config.ini'.
        :param debug: Enables or disables debug mode, default is False.
        :param key: Optional key parameter.
        :param value: Optional value parameter.
        """
        self.debug = debug
        self.init_logging()
        LOG.debug("log initialized")
        self.cfgfile = cfg
        db_config = self.read_db_config(self.cfgfile)
        db_config["ssl_disabled"] = True
        self.db_name = db_config.get("database")
        self.key = None
        self.value = None
        self.conn = None
        self.pool =  pooling.MySQLConnectionPool(pool_name="mypool",
                                                            pool_size=pool_size,
                                                            **db_config)

    def __enter__(self):
        try:
            self.conn = self.pool.get_connection()
            if self.conn.is_connected():
                LOG.debug('Connected to MySQL database successfully')
                return self
            else:
                LOG.error('Failed to obtain connection from pool')
                # Handle the error as needed
        except Exception as e:
            LOG.error('Error while obtaining connection: %s', e)
            # Handle the error as needed

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exits the context of the database connection manager, closing the connection if needed.

        :param exc_type: Exception type.
        :param exc_value: Exception value.
        :param traceback: Exception traceback.
        """
        if self.conn and hasattr(self.conn, 'is_connected') and self.conn.is_connected():
            LOG.debug('Returning connection to pool')
            self.conn.close()
        logging.shutdown()

    @contextmanager
    def cursor(self):
        """
        Provides a cursor to interact with the MySQL database. The cursor is buffered.

        :yield: Buffered cursor.
        """
        cur = self.conn.cursor(buffered=True)
        try:
            yield cur
        finally:
            cur.close()

    @staticmethod
    def init_logging(log_file=None, append=False, console_loglevel=logging.CRITICAL, enable_console_logging=True):
        """
        Initializes the logging system for the database connection manager.
        Configures the log level and format based on the debug setting.

        :param log_file: The path to the log file. If None, logging to a file is disabled.
        :param append: If True, append to the log file; otherwise, overwrite it.
        :param console_loglevel: The log level for console logging.
        :param enable_console_logging: If True, enable logging to the console.
        :return: None
        """
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        if log_file is not None:
            filemode_val = 'a' if append else 'w'
            file_handler = logging.FileHandler(log_file, mode=filemode_val)
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(threadName)s %(name)s %(message)s"))
            logger.addHandler(file_handler)

        if enable_console_logging:
            console = logging.StreamHandler()
            console.setLevel(console_loglevel)
            console.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s'))
            logger.addHandler(console)

        global LOG
        LOG = logger

    @lru_cache(maxsize=100)
    def raw_call(self, call: str) -> Union[List, int]:
        """
        Allows execution of a raw SQL call to the connected MySQL database.

        :param call: The raw SQL query string to execute.
        :return: The result of the query as a list of rows, or 0 if an error occurs.
        :raises Error: If there is an error in executing the query.
        """
        with self.cursor() as cursor:
            query = call
            LOG.debug("EXECUTING: %s", query)
            try:
                cursor.execute(query)
                get_query = cursor.fetchall()
                return get_query
            except Error as err:
                LOG.debug("\n\nSomething went wrong: %s", err)
                return None

    def save(self):
        """
        Commits changes to the connected MySQL database.

        :return: None, as the method commits the changes to the database and logs a success message.
        """
        self.conn.commit()
        LOG.debug('Changes Saved to DB')

    def test_connection(self):
        try:
            # You can use any light query to test the connection
            with self.cursor() as cursor:
                cursor.execute("SELECT 1")
            return True
        except:
            return False

    def show_tables(self) -> List[str]:
        """
        Retrieves the names of all the tables in the connected MySQL database.

        :return: A list of table names in the connected database.
        :raises Exception: If there is an error in retrieving table names.
        """
        with self.cursor() as cursor:
            query: str = "SHOW TABLES"
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query)
            tables: List[str] = list(list(zip(*cursor.fetchall()))[0])
        return tables

    def get_primary_key(self, tablename: str) -> str:
        """
        Retrieves the primary key column name for the specified table in the connected MySQL database.

        :param tablename: The name of the table for which to retrieve the primary key.
        :return: The name of the primary key column.
        :raises Exception: If there is an error in retrieving the primary key.
        """
        with self.cursor() as cursor:
            query = "SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE (`TABLE_NAME` = %s) AND (`COLUMN_KEY` = 'PRI')"
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query, (tablename,))
            primary_key_name = cursor.fetchone()[0]
        return primary_key_name

    def get_column_names(self, tablename: str) -> List[str]:
        """
        Retrieves the names of all columns for the specified table in the connected MySQL database.

        :param tablename: The name of the table for which to retrieve the column names.
        :return: A list of column names.
        :raises Exception: If there is an error in retrieving the column names.
        """
        with self.cursor() as cursor:
            query = "SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ORDINAL_POSITION"
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query, (self.db_name, tablename))
            column_names = list(list(zip(*cursor.fetchall()))[0])
        return column_names

    def get_all_rows(self, tablename: str) -> List[Tuple]:
        """
        Retrieves all rows from the specified table in the connected MySQL database.

        :param tablename: The name of the table from which to retrieve the rows.
        :return: A list of rows, where each row is represented as a tuple.
        :raises Exception: If there is an error in retrieving the rows.
        """
        with self.cursor() as cursor:
            query = "SELECT * FROM {0:s}".format(tablename)
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query)
            all_rows = cursor.fetchall()
        return all_rows

    def get_column(self, tablename: str, column: str) -> List[Any]:
        """
        Retrieves the values of a specific column from the specified table in the connected MySQL database.

        :param tablename: The name of the table from which to retrieve the column.
        :param column: The name of the column to retrieve.
        :return: A list of values representing the specified column.
        :raises Exception: If there is an error in retrieving the column.
        """
        with self.cursor() as cursor:
            query = f"SELECT `{column}` FROM `{tablename}`"
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query)
            db_data = cursor.fetchall()
            all_rows = list(tuple(zip(*db_data))[0])
        return all_rows

    def get_rows_from_columns(self, tablename: str, **cols) -> List[Tuple]:
        """
        Retrieves specific columns from all rows of the specified table in the connected MySQL database.

        :param tablename: The name of the table from which to retrieve the rows.
        :param cols: Keyword argument containing the "columns" key with a list of column names to retrieve.
        :return: A list of rows, where each row is represented as a tuple containing the specified columns.
        """
        with self.cursor() as cursor:
            columns = ', '.join([f"`{col}`" for col in cols.get("columns", [])])
            query = f"SELECT {columns} FROM `{tablename}`"
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query)
            all_rows = cursor.fetchall()
        return all_rows

    def get_rowss_from_columns_by_key(self, tablename: str, key: str, value: Any, **cols) -> List[Tuple]:
        """
        Retrieves specific columns from the rows of the specified table in the connected MySQL database, filtered by a key-value pair.

        :param tablename: The name of the table from which to retrieve the rows.
        :param key: The column name used as a key to filter the rows.
        :param value: The value corresponding to the key used to filter the rows.
        :param cols: Keyword argument containing the "columns" key with a list of column names to retrieve.
        :return: A list of rows, where each row is represented as a tuple containing the specified columns and matching the key-value pair.
        """
        with self.cursor() as cursor:
            columns = ', '.join([f"`{col}`" for col in cols.get("columns", [])])
            query = f"SELECT {columns} FROM `{tablename}` WHERE `{key}` = %s"
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query, (value,))
            all_rows = cursor.fetchall()
        return all_rows

    def get_rows_from_columns_by_key(self, tablename: str, key: str, value: Any) -> List[Tuple]:
        """
        Retrieves all columns from the rows of the specified table in the connected MySQL database, filtered by a key-value pair.

        :param tablename: The name of the table from which to retrieve the rows.
        :param key: The column name used as a key to filter the rows.
        :param value: The value corresponding to the key used to filter the rows.
        :return: A list of rows, where each row is represented as a tuple containing all columns and matching the key-value pair.
        """
        with self.cursor() as cursor:
            query = f"SELECT * FROM `{tablename}` WHERE `{key}` = %s"
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query, (value,))
            all_rows = cursor.fetchall()
        return all_rows

    def get_rows_from_columns_by_foregin_id(self, tablename: str, foregincolumn: str, foreginidx: Any, **cols) -> Union[List[Any], int]:
        """
        Retrieves specific columns from the rows of the specified table in the connected MySQL database, filtered by a foreign key index.

        :param tablename: The name of the table from which to retrieve the rows.
        :param foreigncolumn: The name of the foreign key column used for filtering.
        :param foreignidx: The foreign key index value used for filtering.
        :param cols: Keyword argument containing the "columns" key with a list or string representing the column names to retrieve.
        :return: A list of rows matching the foreign key filter, or 0 if an error occurs.
        :raises Error: If there is an error in executing the query.
        :raises TypeError: If no results are found.
        """
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

    def get_row_by_id(self, tablename: str, idx: Any) -> Union[Tuple, None]:
        """
        Retrieves a single row from the specified table in the connected MySQL database, filtered by a specific index.

        :param tablename: The name of the table from which to retrieve the row.
        :param idx: The index value used to identify the specific row.
        :return: A tuple representing the single row matching the index, or None if not found.
        """
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

    def get_value_id(self, tablename: str, column: str, value: Any) -> Union[int, None]:
        """
        Checks whether a specific value exists in the specified table and column in the connected MySQL database and retrieves the corresponding ID.

        :param tablename: The name of the table in which to check for the value.
        :param column: The name of the column in which to check for the value.
        :param value: The value to search for in the specified table and column.
        :return: The ID corresponding to the value if found, None if not found, or 0 if an error occurs.
        :raises Error: If there is an error in executing the query.
        """
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

    def get_value_id_multiple(self, tablename: str, **colvals) -> Union[int, None]:
        """
        Retrieves the value ID by comparing multiple entries in the specified table in the connected MySQL database.

        :param tablename: The name of the table in which to check for the values.
        :param colvals: Keyword argument containing the "columns" key with a list of column names and the "values" key with a corresponding list of values.
        :return: The ID corresponding to the matched columns and values if found, None if not found, or -1 if an error occurs.
        :raises Error: If there is an error in executing the query.
        """
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

    def get_value_by_id(self, tablename: str, column: str, idx: Any) -> Union[Any, int]:
        """
        Retrieves a specific value from the specified table and column in the connected MySQL database, filtered by the primary key index.

        :param tablename: The name of the table from which to retrieve the value.
        :param column: The name of the column from which to retrieve the value.
        :param idx: The primary key index value used to identify the specific row.
        :return: The value corresponding to the specified column and index, or 0 if an error occurs, or 1 if no TypeError occurs.
        :raises Error: If there is an error in executing the query.
        :raises TypeError: If no results are found.
        """
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


    def value_exists(self, tablename: str, column: str, value: Any) -> int:
        """
        Checks whether a specific value exists in the specified table and column in the connected MySQL database.

        :param tablename: The name of the table in which to check for the value.
        :param column: The name of the column in which to check for the value.
        :param value: The value to search for in the specified table and column.
        :return: The number of rows found with the specified value in the specified table and column.
        """
        query = f"SELECT `{column}`, COUNT(*) FROM `{tablename}` WHERE `{column}` = %s GROUP BY `{column}`"
        LOG.debug("EXECUTING: %s", query)
        with self.cursor() as cursor:
            cursor.execute(query, (value,))
            result = cursor.fetchone()
            number_of_rows_found = result[1] if result else 0
        return number_of_rows_found


    def value_exists_multiple(self, tablename, **colvals):
        """
        Checks whether specific values exist in the specified table and columns in the connected MySQL database.

        :param tablename: The name of the table in which to check for the values.
        :param colvals: Keyword argument containing the "columns" key with a list of column names and the "values" key with a corresponding list of values.
        :return: The number of rows found with the specified values in the specified table and columns, or 0 if an error occurs or no results are found.
        :raises Error: If there is an error in executing the query.
        :raises TypeError: If no results are found.
        """
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
        """
        Inserts a single row into the specified table in the connected MySQL database.

        :param tablename: The name of the table into which to insert the row.
        :param colvals: Keyword argument containing the "columns" key with a list of column names and the "values" key with a corresponding tuple of values.
        :return: The ID of the inserted row, or -1 if an error occurs.
        :raises Error: If there is an error in inserting the row.
        """
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
        """
        Inserts a single row into the specified table in the connected MySQL database by passing a dictionary of column names and values.

        :param tablename: The name of the table into which to insert the row.
        :param dbdata: A dictionary containing the column names as keys and the corresponding values to insert.
        :return: The ID of the inserted row, or -1 if an error occurs.
        :raises Error: If there is an error in inserting the row.
        """
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
        """
        Updates a single row in the specified table in the connected MySQL database.

        :param tablename: The name of the table in which to update the row.
        :param key: The primary key value used to identify the specific row to update.
        :param colvals: Keyword argument containing the "columns" key with a list or string of column names and the "values" key with a corresponding tuple of values.
        :return: 1 if the update is successful, or 0 if an error occurs or if the number of columns and values mismatch.
        :raises Error: If there is an error in updating the row.
        """
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
        """
        Inserts a single value into the specified table and column in the connected MySQL database.

        :param tablename: The name of the table into which to insert the value.
        :param column: The name of the column into which to insert the value.
        :param values: The value to insert into the specified table and column.
        :return: The ID of the inserted row, or -1 if an error occurs.
        :raises Error: If there is an error in inserting the value.
        """
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
        """
        Updates a single value in the specified table, column, and key in the connected MySQL database.

        :param tablename: The name of the table in which to update the value.
        :param key: The primary key value used to identify the specific row to update.
        :param column: The name of the column in which to update the value.
        :param value: The new value to update in the specified table, column, and key.
        :return: 1, indicating the update is successful.
        """
        with self.cursor() as cursor:

            primary_key_column = self.get_primary_key(tablename)

            query = "UPDATE {0:s} SET {1:s} = {2!r} WHERE {3:s} = {4:s}".format(tablename, column, value, primary_key_column, str(key))
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query)

        return 1

    def remove_by_value(self, tablename, column, values):
        """
        Removes a row from the specified table in the connected MySQL database by matching a value in a specific column. This method is considered dangerous; it's better to use remove_by_id.

        :param tablename: The name of the table from which to remove the row.
        :param column: The name of the column used to identify the specific row to remove.
        :param values: The value used to match the row to remove.
        :return: 1, indicating the removal is successful.
        """
        with self.cursor() as cursor:
            query = "DELETE FROM {0:s} WHERE {1:s} = {2!r}".format(tablename, column, values)
            LOG.debug("EXECUTING: %s", query)
            cursor.execute(query)
        return 1

    def remove_by_id(self, tablename, idx):
        """
        Removes a whole row from the specified table in the connected MySQL database by matching the primary key ID.

        :param tablename: The name of the table from which to remove the row.
        :param idx: The primary key ID used to identify the specific row to remove.
        :return: 1 if the removal is successful, or 0 if an error occurs or no results are found.
        :raises Error: If there is an error in executing the query.
        :raises TypeError: If no results are found.
        """
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
        """
        Reads the database configuration from the specified file.

        :param filename: Path to the configuration file.
        :return: Dictionary containing the database configuration.
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
        # out = dbconnect.get_value_by_id("shows", "showName", 22)
        out = dbconnect.get_row_by_id("hdrs", 48)
        print(out)

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

    # dbconnect.save()
    # dbconnect.close_connection()