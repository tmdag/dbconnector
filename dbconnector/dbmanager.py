"""
This python script is using official MySQL python connector from MySQL connector website.
While official python module is using 'low level' MySQL querries,
this dbmanager wraps them around to more pythonic functions.
"""
import sys
import os.path
import logging
from contextlib import contextmanager
from typing import List, Tuple, Any, Union, Optional
from mysql.connector import Error, errorcode, pooling
from functools import lru_cache, wraps
from configparser import ConfigParser

from sfpipecore.logging_utils import get_logger


APP_DIR = os.path.dirname(os.path.abspath(__file__))
LOGGING_CFG_FILE = os.path.join(APP_DIR, "config", "logging_config.ini")

LOG = get_logger(__name__)

class Connect:
    """ main connect class """

    cnxpool = None
    _pools = {}

    def __init__(self, cfg='config.ini', debug=True, pool_size=5):
        """
        Initializes the database connection manager and its connection pool.
        """
        if debug:
            LOG.setLevel(logging.DEBUG)

        pool_name = "sfpipe_pool"

        if pool_name not in Connect._pools:
            LOG.info(f"Connection pool '{pool_name}' not found. Creating new pool.")
            db_config = self.read_db_config(cfg)
            db_config["ssl_disabled"] = True
            Connect._pools[pool_name] = pooling.MySQLConnectionPool(
                pool_name=pool_name,
                pool_size=pool_size,
                **db_config
            )

        self.pool = Connect._pools[pool_name]
        self.conn = None
        self.db_name = self.read_db_config(cfg).get("database")

    def _reconnect(self):
        """Close the stale connection and get a new one from the pool."""
        LOG.warning("Database connection lost. Attempting to reconnect...")
        if self.conn:
            try:
                self.conn.close()
            except Exception as e:
                LOG.debug(f"Exception while closing stale connection: {e}")
        try:
            self.conn = self.pool.get_connection()
            if self.conn.is_connected():
                LOG.info("Successfully reconnected to the database.")
                return True
        except Error as e:
            LOG.error(f"Failed to reconnect to the database: {e}")
        return False

    def __enter__(self):
        try:
            # If we already have a connection object, ping it to ensure it's alive.
            if self.conn and self.conn.is_connected():
                LOG.debug("Pinging existing connection...")
                self.conn.ping(reconnect=True, attempts=3, delay=1)
            else:
                # If no connection, get a new one.
                self.conn = self.pool.get_connection()

            if self.conn.is_connected():
                LOG.debug('Connection to MySQL database is active.')
                return self
            else:
                LOG.error('Failed to obtain a valid connection from pool.')
                raise Error("Could not establish a valid database connection.")
        except Error as e:
            LOG.error(f'Error during database connection context entry: {e}')
            # Attempt a full reconnect as a last resort
            if self._reconnect():
                return self
            else:
                raise # Re-raise if the final reconnect attempt fails

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

    def _reconnect(self) -> bool:
        """
        Close the stale connection and get a new one from the pool.
        This is a last-resort recovery method.
        """
        LOG.warning("Database connection lost. Attempting a full reconnect...")
        if self.conn:
            try:
                # Close the connection object without returning it to the pool
                self.conn.close()
            except Error as e:
                LOG.debug(f"Exception while closing stale connection: {e}")
        try:
            # Get a fresh connection from the pool
            self.conn = self.pool.get_connection()
            if self.conn.is_connected():
                LOG.info("Successfully reconnected to the database.")
                return True
        except Error as e:
            LOG.error(f"Failed to reconnect to the database: {e}")
        return False

    @staticmethod
    def reconnect_on_operational_error(func):
        """
        A decorator that catches MySQL OperationalError, attempts to reconnect,
        and retries the function once.
        """
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Error as err:
                if err.errno in [errorcode.CR_SERVER_GONE_ERROR,
                                errorcode.CR_SERVER_LOST,
                                errorcode.ER_CON_COUNT_ERROR]:
                    LOG.warning(f"Intercepted MySQL connection error in '{func.__name__}': {err}")
                    if self._reconnect():
                        LOG.info(f"Retrying '{func.__name__}' after successful reconnection.")
                        return func(self, *args, **kwargs)
                    else:
                        LOG.critical("Could not retry function as reconnection failed.")
                        raise # Re-raise the original error if reconnect fails
                else:
                    # Re-raise other errors that are not connection-related
                    raise
        return wrapper

    @contextmanager
    def cursor(self, **kwargs):
        """
        Provides a cursor to interact with the MySQL database.
        Allows passing arguments like buffered=True or dictionary=True.

        :yield: Cursor.
        """
        # Default to buffered=True if not specified by caller
        if 'buffered' not in kwargs and 'dictionary' not in kwargs :
             kwargs['buffered'] = True

        cur = None # Initialize to None
        try:
            cur = self.conn.cursor(**kwargs)
            yield cur
        finally:
            if cur:
                cur.close()


    @reconnect_on_operational_error
    def execute(self, query: str, params: Optional[tuple] = None, dictionary: bool = False, fetch: str = "all") -> Union[List, Tuple, int, None]:
        """
        Executes a given SQL query safely with parameters. This is the primary
        method for all database interactions.

        Args:
            query (str): The SQL query string with %s placeholders.
            params (Optional[tuple]): A tuple of parameters to bind to the query.
            dictionary (bool): If True, returns results as dicts instead of tuples.
            fetch (str): Determines what to fetch. One of 'all', 'one', or 'none'.

        Returns:
            - For SELECT with fetch='all': A list of rows (tuples or dicts).
            - For SELECT with fetch='one': A single row or None.
            - For INSERT/UPDATE/DELETE or fetch='none': The number of affected rows (int).
            - None on any SQL error.
        """
        LOG.debug("EXECUTING: Query=%s, Params=%s", query, params)
        try:
            with self.cursor(dictionary=dictionary) as cursor:
                cursor.execute(query, params or ())

                if fetch == "all":
                    return cursor.fetchall()
                elif fetch == "one":
                    return cursor.fetchone()
                else: # 'none'
                    return cursor.rowcount
        except Error as err:
            LOG.error("SQL Error: %s", err)
            LOG.error("Failed Query: %s | Failed Params: %s", query, params)
            return None

    @reconnect_on_operational_error
    def save(self):
        """Commits the current transaction."""
        self.conn.commit()
        LOG.debug('Changes Saved to DB')

    @reconnect_on_operational_error
    def rollback(self):
        """Rolls back the current transaction."""
        self.conn.rollback()
        LOG.debug('DB Changes Rolled Back')


    @reconnect_on_operational_error
    def raw_call(self, query: str, params: Optional[tuple] = None, dictionary: bool = False) -> Union[List, int, None]:
        """
        Executes a raw SQL call, safely handling parameters and cursor type.

        Args:
            query (str): The raw SQL query string with placeholders (%s).
            params (Optional[tuple]): A tuple of parameters to be safely substituted.
            dictionary (bool): If True, returns results as a list of dictionaries.

        Returns:
            The result of the query.
        """
        # The LOG call now shows the query template AND the parameters, which is much better for debugging.
        LOG.debug("EXECUTING: %s PARAMS: %s", query, params)
        try:
            # Pass the dictionary argument to the cursor context manager
            with self.cursor(dictionary=dictionary) as cursor:
                cursor.execute(query, params or ()) # Ensure params is a tuple, even if empty

                # fetchall() is what we want for SELECT queries.
                # For INSERT/UPDATE, the command runs but fetchall() will be empty.
                if cursor.statement.strip().upper().startswith("SELECT"):
                    return cursor.fetchall()
                else:
                    # For non-SELECT queries, we can return the row count to indicate success.
                    return cursor.rowcount
        except Error as err:
            LOG.error("SQL Error in raw_call: %s", err)
            LOG.error("Failed Query: %s | Failed Params: %s", query, params)
            return None

    @reconnect_on_operational_error
    def save(self):
        """
        Commits changes to the connected MySQL database.

        :return: None, as the method commits the changes to the database and logs a success message.
        """
        self.conn.commit()
        LOG.debug('Changes Saved to DB')

    @reconnect_on_operational_error
    def test_connection(self):
        try:
            # You can use any light query to test the connection
            with self.cursor() as cursor:
                cursor.execute("SELECT 1")
            return True
        except:
            return False

    @reconnect_on_operational_error
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

    @reconnect_on_operational_error
    @lru_cache(maxsize=128)
    def get_primary_key(self, tablename: str) -> str:
        """
        Retrieves the primary key column name for the specified table.
        This method is cached for performance.
        """
        with self.cursor() as cursor:
            # Use parameterized query for safety
            query = "SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE (`TABLE_NAME` = %s) AND (`COLUMN_KEY` = 'PRI')"
            LOG.debug("EXECUTING: %s with params (%s,)", query, tablename)
            cursor.execute(query, (tablename,))
            result = cursor.fetchone()
            if not result:
                raise ValueError(f"Could not find a primary key for table '{tablename}'.")
            primary_key_name = result[0]
        return primary_key_name

    @reconnect_on_operational_error
    @lru_cache(maxsize=128)
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

    @reconnect_on_operational_error
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

    @reconnect_on_operational_error
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

    @reconnect_on_operational_error
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

    @reconnect_on_operational_error
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

    @reconnect_on_operational_error
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

    @reconnect_on_operational_error
    def get_rows_by_key(self, tablename: str, key_column: str, key_value: Any, select_columns: Optional[List[str]] = None) -> List[Tuple]:
        """
        Retrieves rows from a table, filtered by a key-value pair, allowing selection of specific columns.

        If `select_columns` is None or empty, all columns ('*') are retrieved. Otherwise, only the
        specified columns are retrieved.

        Args:
            tablename (str): The name of the table from which to retrieve the rows.
            key_column (str): The column name used as a key to filter the rows.
            key_value (Any): The value corresponding to the key_column used to filter the rows.
            select_columns (Optional[List[str]], optional): A list of column names to retrieve.
                If None or empty, all columns are selected. Defaults to None.

        Returns:
            List[Tuple]: A list of rows, where each row is represented as a tuple.
                        Each tuple contains the selected column values in the order specified
                        (or all columns if `select_columns` is None). Returns an empty list
                        if no rows are found or an error occurs.
        """
        with self.cursor() as cursor:
            if select_columns and len(select_columns) > 0:
                columns_str = ', '.join([f"`{col}`" for col in select_columns])
            else:
                columns_str = '*'

            query = f"SELECT {columns_str} FROM `{tablename}` WHERE `{key_column}` = %s"
            LOG.debug(f"EXECUTING: {query} with value: {key_value}")
            try:
                cursor.execute(query, (key_value,))
                all_rows = cursor.fetchall()
                return all_rows
            except Error as err:
                LOG.error(f"Error executing get_rows_by_key for table {tablename}: {err}")
                return []

    @reconnect_on_operational_error
    def get_rows_from_columns_by_foreign_id(self, tablename: str, foregincolumn: str, foreginidx: Any, **cols) -> Union[List[Any], int]:
        """
        Retrieves specific columns from rows filtered by a foreign key index.

        Args:
            tablename (str): The name of the table.
            foreign_column (str): The name of the foreign key column for filtering.
            foreign_idx (Any): The foreign key index value for filtering.
            **cols: Keyword argument "columns" (List[str] or str): Column(s) to retrieve.

        Returns:
            Union[List[Any], int]: A list of rows matching the filter, or 0 on error/no results.
                                If a single column string is provided in **cols, a flat list
                                of values from that column is returned. If multiple columns
                                are specified, a list of tuples is returned.

        Raises:
            Error: If there is an error in executing the query (logged, returns 0).
            TypeError: If no results are found (logged, returns 0).
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

    @reconnect_on_operational_error
    def get_row_by_id(self, tablename: str, idx: Any) -> Union[Tuple, None]:
        """
        Retrieves a single row from the specified table in the connected MySQL database, filtered by a specific index.

        :param tablename: The name of the table from which to retrieve the row.
        :param idx: The index value used to identify the specific row.
        :return: A tuple representing the single row matching the index, or None if not found.
        """
        with self.cursor() as cursor:
            primary_key_col = self.get_primary_key(tablename)
            query = f"SELECT * FROM `{tablename}` WHERE `{primary_key_col}` = %s"
            params = (idx,)
            LOG.debug("EXECUTING: %s with params %s", query, params)
            cursor.execute(query, params)
            single_row = cursor.fetchone()
        return single_row

    @reconnect_on_operational_error
    def get_value_id(self, tablename: str, column: str, value: Any) -> Union[int, None]:
        """
        Retrieves the primary key ID of a row where a specific column matches a given value.
        Uses parameterized queries for security.

        Args:
            tablename (str): The name of the table.
            column (str): The name of the column to check.
            value (Any): The value to search for in the specified column.

        Returns:
            Union[int, None]: The ID if found, None if not found.
                            Returns 0 on SQL error (consider raising exception instead for clearer error handling).
        """
        primary_key_col = self.get_primary_key(tablename)
        if not primary_key_col:
            LOG.error(f"Could not determine primary key for table {tablename}. Cannot get value ID.")
            return 0 # Or raise an exception

        # Use backticks for table and column names to handle reserved words or special chars
        # Use %s for values in parameterized queries
        query = f"SELECT `{primary_key_col}` FROM `{tablename}` WHERE `{column}` = %s"
        params = (value,)
        LOG.debug(f"EXECUTING: {query} with params {params}")

        with self.cursor() as cursor:
            try:
                cursor.execute(query, params)
                result = cursor.fetchone()
                if result:
                    return result[0]
                else:
                    return None # Value not found
            except Error as err:
                LOG.error(f"SQL error in get_value_id for table {tablename}, column {column}, value {value}: {err}")
                return 0

    @reconnect_on_operational_error
    def get_value_id_multiple(self, tablename: str, **colvals) -> Union[int, None]:
        """
        Retrieves the primary key ID by matching multiple column-value pairs.

        Uses parameterized queries for security.

        Args:
            tablename (str): The name of the table.
            **colvals: Expects "columns" (list of column names) and "values" (list of corresponding values).

        Returns:
            Union[int, None]: The ID if a unique match is found, None otherwise.
                              Returns -1 on SQL error or if columns/values mismatch.
                              (Consider raising exceptions for clearer error handling).
        """
        primary_key_col = self.get_primary_key(tablename)
        if not primary_key_col:
            LOG.error(f"Could not determine primary key for table {tablename}. Cannot get value ID.")
            return -1

        columns = colvals.get("columns")
        values = colvals.get("values")

        if not columns or not values or len(columns) != len(values):
            LOG.error("Columns and values must be provided and have the same length.")
            return -1

        # Build the WHERE clause with %s placeholders
        where_clauses = [f"`{col}` = %s" for col in columns]
        query = f"SELECT `{primary_key_col}` FROM `{tablename}` WHERE {' AND '.join(where_clauses)}"
        params = tuple(values) # Ensure values are passed as a tuple

        LOG.debug(f"EXECUTING: {query} with params {params}")

        with self.cursor() as cursor:
            try:
                cursor.execute(query, params)
                result = cursor.fetchone()
                if result:
                    return result[0]
                else:
                    return None # No matching record found
            except Error as err:
                LOG.error(f"SQL error in get_value_id_multiple for table {tablename}, criteria {dict(zip(columns, values))}: {err}")
                return -1

    @reconnect_on_operational_error
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
            primary_key_col = self.get_primary_key(tablename)
            query = f"SELECT `{column}` FROM `{tablename}` WHERE `{primary_key_col}` = %s"
            params = (idx,)
            LOG.debug("EXECUTING: %s with params %s", query, params)

            try:
                cursor.execute(query, params)
                result = cursor.fetchone()
                return result[0] if result else None
            except Error as err:
                LOG.error(f"SQL error in get_value_by_id: {err}")
                return 0

    @reconnect_on_operational_error
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

    @reconnect_on_operational_error
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

    @reconnect_on_operational_error
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
                insert_id_tuple = cursor.fetchone()
                if insert_id_tuple and insert_id_tuple[0] is not None:
                    new_id = int(insert_id_tuple[0])
                    if new_id > 0:
                        return new_id
                    else:
                        # LAST_INSERT_ID() returned 0, meaning no new auto-increment ID was generated.
                        # This is an issue if we expected a new row.
                        LOG.warning(f"LAST_INSERT_ID() returned {new_id} for table {tablename}. Assuming insert failed to generate a new row ID.")
                        return -1
                else:
                    LOG.warning(f"LAST_INSERT_ID() returned no result (None) after insert into {tablename}. Assuming insert failed.")
                    return -1

    @reconnect_on_operational_error
    def insert_single_row2(self, tablename, dbdata: dict):
        """
        Inserts a single row into the specified table in the connected MySQL database
        by passing a dictionary of column names and values, using named placeholders.

        :param tablename: The name of the table into which to insert the row.
        :param dbdata: A dictionary containing the column names as keys and the corresponding values to insert.
        :return: The ID of the inserted row, or -1 if an error occurs.
        """
        with self.cursor() as cursor:
            columns = list(dbdata.keys())

            column_names_str = ", ".join([f"`{col}`" for col in columns])
            value_placeholders_str = ", ".join([f"%({col})s" for col in columns]) # e.g., %(uuid)s, %(shows_showID)s

            query = f"INSERT INTO `{tablename}` ({column_names_str}) VALUES ({value_placeholders_str})"

            LOG.debug(f"EXECUTING (named placeholders): {query} with data_dict: {dbdata}")

            try:
                cursor.execute(query, dbdata)
            except Error as err:
                LOG.error(f"DBManager insert_single_row2 SQL Error for table {tablename}: {err}")
                LOG.error(f"Query: {query}")
                LOG.error(f"Data: {dbdata}")
                return -1
            else:
                cursor.execute("SELECT LAST_INSERT_ID();")
                insert_id_row = cursor.fetchone()
                if insert_id_row:
                    return insert_id_row[0]
                else:
                    LOG.warning(f"LAST_INSERT_ID() returned no result after insert into {tablename}.")
                    return -1

    @reconnect_on_operational_error
    def update_single_row(self, tablename: str, key: Any, **colvals) -> int:
        """
        Updates a single row in the specified table identified by its primary key.

        Constructs a single SQL UPDATE statement for efficiency and uses parameterized
        queries for security.

        Args:
            tablename (str): The name of the table to update.
            key (Any): The primary key value of the row to update.
            **colvals: Expects "columns" (list of column names to update) and
                       "values" (list or tuple of corresponding new values).

        Returns:
            int: 1 if the update is successful (row found and updated, or row not found but no SQL error),
                 0 if an error occurs (e.g., SQL error, columns/values mismatch).
                 Note: `cursor.rowcount` could be checked to see if a row was actually affected.
        """
        primary_key_column = self.get_primary_key(tablename)
        if not primary_key_column:
            LOG.error(f"Could not determine primary key for table {tablename}. Cannot update row.")
            return 0

        columns_to_update = colvals.get("columns")
        new_values = colvals.get("values")

        if not columns_to_update or not new_values or not isinstance(columns_to_update, list) or not isinstance(new_values, (list, tuple)):
            LOG.error("Invalid 'columns' or 'values' provided. Both must be lists/tuples.")
            return 0

        if len(columns_to_update) != len(new_values):
            LOG.error("Number of columns to update does not match number of new values.")
            return 0

        if not columns_to_update: # No columns to update
            LOG.info(f"No columns specified for update in table {tablename}. No action taken.")
            return 1 # Or 0, depending on desired semantics for "no-op"

        # Construct the SET part of the query, e.g., "`col1` = %s, `col2` = %s"
        set_clause_parts = [f"`{col}` = %s" for col in columns_to_update]
        set_clause = ", ".join(set_clause_parts)

        query = f"UPDATE `{tablename}` SET {set_clause} WHERE `{primary_key_column}` = %s"

        # Parameters for the query: all new values first, then the key for the WHERE clause
        params = tuple(new_values) + (key,)

        LOG.debug(f"EXECUTING: {query} with params {params}")

        with self.cursor() as cursor:
            try:
                cursor.execute(query, params)
                # self.conn.commit() # Assuming save/commit is handled by the caller (DBBridge)
                # LOG.debug(f"Update executed for table {tablename}, PK {key}. Affected rows: {cursor.rowcount}")
                return 1 # Indicates successful execution of the query
            except Error as err:
                LOG.error(f"SQL error updating row in table {tablename} for PK {key}: {err}")
                LOG.error(f"Query: {query}, Params: {params}")
                return 0

    @reconnect_on_operational_error
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

    @reconnect_on_operational_error
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

    @reconnect_on_operational_error
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

    @reconnect_on_operational_error
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
            primary_key_col = self.get_primary_key(tablename)
            query = f"DELETE FROM `{tablename}` WHERE `{primary_key_col}` = %s"
            params = (idx,)
            LOG.debug("EXECUTING: %s with params %s", query, params)
            try:
                cursor.execute(query, params)
                return 1
            except Error as err:
                LOG.error(f"SQL error in remove_by_id: {err}")
                return 0

    def as_list(self, db_data):
        db_data = list(tuple(zip(*db_data))[0])
        return db_data

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
        ''' Closes the database connection gracefully. '''
        if self.conn and self.conn.is_connected():
            try:
                self.conn.close()
                LOG.debug('Connection closed.')
            except Error as e:
                LOG.error(f"Error while closing connection: {e}")
        else:
            LOG.debug('No active connection to close.')
        if logging.getLogger().hasHandlers():
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