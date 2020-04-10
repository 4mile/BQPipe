import os
import datetime
import logging
import pandas as pd
from typing import Union

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

import snowflake.connector
from snowflake.connector.errors import Error

# Destination dataset for writing tables, only dataset that users can write to.
APP_NAME = 'BQPipe'
DESTINATION_DATASET = 'analytics'


class SnowflakeClient(object):
    """Client with configuration to run Snowflake API requests."""
    def __init__(self, snowflake_account_name: str, authentication_method: str = 'KEY_PAIR',
                 authentication_params: dict = None, connection_details: dict = None):
        """Initialize parameters for Snowflake Client and authentication.

        Args:
            snowflake_account_name: Your Snowflake account name, just the prefix. For example, if your full URL is
                                    'mycompany.snowflakecomputing.com', just supply 'mycompany'.
            authentication_method: String representing the authentication method, either 'KEY_PAIR', or 'USER_LOGIN'.
                                   Depending on choice of method you'll need to populate 'authentication_params'
                                   accordingly (see below).
            authentication_params: Dict representing the authentication details to be supplied to the Snowflake
                                   connection and authentication API calls, requirements vary per authentication method.
                                   If using KEY_PAIR, supply rsa_key_path with the file path to your generated RSA key,
                                   your username to username and your private key passphrase to private_passphrase.
                                   If using USER_LOGIN, supply user and password parameters for Snowflake login.
            connection_details: Dict representing additional Snowflake client connection details. Commonly used params
                                include 'warehouse' for the default virtual warehouse, 'database' for the default
                                database, and 'schema' for the default schema.
        """
        self.account_name = snowflake_account_name
        self.authentication_method = authentication_method.upper()

        logging.info('Attempting to authenticate with authentication method: {}'.format(self.authentication_method))
        auth_and_connection_params = self._validate_params(authentication_params, connection_details)

        if self.authentication_method == 'KEY_PAIR':
            self.client = self._authenticate_with_key_pair(**auth_and_connection_params)
        elif self.authentication_method == 'USER_LOGIN':
            self.client = self._authenticate_with_user_credentials(**auth_and_connection_params)
        else:
            logging.error('You have chosen an invalid authentication method, please use either "KEY_PAIR" or '
                          '"USER_LOGIN" and supply appropriate login credentials accordingly')
            exit(1)

        self.cursor = self.client.cursor()
        self.cursor.check_can_use_pandas()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.client:
            self.client.close()

    def _validate_params(self, authentication_params, connection_details):
        """Validate input parameters based on authentication choice"""
        if authentication_params:
            if self.authentication_method == 'KEY_PAIR':
                if (not authentication_params.get('user')) or (not authentication_params.get('rsa_key_path')):
                    logging.error('You have chosen key pair authentication but failed to specify necessary params '
                                  'to the authentication_params input. This input dictionary should have keys '
                                  '"user" and "rsa_key_path" as well as "private_passphrase" (if applicable).')
                    exit(1)
            elif self.authentication_method == 'USER_LOGIN':
                if (not authentication_params.get('user')) or (not authentication_params.get('password')):
                    logging.error('You have chosen user login authentication but failed to specify necessary params '
                                  'to the authentication_params input. This input dictionary should have keys '
                                  '"user" and "password".')
                    exit(1)
        else:
            logging.error('You must include the authentication_params input with auth details to use the client.')
            exit(1)

        if connection_details:
            return {**authentication_params, **connection_details}
        else:
            return authentication_params

    def _authenticate_with_key_pair(self, rsa_key_path: str, private_passphrase: str = None,
                                    connection_details: dict = None,
                                    **kwargs) -> snowflake.connector.SnowflakeConnection:
        with open(rsa_key_path, "rb") as key:
            p_key = serialization.load_pem_private_key(
                key.read(),
                password=private_passphrase.encode(),
                backend=default_backend()
            )

        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        return snowflake.connector.connect(
            account=self.account_name,
            application=APP_NAME,
            validate_default_parameters=True,
            protocol='https',
            private_key=pkb,
            **kwargs
        )

    def _authenticate_with_user_credentials(self, **kwargs) -> snowflake.connector.SnowflakeConnection:
        return snowflake.connector.connect(
            account=self.account_name,
            application=APP_NAME,
            validate_default_parameters=True,
            protocol='https',
            **kwargs
        )

    @property
    def region(self) -> str:
        """Return Snowflake account region as a string for the connected Snowflake account."""
        return self.client.region

    @property
    def current_user(self) -> str:
        """Return Snowflake account region as a string for the connected Snowflake account."""
        return self.client.user

    @property
    def current_warehouse(self) -> str:
        """Return Snowflake account region as a string for the connected Snowflake account."""
        return self.client.warehouse

    @property
    def current_database(self) -> str:
        """Return Snowflake account region as a string for the connected Snowflake account."""
        return self.client.database

    @property
    def current_schema(self) -> str:
        """Return Snowflake account region as a string for the connected Snowflake account."""
        return self.client.schema

    @property
    def current_role(self) -> str:
        """Return Snowflake account region as a string for the connected Snowflake account."""
        return self.client.role

    def set_database(self, database_name: str):
        """Set the active database."""
        self.cursor.execute("USE DATABASE {};".format(database_name.upper()))

    def set_schema(self, schema_name: str):
        """Set the active database schema (database must be set)."""
        self.cursor.execute("USE SCHEMA {};".format(schema_name.upper()))

    def set_role(self, role_name: str):
        """Set the active role."""
        self.cursor.execute("USE ROLE {};".format(role_name.upper()))

    def set_warehouse(self, warehouse_name: str):
        """Set the active warehouse."""
        self.cursor.execute("USE WAREHOUSE {};".format(warehouse_name.upper()))

    def list_databases(self) -> list:
        """Return list of all database names and details on the account that user has permission to access."""
        self.cursor.execute("SHOW DATABASES;")
        return self.cursor.fetchall()

    def list_schemas(self, database: str = None) -> list:
        """Return list of all schemas details in given database that user has permission to access."""
        if not database:
            database = self.current_database
        self.cursor.execute("SHOW SCHEMAS IN DATABASE {}".format(database))
        return self.cursor.fetchall()

    def list_tables(self, database: str = None, schema: str = None, set_uppercase: bool = True) -> pd.DataFrame:
        """Return list of table details in given Snowflake schema that user has permission to access."""
        if database and set_uppercase:
            database = database.upper()
        if schema and set_uppercase:
            schema = schema.upper()
        
        if not database and self.current_database:
            database = self.current_database
        elif not database and not self.current_database:
            logging.error('You must either specify a database or have a current database set to list tables')
            exit(1)
        if not schema and self.current_schema:
            schema = self.current_schema
        elif not schema and not self.current_schema:
            logging.error('You must either specify a database or have a current database set to list tables')
            exit(1)
        
        list_tables_sql = """
            SELECT  table_name
            FROM    {}.INFORMATION_SCHEMA.TABLES
            WHERE   table_schema = '{}'
        """.format(database, schema)
        self.cursor.execute(list_tables_sql)
        logging.debug('Generated list tables metadata SQL:\n{}.'.format(list_tables_sql))

        return self.cursor.fetchall()

    def get_table_schema(self, schema: str, table: str) -> list:
        """Retrieve list of dictionaries representing the schema of given table in given dataset."""
        list_table_schema_sql = """
            SELECT  C.column_name, C.data_type, C.is_nullable, CFP.description
            FROM    {schema_name}.INFORMATION_SCHEMA.COLUMNS C
                    INNER JOIN {schema_name}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS CFP
                        ON C.table_catalog = CFP.table_catalog
                        AND C.table_schema = CFP.table_schema
                        AND C.table_name = CFP.table_name
                        AND C.column_name = CFP.column_name
            WHERE   C.table_name = '{table_name}'
                    AND CFP.column_name = CFP.field_path
        """.format(schema_name=schema, table_name=table)
        logging.debug('Generated list tables metadata SQL:\n{}.'.format(list_table_schema_sql))

        self.cursor.execute(list_table_schema_sql)
        result_df = self.cursor.fetch_pandas_all()
        dict_output = result_df.to_dict(orient='records')

        for column in dict_output:
            column['name'] = column.pop('column_name')
            column['field_type'] = column.pop('data_type')
            if column['is_nullable'] == 'YES':
                column['mode'] = 'REQUIRED'
            else:
                column['mode'] = 'NULLABLE'
            column.pop('is_nullable')

        return dict_output

    def fetch_table_data(self, table: str, fields: Union[tuple, str] = '*', where_clause: str = '1 = 1',
                         number_of_rows: int = 0, schema: str = 'public') -> pd.DataFrame:
        """Download specified table as Pandas DataFrame from specified Snowflake table.

        Args:
            table: String representing the table source to query.
            fields: Tuple of fields to pull from the table, defaults to all fields.
            where_clause: String representing a SQL Where clause applied when fetching data, default is no Where clause.
            number_of_rows: Integer representing the number of rows to return, default to all rows in table.
            schema: The Schema the table is located in, default to schema "public".
        Returns:
            Pandas DataFrame representing the query output.
        """
        try:
            if isinstance(fields, tuple) and len(fields) > 1:
                select_clause = 'SELECT * ' if fields == '*' else 'SELECT {} '.format(', '.join(fields))
            else:
                select_clause = 'SELECT * ' if fields == '*' else 'SELECT {} '.format(fields)

            from_clause = 'FROM {}.{} '.format(schema, table)
            limit_clause = '' if number_of_rows < 1 else ' LIMIT {}'.format(number_of_rows)

            if where_clause[:4].lower() == 'where':
                where_clause = where_clause + ' '
            else:
                where_clause = 'WHERE {} '.format(where_clause)

            fetch_table_sql = select_clause + from_clause + where_clause + limit_clause
            logging.debug('Fetch table generated SQL:\n' + fetch_table_sql)
            self.cursor.execute(fetch_table_sql)

            return self.cursor.fetch_pandas_all()

        except Error as e:
            logging.error('One of the objects specified in your query does not exist or the query connection failed. '
                          'Please review and confirm the table exists and is spelled correctly with the correct '
                          'dataset specified.\nError Details: {}'.format(e))
            exit(1)

    def fetch_sql_output(self, sql_select_statement: str) -> pd.DataFrame:
        """Run SQL on Snowflake and fetch output as Pandas DataFrame.

        Args:
            sql_select_statement: String representing the SELECT query to run in Snowflake.
        Returns:
            Pandas DataFrame representing the query output.
        """
        try:
            self.cursor.execute(sql_select_statement)
            return self.cursor.fetch_pandas_all()

        except Error as e:
            logging.error('One of the SQL objects specified in your query does not exist or the SQL is invalid. Please '
                          'review and confirm all tables in the query are spelled correctly with their correct '
                          'dataset specified. Error details: {}'.format(e))
            exit(1)

    def insert_into_table(self, dataframe: pd.DataFrame, destination_table: str, insert_type: str = 'append',
                          accept_incomplete_schema: bool = False, create_table_if_missing: bool = False,
                          custom_table_schema: list = None, accept_capital_letters: bool = False) -> tuple:
        """Write data into specified Snowflake destination table, with option to create a new table.

        If you would like to create a new table, set create_if_missing to True. By default, the script will autodetect
        your schema the best it can. However, it's best to specify the schema directly, especially if you want to
        specify Nullability, add a description, or ensure the column type is correct. Note: Please use the snake_case
        convention for column names for consistency.

        The custom_table_schema takes the following format:
        sample_schema = [
            {
                'name': 'experiment_name',  # (String) The name of the field, use camel_case.
                'field_type': 'string',  # (String) Options: string, integer, float, boolean, date, timestamp, or bytes.
                'mode': 'required',  # (String, Optional) Either nullable or required, default is nullable.
                'description': 'The user-friendly name for the experiment'  # (String, Optional) Default is None.
            },
            {
                'name': 'account_id',
                'field_type': 'integer'
            }
        ]

        Args:
            dataframe: Pandas DataFrame representing the data to write to BigQuery.
            destination_table: String representing the destination table to write the DataFrame to.
            insert_type: (Optional) String representing the Method to upload the file, either 'append' or 'truncate'
                         (truncates existing table), default 'append'.
            accept_incomplete_schema: (Optional) Boolean, specify True if sending DataFrame that does not perfectly
                                      match the BigQuery table's schema (non-included columns will be populated with
                                      Null). Default is False.
            create_table_if_missing: (Optional) Boolean, specify True if the specified table should be created if it
                                     doesn't already exist. Default is True (throws error if table doesn't exist).
            custom_table_schema: (Optional) Tuple of dictionaries representing the schema for a new table (see above for
                                 further details on example schema).
            accept_capital_letters: (Optional) Boolean, Set to True if you'd like to work with a table with capital
                                    letters. BigQuery naming conventions typically follow camel_case, so this should
                                    generally not be used. Default is False.
        Returns:
            Tuple with the response of the table write API request.
        """
        destination_table = destination_table.strip()
        insert_type = insert_type.lower().strip()
        insert_type_acceptable_values = ('append', 'truncate')

        if not accept_capital_letters:
            destination_table = destination_table.lower()

        if insert_type not in insert_type_acceptable_values:
            raise ValueError('Specified insert_type parameter {} is not an acceptable value. insert_type must be '
                             'one of the following: {}.'.format(insert_type, str(insert_type_acceptable_values)))

        table_already_exists = True
        new_table_schema = []

        if not does_table_exist(self.client, destination_table, schema=DESTINATION_DATASET):
            table_already_exists = False
            if create_table_if_missing:
                logging.info('Creating missing specified table output "{}" in Dataset "{}" as '
                             'create_if_missing was set to True'.format(destination_table, DESTINATION_DATASET))
                if custom_table_schema is not None:
                    logging.info('Creating table with user-specified custom schema.')
                    new_table_schema = get_detected_schema(dataframe, tuple(custom_table_schema))
                else:
                    logging.info('Creating table without specified schema; auto-detecting schema to append table.')
            else:
                logging.error('Write to BigQuery failed as table "{table}" does not exist in Dataset "{dataset}".'
                              'Either update the specified table name to an existing table, or set function parameter\n'
                              'create_if_missing to True to create "{dataset}.{table}".'.format(
                                table=destination_table, dataset=DESTINATION_DATASET))
                raise ValueError('Specified table "{}" does not exist.'.format(destination_table))

        # Add appended created_at column to DataFrame
        created_at_col = 'bq_created_at'
        if dataframe.shape[0] > 0:
            dataframe[created_at_col] = pd.Timestamp(datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
            output_schema = new_table_schema
        else:
            created_at_schema = {
                'name': created_at_col,
                'field_type': 'timestamp',
                'mode': 'required',
                'description': 'Timestamp for time field was added to BigQuery.'
            }
            output_schema = new_table_schema.append(created_at_schema)
            logging.debug(output_schema)

        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
        if accept_incomplete_schema:
            job_config.allow_jagged_rows = True
        job_config.ignore_unknown_values = True

        if custom_table_schema is None:
            job_config.autodetect = True
        else:
            job_config.schema = output_schema

        if insert_type == 'append' and table_already_exists:
            logging.info('Appending input data to existing table {}.'.format(destination_table))
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        elif insert_type == 'truncate' and table_already_exists:
            logging.warning('Insert type set to Truncate, table will be truncated to prior to writing input data.')
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:
            logging.info('Creating new table "{}" which will be populated with input data.'.format(destination_table))
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY

        table_id = DESTINATION_DATASET + '.' + destination_table
        load_job = self.client.load_table_from_dataframe(
            dataframe, table_id, job_config=job_config
        )

        assert load_job.job_type == 'load'
        load_response = load_job.result()  # Waits for table load to complete.
        assert load_job.state == 'DONE'
        if load_job.error_result:
            raise RuntimeError(load_job.errors)

        return load_response
    
    
def does_table_exist(snowflake_client: snowflake.connector.SnowflakeConnection,
                     table: str, schema: str = 'analytics') -> bool:
    """Check if given table from given schema exists in Snowflake, return True if so."""
    try:
        cursor = snowflake_client.cursor()
        existence_check_sql = """
        SELECT  COUNT(*)
        FROM    information_schema.tables
        WHERE   table_schema = '{}'
                AND table_name = '{}'
        """.format(schema, table)
        cursor.execute(existence_check_sql)
        result = cursor.fetchone()
        if result:
            logging.info('Table "{}" in Schema "{}" exists in Snowflake.'.format(table, schema))
            return True
    except errors.Error as error:
        logging.warning('Table "{}" does not exist in Snowflake Schema "{}" or difficulty connecting to confirm if '
                        'table exists in Snowflake. Ref: {}.'.format(table, schema, error))
        return False


def get_detected_schema(dataframe: pd.DataFrame, custom_schema: tuple = None) -> list:
    """Return tuple of dictionaries with detected schema (auto-detect if custom_schema not specified)."""
    output_schema = []
    field_names = []
    if custom_schema:
        for schema in custom_schema:
            if 'name' not in schema or 'field_type' not in schema:
                logging.error(
                    'You have at least one schema column defined without a name or field_type. All columns in\n'
                    'custom schema must have specified keys "name" and "field_type". Update your custom schema.')
                sys.exit()
            if 'mode' not in schema:
                schema['mode'] = 'NULLABLE'
            name, f_type, mode = schema['name'].lower(), schema['field_type'].upper(), schema['mode'].upper()
            if 'description' not in schema:
                column_schema = bigquery.SchemaField(name, f_type, mode=mode)
            else:
                column_schema = bigquery.SchemaField(name, f_type, mode=mode, description=schema['description'])
            output_schema.append(column_schema)
            field_names.append(name)
    else:
        logging.error("Auto-detect not yet implemented, please specify custom schema.")
        sys.exit()

    if 'created_at' in field_names:
        logging.error('You''ve specified a "created_at" field, however, this field is added automatically during\n'
                      'upload to capture upload to BigQuery time and is a BQPipe system field. Please choose another'
                      'field name for your field.')
        sys.exit()
    else:
        created_at_schema = bigquery.SchemaField('created_at', 'STRING', mode='REQUIRED',
                                                 description='Date inserted into BigQuery.')
        output_schema.append(created_at_schema)

    return output_schema
