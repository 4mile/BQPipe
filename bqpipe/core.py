import datetime
import logging
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import BadRequest, NotFound
from . import helpers


def list_datasets() -> list:
    """Return list of all dataset names (as strings) for your authenticated project."""
    bigquery_client = bigquery.Client()
    project = bigquery_client.project
    datasets = list(bigquery_client.list_datasets())
    dataset_list = []

    if datasets:
        for dataset in datasets:  # API request(s)
            dataset_list.append(dataset.dataset_id)
    else:
        logging.info("{} project does not contain any datasets.".format(project))

    return dataset_list


def list_tables_in_dataset(dataset: str) -> pd.DataFrame:
    """Return list of tables (as strings) in given BigQuery dataset."""
    list_tables_sql = """
        SELECT table_name
        FROM   {}.INFORMATION_SCHEMA.TABLES
    """.format(dataset)
    logging.debug('Generated list tables metadata SQL:\n{}.'.format(list_tables_sql))

    try:
        bigquery_client = bigquery.Client()

        query_job = bigquery_client.query(list_tables_sql)
        result_df = query_job.to_dataframe()
        list_result = result_df['table_name'].values.tolist()

        if not list_result:
            logging.warning('The dataset "{}" you''ve specified consists of no tables.'.format(dataset))

        return list_result

    except NotFound as not_found_error:
        logging.error('The dataset you''ve specified was not found for your given credentials. Ref: {}'.format(
                        not_found_error))
        raise RuntimeError('Dataset {} not found.'.format(dataset))
    except BadRequest as bad_request_error:
        logging.error('Request is invalid, please review and confirm your input dataset is valid. Ref: {}.'.format(
                        bad_request_error))
        raise ValueError('Request for given dataset "{}" was invalid. Information Schema SQL request: {}'.format(
            dataset, list_tables_sql))


def get_table_schema(dataset: str, table: str) -> list:
    """Retrieve list of dictionaries representing the schema of given table in given dataset."""
    bigquery_client = bigquery.Client()
    list_table_schema_sql = """
        SELECT  C.column_name, C.data_type, C.is_nullable, CFP.description
        FROM    {dataset_name}.INFORMATION_SCHEMA.COLUMNS C
                INNER JOIN {dataset_name}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS CFP
                    ON C.table_catalog = CFP.table_catalog
                    AND C.table_schema = CFP.table_schema
                    AND C.table_name = CFP.table_name
                    AND C.column_name = CFP.column_name
        WHERE   C.table_name = '{table_name}'
                AND CFP.column_name = CFP.field_path
    """.format(dataset_name=dataset, table_name=table)
    logging.debug('Generated list tables metadata SQL:\n{}.'.format(list_table_schema_sql))

    query_job = bigquery_client.query(list_table_schema_sql)
    result_df = query_job.to_dataframe()
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


def fetch_table_data(table: str, fields: tuple = '*', where_clause: str = '1 = 1', number_of_rows: int = 0,
                     dataset='analytics') -> pd.DataFrame:
    """Download specified table as Pandas DataFrame from specified BigQuery table.

    Args:
        # bigquery_client: The Google Cloud BigQuery client for your Project.
        table: String representing the table source to query.
        fields: Tuple of fields to pull from the table, defaults to all fields.
        where_clause: String representing a SQL Where clause applied when fetching the data, default is no Where clause.
        number_of_rows: Integer representing the number of rows to return, default to all rows in table.
        dataset: The Dataset the table is located in.
    Returns:
        Pandas DataFrame representing the query output.
    """
    try:
        bigquery_client = bigquery.Client()

        print(fields)
        print(len(fields))
        if isinstance(fields, tuple) and len(fields) > 1:
            select_clause = 'SELECT * ' if fields == '*' else 'SELECT {} '.format(', '.join(fields))
        else:
            select_clause = 'SELECT * ' if fields == '*' else 'SELECT {} '.format(fields)

        from_clause = 'FROM {}.{} '.format(dataset, table)
        limit_clause = '' if number_of_rows < 1 else ' LIMIT {}'.format(number_of_rows)

        if where_clause[:4].lower() == 'where':
            where_clause = where_clause + ' '
        else:
            where_clause = 'WHERE {} '.format(where_clause)

        fetch_table_sql = select_clause + from_clause + where_clause + limit_clause
        logging.debug('Fetch table generated SQL:\n' + fetch_table_sql)
        query_job = bigquery_client.query(fetch_table_sql)

        return query_job.to_dataframe()

    except NotFound as not_found_error:
        logging.error('One of the objects specified in your query does not exist. Please review and confirm the\n'
                      'table exists and is spelled correctly with the correct dataset specified.\nRef: {}'.format(
                        not_found_error))
        raise RuntimeError('Requested table "{}" in dataset {} not found.'.format(table, dataset))
    except BadRequest as bad_request_error:
        logging.error('Your inputs created an invalid SQL request, please review inputs and confirm your SQL is valid.'
                      'Ref: {}.'.format(bad_request_error))
        raise ValueError('Request is invalid, confirm inputs formed correctly. Review generated SQL and adjust input '
                         'parameters accordingly to fix the SQL request.')


def fetch_sql_output(sql_select_statement: str) -> pd.DataFrame:
    """Run SQL on BigQuery and fetch output as Pandas DataFrame.

    Args:
        sql_select_statement: String representing the SELECT query to run in BigQuery.
    Returns:
        Pandas DataFrame representing the query output.
    """
    try:
        bigquery_client = bigquery.Client()
        query_job = bigquery_client.query(sql_select_statement)

        return query_job.to_dataframe()
    except NotFound as not_found_error:
        logging.error('One of the SQL objects specified in your query does not exist. Please review and confirm all\n'
                      'tables in the query are spelled correctly with their correct dataset specified.\nRef: {}'.format(
                        not_found_error))
        raise RuntimeError('SQL query references object(s) which do not exist, review SQL and confirm all objects '
                           'exist.')
    except BadRequest as bad_request_error:
        logging.error('Input SQL is invalid, please review and confirm your SQL is valid. Ref: {}.'.format(
                        bad_request_error))
        raise ValueError('Invalid SQL, review and fix any syntax errors.')


def write_to_bigquery(dataframe: pd.DataFrame, destination_table: str,
                      insert_type: str = 'append', accept_incomplete_schema: bool = False,
                      create_table_if_missing: bool = False, custom_new_table_schema: list = None,
                      destination_dataset: str = 'analytics', accept_capital_letters: bool = False) -> tuple:
    """Write data into specified BigQuery destination table, with option to create a new table.

    If you would like to create a new table, set create_if_missing to True. By default, the script will autodetect
    your schema the best it can. However, it's best to specify the schema directly, especially if you want to specify
    Nullability, add a description, or ensure the column type is correct. Note: Please use the snake_case convention for
    column names for consistency.

    The custom_new_table_schema takes the following format:
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
        # bigquery_client: The Google Cloud BigQuery client for your Project.
        dataframe: Pandas DataFrame representing the data to write to BigQuery.
        destination_table: String representing the destination table to write the DataFrame to.
        insert_type: Method to upload file, either 'append' or 'truncate' (truncates existing table), default 'append'.
        accept_incomplete_schema: Boolean, specify True if sending DataFrame that does not perfectly match the BigQuery
                                  table's schema (non-included columns will be populated with Null). Default is False.
        create_table_if_missing: Boolean, specify True if the specified table should be created if it doesn't already
                            exist. Default is True (throws error if table doesn't already exist).
        custom_new_table_schema: Tuple of dictionaries representing the schema for a new table (see above for details).
        destination_dataset: The destination Dataset of the table to create/insert into.
        accept_capital_letters: Boolean, Set to True if you'd like to work with a table with capital letters. BigQuery
                                generally stipulates camel_case, so this should generally not be used. Default False.
    Returns:
        Tuple with the response of the table write API request.
    """
    destination_dataset = destination_dataset.strip()
    destination_table = destination_table.strip()
    insert_type = insert_type.lower().strip()
    insert_type_acceptable_values = ('append', 'truncate')

    if not accept_capital_letters:
        destination_dataset = destination_dataset.lower()
        destination_table = destination_table.lower()

    if insert_type not in insert_type_acceptable_values:
        raise ValueError('Specified insert_type parameter {} is not an acceptable value for the parameter. insert_type '
                         'must be one of the following: {}.'.format(insert_type, str(insert_type_acceptable_values)))

    bigquery_client = bigquery.Client()
    table_already_exists = True
    new_table_schema = []

    if not helpers.does_table_exist(bigquery_client, destination_table, dataset=destination_dataset):
        table_already_exists = False
        if create_table_if_missing:
            logging.info('Creating missing specified table output "{}" in Dataset "{}" as '
                         'create_if_missing was set to True'.format(destination_table, destination_dataset))
            if custom_new_table_schema is not None:
                logging.info('Creating table with user-specified custom schema.')
                new_table_schema = helpers.get_detected_schema(dataframe, tuple(custom_new_table_schema))
            else:
                logging.info('Creating table without specified schema; auto-detecting schema to append table.')
        else:
            logging.error('Write to BigQuery failed as table "{table}" does not exist in Dataset "{dataset}". Either\n'
                          'update the specified table name to an existing table, or set function parameter\n'
                          'create_if_missing to True to create "{dataset}.{table}".'.format(
                            table=destination_table, dataset=destination_dataset))
            raise ValueError('Specified table "{}" does not exist.'.format(destination_table))

    # Add appended created_at column to DataFrame
    dataframe['created_at'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
    if accept_incomplete_schema:
        job_config.allow_jagged_rows = True
    job_config.ignore_unknown_values = True

    if custom_new_table_schema is None:
        job_config.autodetect = True
    else:
        job_config.schema = new_table_schema

    if insert_type == 'append' and table_already_exists:
        logging.info('Appending input data to existing table {}.'.format(destination_table))
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    elif insert_type == 'truncate' and table_already_exists:
        logging.warning('Insert type set to Truncate, table will be truncated to prior to writing input data.')
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    else:
        logging.info('Creating new table "{}" which will be populated with input data.'.format(destination_table))
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY

    table_id = destination_dataset + '.' + destination_table
    load_job = bigquery_client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )

    assert load_job.job_type == 'load'
    load_response = load_job.result()  # Waits for table load to complete.
    assert load_job.state == 'DONE'
    if load_job.error_result:
        raise RuntimeError(load_job.errors)
    return load_response
