import datetime
import logging
import os
import sys
import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import BadRequest, NotFound

logging.basicConfig(level='WARNING')


def fetch_from_bigquery(table: str, fields: tuple = '*', dataset='analytics') -> pd.DataFrame:
    """Download specified table as Pandas DataFrame from specified GCP BigQuery table.

    Args:
        # bigquery_client: The Google Cloud BigQuery client for your Project.
        table: String representing the table source to query.
        fields: Tuple of fields to pull from the table, defaults to all fields.
        dataset: The Dataset the table is located in.
    Returns:
        Pandas DataFrame representing the query output.
    """
    print('Not yet implemented.')


def fetch_sql_output_bigquery(sql_select_statement: str) -> pd.DataFrame:
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
    except BadRequest as bad_request_error:
        logging.error('Input SQL is invalid, please review and confirm your SQL is valid. Ref: {}.'.format(
                        bad_request_error))


def write_to_bigquery(dataframe: pd.DataFrame, destination_table: str,
                      insert_type: str = 'Append', accept_incomplete_schema: bool = False,
                      create_table_if_missing: bool = False, custom_new_table_schema: list = None,
                      destination_dataset: str = 'analytics') -> tuple:
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
        insert_type: Method to upload file, either 'Append' or 'Truncate' (truncates existing table), default 'Append'.
        accept_incomplete_schema: Boolean, specify True if sending DataFrame that does not perfectly match the BigQuery
                                  table's schema (non-included columns will be populated with Null). Default is False.
        create_table_if_missing: Boolean, specify True if the specified table should be created if it doesn't already
                            exist. Default is True (throws error if table doesn't already exist).
        custom_new_table_schema: Tuple of dictionaries representing the schema for a new table (see above for details).
        destination_dataset: The destination Dataset of the table to create/insert into.
    Returns:
        Tuple with the response of the table write API request.
    """
    bigquery_client = bigquery.Client()
    table_already_exists = True
    new_table_schema = []

    if not _does_table_exist(bigquery_client, destination_table):
        table_already_exists = False
        if create_table_if_missing:
            logging.info('Creating missing specified table output "{}" in Dataset "{}" as '
                         'create_if_missing was set to True'.format(destination_table, destination_dataset))
            if custom_new_table_schema is not None:
                logging.info('Creating table with user-specified custom schema.')
                new_table_schema = _get_detected_schema(dataframe, tuple(custom_new_table_schema))
            else:
                logging.info('Creating table without specified schema; auto-detecting schema to append table.')
        else:
            logging.error('Write to BigQuery failed as table "{table}" does not exist in Dataset "{dataset}". Either\n'
                          'update the specified table name to an existing table, or set function parameter\n'
                          'create_if_missing to True to create "{table}" as a new table.'.format(
                            table=destination_table, dataset=destination_dataset))
            return False, 'Specified table does not exist.'

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

    if insert_type.lower() == 'append' and table_already_exists:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    elif insert_type.lower() == 'truncate' and table_already_exists:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    else:
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


def _does_table_exist(bigquery_client: bigquery.Client, table: str, dataset: str = 'analytics') -> bool:
    """Check if given table from given Dataset exists in BigQuery, return True if so."""
    try:
        table_reference = bigquery_client.dataset(dataset).table(table)
        is_table = bigquery_client.get_table(table_reference)
        if is_table:
            logging.info('Table "{}" in Dataset "{}" already exists in BigQuery.'.format(table, dataset))
            return True
    except NotFound as error:
        logging.warning('Table "{}" does not exist in BigQuery Dataset "{}". Ref: {}.'.format(table, dataset, error))
        return False


def _get_detected_schema(dataframe: pd.DataFrame, custom_schema: tuple = None) -> tuple:
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
        print(dataframe.head())
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

    return tuple(output_schema)


if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/johnathanbrooks/Downloads/fivetran-better-help-warehouse' \
                                                   '-5c0701231749.json'
    sample_csv_path = '../examples/sample_experiment_data.csv'
    df = pd.read_csv(sample_csv_path)

    sample_schema = [
        {
            'name': 'experiment_name',
            'field_type': 'string',
            'mode': 'required',
            'description': 'The name of the experiment'
        },
        {
            'name': 'account_id',
            'field_type': 'integer'
        }
    ]

    write_to_bigquery(df, 'experiment_sample', create_table_if_missing=True,
                      custom_new_table_schema=sample_schema)