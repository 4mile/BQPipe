import sys
import logging
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def does_table_exist(bigquery_client: bigquery.Client, table: str, dataset: str = 'analytics') -> bool:
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


def get_detected_schema(dataframe: pd.DataFrame, custom_schema: tuple = None) -> tuple:
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
