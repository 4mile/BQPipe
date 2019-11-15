import os
import pandas as pd
import bqpipe

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/johnathanbrooks/Downloads/fivetran-better-help-warehouse' \
                                               '-5c0701231749.json'

csv_data_path = 'sample_experiment_data.csv'
new_table_name = 'prediction_sample'
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
    },
    {
        'name': 'prediction',
        'field_type': 'float'
    },
    {
        'name': 'is_active',
        'field_type': 'boolean',
        'mode': 'required'
    }
]

df = pd.read_csv(csv_data_path)
# bqpipe.write_to_bigquery(df, new_table_name, create_table_if_missing=True, custom_new_table_schema=sample_schema,
#                          insert_type='Truncate')
# round(datetime.datetime.utcnow().timestamp() * 1000)
sample_sql = 'SELECT 1 FROM analytics.prediction_sample2'
output_df = bqpipe.fetch_sql_output_bigquery(sample_sql)
print(output_df)
