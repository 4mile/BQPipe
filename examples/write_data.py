import os
import pandas as pd
import bqpipe

# Authenticate to BQ Project with your credentials.
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/johnathanbrooks/Downloads/fivetran-better-help-warehouse' \
                                               '-5c0701231749.json'

# Specify data source and destination table.
csv_data_path = 'sample_experiment_data.csv'
df = pd.read_csv(csv_data_path)
data_schema = [
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

# Specify BigQuery destination table.
destination_table = 'Prediction_sample'

# Write to BigQuery.
bqpipe.write_to_bigquery(df, destination_table, custom_new_table_schema=data_schema,
                         insert_type='append')
