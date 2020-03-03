import json
import pandas as pd
import bqpipe

# Fetch key file path from config.json file in current directory.
    with open('config.json') as config_file:
        data = json.load(config_file)

    # Authenticate to BQ Project with your credentials.
    json_file_path = data['json_file_path']
    client = bqpipe.BigQueryClient(json_file_path)

# Specify data source (empty DataFrame for new table) and destination table.
df = pd.DataFrame()

new_table_schema = [
    {
        'name': 'id',
        'field_type': 'number',
        'mode': 'required',
        'description': 'The primary key of the table'
    },
    {
        'name': 'experiment_name',
        'field_type': 'string',
        'mode': 'required',
        'description': 'The name of the experiment'
    },
    {
        'name': 'significance',
        'field_type': 'float',
        'mode': 'required',
        'description': 'The statistical significance level at which the power calculation was computed.'
    },
    {
        'name': 'power',
        'field_type': 'float',
        'mode': 'required',
        'description': 'The power level at which the power calculation was computed.'
    },
    {
        'name': 'sample_size',
        'field_type': 'integer',
        'mode': 'required',
        'description': 'The required sample size needed with the given parameters.'
    },
]

# Specify new BigQuery destination table.
destination_table = 'sample_experiment_table'

# Write to BigQuery.
client.write_to_bigquery(df, destination_table, custom_table_schema=new_table_schema)
