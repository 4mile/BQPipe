import json
import bqpipe

# Fetch key file path from config.json file in current directory.
with open('config.json') as config_file:
    data = json.load(config_file)

# Authenticate to BQ Project with your credentials.
json_file_path = data['json_file_path']
client = bqpipe.BigQueryClient(json_file_path)

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
client.write_to_bigquery(df, destination_table, custom_table_schema=data_schema,
                         insert_type='append')

print(client.write_to_bigquery.__doc__)
