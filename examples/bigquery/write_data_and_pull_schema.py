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

# Specify BigQuery destination table.
destination_dataset = 'analytics'
destination_table = 'prediction_sample_test'

# Pull schema from destination table.
data_schema = client.get_table_schema(destination_dataset, destination_table)
print(data_schema)

# Write to BigQuery. If dataset not specified, default is the "analytics" dataset.
client.write_to_bigquery(df, destination_table, custom_table_schema=data_schema)
