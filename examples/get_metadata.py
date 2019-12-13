import json
import bqpipe

# Fetch key file path from config.json file in current directory.
with open('config.json') as config_file:
    data = json.load(config_file)

# Authenticate to BQ Project with your credentials.
json_file_path = data['json_file_path']
client = bqpipe.BigQueryClient(json_file_path)

# Get datasets
datasets = client.list_datasets()

print(client.list_datasets.__doc__)
print('Datasets List Result:')
print(datasets)

# Get all tables in a specified dataset
my_dataset = 'analytics'
tables_in_dataset = client.list_tables_in_dataset(my_dataset)

print('\n')
print(client.list_tables_in_dataset.__doc__)
print('Tables in Dataset Result:')
print(tables_in_dataset)

# Get column metadata from specified dataset and table.
schema_dataset = 'analytics'
schema_table = 'prediction'
schema = client.get_table_schema(schema_dataset, schema_table)

print('\n')
print(client.get_table_schema.__doc__)
print('Table Schema Result:')
print(schema)
