import bqpipe

# Authenticate to BQ Project with your credentials.
json_file_path = '[my key file location]'
client = bqpipe.authenticate_with_service_account_json(json_file_path)

# Get datasets
datasets = bqpipe.list_datasets(client)

print(bqpipe.list_datasets.__doc__)
print('Datasets List Result:')
print(datasets)

# Get all tables in a specified dataset
my_dataset = 'analytics'
tables_in_dataset = bqpipe.list_tables_in_dataset(client, my_dataset)

print('\n')
print(bqpipe.list_tables_in_dataset.__doc__)
print('Tables in Dataset Result:')
print(tables_in_dataset)

# Get column metadata from specified dataset and table.
schema_dataset = 'analytics'
schema_table = 'prediction'
schema = bqpipe.get_table_schema(client, schema_dataset, schema_table)

print('\n')
print(bqpipe.get_table_schema.__doc__)
print('Table Schema Result:')
print(schema)
