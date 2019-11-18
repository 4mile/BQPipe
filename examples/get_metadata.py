import os
import bqpipe

# Authenticate to BQ Project with your credentials.
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/johnathanbrooks/Downloads/fivetran-better-help-warehouse' \
                                               '-5c0701231749.json'

# Get datasets
datasets = bqpipe.list_datasets()

print(bqpipe.list_datasets.__doc__)
print('Datasets List Result:')
print(datasets)

# Get all tables in a specified dataset
my_dataset = 'analytics'
tables_in_dataset = bqpipe.list_tables_in_dataset(my_dataset)

print('\n')
print(bqpipe.list_tables_in_dataset.__doc__)
print('Tables in Dataset Result:')
print(tables_in_dataset)

# Get column metadata from specified dataset and table.
schema_dataset = 'analytics'
schema_table = 'prediction'
schema = bqpipe.get_table_schema(schema_dataset, schema_table)

print('\n')
print(bqpipe.get_table_schema.__doc__)
print('Table Schema Result:')
print(schema)
