import os
import bqpipe

# Authenticate to BQ Project with your credentials.
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/johnathanbrooks/Downloads/fivetran-better-help-warehouse' \
                                               '-5c0701231749.json'

# Get datasets
datasets = bqpipe.list_datasets()

print(bqpipe.list_datasets.__doc__)
print('\nResult:')
print(datasets)

tables_in_dataset = bqpipe.list_tables_in_dataset('analytics')
print(tables_in_dataset)
