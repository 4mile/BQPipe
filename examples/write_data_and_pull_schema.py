import os
import pandas as pd
import bqpipe

# Authenticate to BQ Project with your credentials.
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/johnathanbrooks/Downloads/fivetran-better-help-warehouse' \
                                               '-5c0701231749.json'

# Specify data source and destination table.
csv_data_path = 'sample_experiment_data.csv'
df = pd.read_csv(csv_data_path)

# Specify BigQuery destination table.
destination_dataset = 'analytics'
destination_table = 'prediction_sample'

# Pull schema from destination table.
data_schema = bqpipe.get_table_schema(destination_dataset, destination_table)
print(data_schema)

# Write to BigQuery. If dataset not specified, default is the "analytics" dataset.
bqpipe.write_to_bigquery(df, destination_table, destination_dataset='test_dataset')
