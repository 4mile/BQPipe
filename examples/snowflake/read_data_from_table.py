import json
import bqpipe

# Fetch key file path from config.json file in current directory.
with open('config.json') as config_file:
    data = json.load(config_file)

# Authenticate to BQ Project with your credentials.
json_file_path = data['json_file_path']
client = bqpipe.BigQueryClient(json_file_path)

# Read from BigQuery. If dataset not specified, default is the "analytics" dataset.
source_table = 'prediction_sample'

# Simple example: Get full table output as DataFrame.
df = client.fetch_table_data(source_table)
print(df)

# Another example: Get specific fields from table.
field_subset = ('experiment_name', 'is_active')
df2 = client.fetch_table_data(source_table, fields=field_subset)
print(df2)

# Another example: Apply Where clause to table output.
where_clause = "is_active = True AND lower(experiment_name) like '%test%'"
df3 = client.fetch_table_data(source_table, where_clause=where_clause)
print(df3)

# Another example: Choose Selected fields, Where clause and row limit.
select_fields = '* EXCEPT (created_at)'
where_sql = "is_active = True AND prediction < 1"
rows = 1
df4 = client.fetch_table_data(source_table, fields=select_fields, where_clause=where_sql, number_of_rows=rows)
print(df4)
