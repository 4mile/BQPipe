import json
import bqpipe

# Fetch key file path from config.json file in current directory.
with open('config.json') as config_file:
    data = json.load(config_file)

# Authenticate to BQ Project with your credentials.
json_file_path = data['json_file_path']
client = bqpipe.BigQueryClient(json_file_path)

# Specify the SQL statement you'd like to execute.
sql = """
    SELECT  *
    FROM    analytics.prediction_sample
"""

# Get output as DataFrame.
df = client.fetch_sql_output(sql)
print(df)
