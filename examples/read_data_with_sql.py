import bqpipe

# Authenticate to BQ Project with your credentials.
json_file_path = '[my key file location]'
client = bqpipe.authenticate_with_service_account_json(json_file_path)

# Specify the SQL statement you'd like to execute.
sql = """
    SELECT  *
    FROM    analytics.prediction_sample
"""

# Get output as DataFrame.
df = bqpipe.fetch_sql_output(client, sql)
print(df)
