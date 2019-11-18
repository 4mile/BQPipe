import os
import bqpipe

# Authenticate to BQ Project with your credentials.
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/johnathanbrooks/Downloads/fivetran-better-help-warehouse' \
                                               '-5c0701231749.json'

# Specify the SQL statement you'd like to execute.
sql = """
    SELECT  *
    FROM    analytics.prediction_sample2
"""

# Get output as DataFrame.
df = bqpipe.fetch_sql_output_bigquery(sql)
print(df)
