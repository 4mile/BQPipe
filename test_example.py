import os
import bqpipe

SNOWFLAKE_ACCOUNT = 'betterhelp'

os.environ['SF_USER'] = 'jbrooks'
os.environ['SF_PWD'] = '4DHzfbftqkpMXKMYogbJNN$B'

# Authenticate to Snowflake account with your credentials.
user_flow_auth_params = {
    'user': os.getenv('SF_USER'),
    'password': os.getenv('SF_PWD')
}

with bqpipe.SnowflakeClient(SNOWFLAKE_ACCOUNT, authentication_method='USER_LOGIN',
                            authentication_params=user_flow_auth_params) as client:
    print(client.current_user)
    print(client.current_role)
    print(client.current_warehouse)
    print(client.current_database)
    print(client.current_schema)

    # client.set_schema('analytics')
    # print(client.current_schema)

    # client.set_database('SNOWFLAKE')
    # print(client.current_database)

    client.set_warehouse('KEBOOLA_WH')
    print(client.current_warehouse)

    dbs = client.list_databases()
    print('list DBs:')
    print(type(dbs))
    print(dbs)

    schemas = client.list_schemas(client.current_database)
    print('list schemas:')
    print(type(schemas))
    print(schemas)

    sql = "SELECT COUNT(*) FROM aurora_multi_site_kbc_v2.account"
    df = client.fetch_sql_output(sql)
    print(df.head())

    client.set_database('BH_DB')
    print(client.current_database)
    print(client.current_schema)

    tables = client.list_tables(schema='aurora_multi_site_kbc_v2')
    print(tables)
