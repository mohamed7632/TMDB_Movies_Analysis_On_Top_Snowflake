from snowflake.connector.pandas_tools import write_pandas,pd_writer
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

engine = create_engine(URL(
    account='GDZSVJI-GV36963',
    user = 'SHERIF1922',
    password = '1541998@Firstyear',
    database = 'TMDP_MOVIES',
    schema = 'public',
    warehouse = 'COMPUTE_WH',
    role='ACCOUNTADMIN',
))


connection = engine.connect()
results = connection.execute('select current_version()').fetchone()
print(results[0])


# read tmdb-movies data
df =  pd.read_csv('tmdb-movies.csv')
#specify the columns that I will use in the analysis process
movies_df = df[['original_title','genres','director','popularity','vote_average','budget_adj','revenue_adj','release_year']]
connection.execute("USE TMDB_MOVIES")
#cur.execute("USE SCHEMA PUPLIC")

# lading data to statging layer on snowflake
if_exists = 'replace'
table_name = 'MOVIES'
movies_df.to_sql(name=table_name.lower(), con=connection, index=False ,if_exists=if_exists)

