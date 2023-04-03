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


# execute data-base
connection.execute("USE TMDB_MOVIES")
table_name = 'MOVIES'
#fetching data
df = pd.read_sql_query("select * from MOVIES ",con=connection)

#----------------------------------------
# 1- remove duplicates
# calculating number of dublicated rows
df.duplicated().sum()
#drop dublicated rows
df.drop_duplicates(inplace=True)
#check shape of dataset after dropping duplicates
print(df.shape)
#2- Remove the movies which are having zero value of budget or revenue
df = df.drop(df[df.budget_adj == 0].index | df[df.revenue_adj == 0 ].index)
# 3-check about nullable fields
print(df.isnull().sum())
#Drop nullable rows
df = df.dropna()
# check about nullable fields
print(df.isnull().sum())
#----------------------------
#regarding the snowflake DB connection
connection.execute("USE TMDB_MOVIES") 
if_exists = 'replace'
#which are the most popular movies ?
popular_movies = df[['original_title','popularity']].sort_values(by = 'popularity',ascending  = False)
top_10_popular_movies = popular_movies.head(10)
table_name = 'popular_movies'
top_10_popular_movies.to_sql(name=table_name.lower(), con=connection, index=False ,if_exists=if_exists)
#-----------------------------------------------
# #top ten profitable movies
df['profit'] =df['revenue_adj'] - df['budget_adj']
prfitable_movies = df[['original_title','profit']].sort_values(by = 'profit',ascending= False)
top_10_profitable_movies =prfitable_movies.head(10) 
table_name = 'profitable_movies'
top_10_profitable_movies.to_sql(name=table_name.lower(), con=connection, index=False ,if_exists=if_exists)
#-----------------------------------
#Top 10 rated movies
movies_rate =  df[['original_title','vote_average']].sort_values(by = 'vote_average',ascending= False)
top_10_rated_movies = movies_rate.head(10)
print(top_10_rated_movies)
table_name = 'top_rated_movies'
top_10_rated_movies.to_sql(name=table_name.lower(), con=connection, index=False ,if_exists=if_exists)

