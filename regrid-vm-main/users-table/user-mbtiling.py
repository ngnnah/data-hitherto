import sqlalchemy
import geopandas as gp

pg_driver = 'postgresql+psycopg2'
pg_user = 'boss_user'
pg_pass = 'passDEV9g47uibjn2ijovZNEW' # boss dev
pg_host = 'dev-boss.db.ready.net' # boss dev
pg_port = '5432'
pg_db = 'boss_db_dev'

engine = sqlalchemy.create_engine(f"{pg_driver}://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}") 

cols = ['account_status',
'account_type',
# 'active',
# 'autopay',
'first_name',
'id',
'last_name',
'map_point',
'service_status',
'updated_at',
'network_node_id',]


tablename = 'public.users'
# from_postgis_sql = f"SELECT * FROM {tablename} " # SELECT all fields
from_postgis_sql = f"SELECT {','.join(cols)} FROM {tablename} "

print("Starting to download users table geopandas.from_postgis")
# 30sec
udf = gp.GeoDataFrame.from_postgis(from_postgis_sql, engine, geom_col='map_point') 


# 30sec
udf[cols].to_file('data/users.json', driver='GeoJSON')
print("Finished saving users table to data/users.json")
