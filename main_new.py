# Developed by Parthesh B.
# Date: 18/10/2019

from h3 import h3
import pandas as pd
import geopandas as gp
from shapely.geometry import Polygon
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *
from geopandas_postgis import PostGIS
import os,json

currentDir = os.path.dirname(os.path.realpath(__file__))
configFile=open(currentDir+"/geobinning_config.json")
parameters = configFile.read()
parameters = json.loads(parameters)
print(parameters)
#--------------------------------------json data has been successfully load in  -----------------------
connection = parameters["connection"]
table_to_fetch = parameters["table_to_fetch"]
table_to_push = parameters["table_to_push"]
latitude_col = parameters["latitude_col"]
longitude_col = parameters["longitude_col"]
operator_col = parameters["operator_col"]
technology_col = parameters["technology_col"]
value_col = parameters["value_col"]
test_type_col = parameters["test_type_col"]




engine = create_engine(connection)        #-------getting a input data from database
print(engine)
def df_tests_to_gphex(df, resolution,latitude_col_name,longitude_col_name,feature_col_name,operator_col_name,technology_col_name,testtype_col_name):
    
    '''Use h3.geo_to_h3 to index each data point into the spatial index of the specified resolution.
      Use h3.h3_to_geo_boundary to obtain the geometries of these hexagons'''

    df = df[[latitude_col_name,longitude_col_name,feature_col_name,operator_col_name,technology_col_name,testtype_col_name]].copy()



    df["hex_id"] = df.apply(lambda row: h3.geo_to_h3(row[latitude_col_name], row[longitude_col_name], resolution), axis = 1)#----------this is for the title of the table in database

    df_aggreg = df.groupby(by = ["hex_id",operator_col_name,technology_col_name,testtype_col_name]).agg({feature_col_name: ['mean', 'count']}).reset_index()
    df_aggreg.columns = ["hex_id",operator_col_name,technology_col_name,testtype_col_name, "value","count"]
    df_aggreg['bin_size'] = resolution

    geom =  df_aggreg.hex_id.apply(lambda x: Polygon(h3.h3_to_geo_boundary(h3_address=x,geo_json=True)) )
    crs = {'init': 'epsg:4326'}
    gpdf_aggreg = gp.GeoDataFrame(df_aggreg,geometry=geom,crs=crs)
    return gpdf_aggreg

data = pd.read_sql_query("select "+latitude_col+","+longitude_col+","+operator_col+", "+technology_col+","+value_col+","+test_type_col+" from "+table_to_fetch+";",engine)


gphex_3 = df_tests_to_gphex(df = data , resolution = 3,latitude_col_name=latitude_col,longitude_col_name=longitude_col, feature_col_name=value_col,operator_col_name   = operator_col,technology_col_name = technology_col,testtype_col_name   = test_type_col)
print("resolution 3 processed")
gphex_4 = df_tests_to_gphex(df = data , resolution = 4,latitude_col_name=latitude_col,longitude_col_name=longitude_col, feature_col_name=value_col,operator_col_name   = operator_col,technology_col_name = technology_col,testtype_col_name   = test_type_col)
print("resolution 4 processed")
gphex_5 = df_tests_to_gphex(df = data , resolution = 5,latitude_col_name=latitude_col,longitude_col_name=longitude_col, feature_col_name=value_col,operator_col_name   = operator_col,technology_col_name = technology_col,testtype_col_name   = test_type_col)
print("resolution 5 processed")
gphex_6 = df_tests_to_gphex(df = data , resolution = 6,latitude_col_name=latitude_col,longitude_col_name=longitude_col, feature_col_name=value_col,operator_col_name   = operator_col,technology_col_name = technology_col,testtype_col_name   = test_type_col)
print("resolution 6 processed")



print('ok')
gphex_3.postgis.to_postgis(con=engine, table_name=table_to_push, if_exists='append',geometry='Polygon',index=False)
print("resolution 3 pushed to DB")
try:
    gphex_4.postgis.to_postgis(con=engine, table_name=table_to_push, if_exists='append',geometry='Polygon',index=False)
except Exception as e:
    print("Error in gphex_4")
print("resolution 4 pushed to DB")
gphex_5.postgis.to_postgis(con=engine, table_name=table_to_push, if_exists='append',geometry='Polygon',index=False)
print("resolution 5 pushed to DB")
gphex_6.postgis.to_postgis(con=engine, table_name=table_to_push, if_exists='append',geometry='Polygon',index=False)
print("resolution 6 pushed to DB")
print('data has been successfully pushed')
engine.dispose()