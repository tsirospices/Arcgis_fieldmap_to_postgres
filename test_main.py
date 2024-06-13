# -*- coding: utf-8 -*-
"""
Created on Mon Feb  5 16:28:11 2024

@author: michel.raherimanant
"""

import sys
import datetime
import utilis_ as ut
import pyproj as proj
#ppath = "C:/Users/RaherimanantsoaMichel/.conda/envs/SPICES_TSIIRO/Library/share/proj"
ppath = ut.ppath_maker()
proj.datadir.set_data_dir(ppath)

inputs = sys.argv

'''
layer_name : The name of the FeatureLayer on ArcGis online | input,
layer_id : The ID pf the the FeatureLayer on ArcGis online | input,
epsg: the desired SCR/projecton code of the layer | input ,
-- now_: today | automated input, 
base_dir : the directory of the archived_parcel on shp format| input,

username : ArcGIS Account username| input ,
password: ArcGis Account password| input,

gdb_name: The postgres/postgis GeoDatabase name|input,
table: The table where the geodata will be stocked|input,
password: The password of the postgres/postgis| input,
host : host (usually localhost for local geoDatabase) | input,
username: postgres username |input,
arcgis_gdb: the geodata from ArcGis | automated input

'''

'''
inputs = [py_script, layer_name_arcgis, layer_id_arcgis, epsg, base_dir, username_arcgis,
          password_arcgis,
          gdb_name_postgis, table_postgis, password_postgres, host_postgres,
          username_postgres]

'''
def Main_extract():
    gbd_ = ut.Almighty_arcgis_geojson(layer_name = inputs[1],
                                      layer_id = inputs[2],
                                      epsg = int(inputs[3]),
                                      now_ = str(datetime.datetime.now().date()),
                                      base_dir = inputs[4],
                                      username = inputs[5],
                                      password = inputs[6])
    #gbd_.back_up()
    
    data = gbd_.to_sdf()
    
    t = ut.gdb_operator(gdb_name = inputs[7],
                        table = inputs[8],
                        password = inputs[9],
                        host = inputs[10],
                        port = int(inputs[11]),
                        username = inputs[12],
                        arcgis_gdb = data)
    
    t.main()

if __name__ == "__main__":
    Main_extract()
