# -*- coding: utf-8 -*-
"""
Created on Thu Dec 14 15:22:13 2023

@author: michel.raherimanant
"""

import geopandas as gpd
import pandas as pd
import datetime
import sys
from sqlalchemy import create_engine
import psycopg2
import time
from arcgis.gis import GIS
import shutil
import random
import sys
import fiona
import zipfile
import json
import numpy as np
import json
import sqlalchemy

class read_config:
    '''
    this is the reader of the simple excel reader
    which contains the id of the features collections to be downloaded,
    make sure to pu the proper id and the gdb_name.
    the id if for downloading, the gdb_name will be the name 
    in postgres/postgis.
    caution : do not change the name of the columns
    '''
    
    def __init__(self, index_):
        self.index_ = index_
    
    def read_config(self):
        return pd.read_excel("./files/config_.xlsx", sheet_name= "id_arcgis_item")
    
    def id_reader(self):
        df = self.read_config()
        return df["id"].iloc[self.index_]
    
    def gdb_name(self):
        df = self.read_config()
        return df["gdb_name"].iloc[self.index_]

class Almighty_arcgis_geojson:
    def __init__(self, layer_name, layer_id,epsg,  now_, base_dir,
                 username, password):
        self.layer_name = layer_name
        self.layer_id = layer_id
        #self.dir_ = dir_
        self.now_ = now_
        self.base_dir = base_dir
        self.username = username
        self.password = password
        self.epsg = epsg
        
    def get_layer_(self):
        #get the feature layer for downloading
        #make sure that the layer is editable first
        gis = GIS(username = self.username, 
                  password = self.password)
        portal_item = gis.content.get(self.layer_id)
        ports_layer = portal_item.layers[0]
        return ports_layer
    
    def to_gjson(self):
        item = self.get_layer_()
        if type(item) == list:
            sys.exit("NO LAYER IS ACTUALLY AVAIBLE FOR {}".format(self.layer_id))
        else:
            pass
        fset = item.query()
        gjson_string = fset.to_geojson
        #gjson_dict = json.loads(gjson_string)
        '''
        gdf = gpd.GeoDataFrame.from_features(gjson_dict['features'], 
                                             crs = "EPSG:{}".format(self.epsg))
        '''
        gdf = gpd.read_file(gjson_string, driver='GeoJSON',
                                         crs = "epsg:{}".format(self.epsg))
        return gdf.set_crs("epsg:{}".format(self.epsg))
    
    def to_sdf(self):
        item = self.get_layer_()
        if type(item) == list:
            sys.exit("NO LAYER IS ACTUALLY AVAIBLE FOR {}".format(self.layer_id))
        else:
            pass
        fset = item.query()
        gjson_string = fset.to_json
        #gjson_dict = json.loads(gjson_string)
        '''
        gdf = gpd.GeoDataFrame.from_features(gjson_dict['features'], 
                                             crs = "EPSG:{}".format(self.epsg))
        '''
        gdf = gpd.GeoDataFrame.from_file(gjson_string, geometry = 'SHAPE',
                               crs = "epsg:{}".format(self.epsg))
        return gdf.set_crs("epsg:{}".format(self.epsg))
    
    def back_up(self):
        gdf = self.to_sdf()
        gdf.to_file(self.base_dir + self.now_ +"_" + self.layer_id + ".shp",
                    crs = "epsg:{}".format(self.epsg))


class gdb_operator:
    def __init__(self, gdb_name, table, password,host, port, username, 
                 arcgis_gdb):
        self.gdb_name = gdb_name
        self.table = table
        self.password = password
        self.port =port
        self.host = host
        self.username = username
        self.arcgis_gdb = arcgis_gdb
    
    def engine_builder(self):
        engine = create_engine("postgresql://postgres:{}@localhost:{}/{}".format(
            self.password,
            self.port,
            self.gdb_name
            ))
        return engine
    
    def post_gis_writer_first(self, gdb):
        engine = self.engine_builder()
        gdb.to_postgis(self.table, engine, if_exists = 'replace')
        gdb.to_postgis(self.table + "_raw", engine, if_exists = 'replace')
        #_raw here refer to the gdb cloud , please do not create view on this 
    
    def post_gis_writer_raw(self, gdb):
        engine = self.engine_builder()
        gdb.to_postgis(self.table + "_raw", engine, if_exists = 'replace')
    
    def post_gis_writer(self, gdb):
        engine = self.engine_builder()
        gdb.to_postgis(self.table, engine, if_exists = 'append')
        #this is for appending the existing geo_database
        #an update table sql script will aim to 
        #gdb.to_postgis(self.table + "_raw", engine, if_exists = 'replace')
        #_raw here refer to the gdb cloud , please do not create view on this
        
    def query(self):
        return "select *,ST_CurveToLine(geometry) as geom from {};".format(self.table)
    
    def query_raw(self):
        return "select * from {};".format(self.table + "_raw")
    
    def loader(self):
        base = gpd.GeoDataFrame.from_postgis(self.query(), 
                                             self.engine_builder(),
                                             geom_col = "geom")
        return base
    
    def loader_raw(self):
        base = gpd.GeoDataFrame.from_postgis(self.query_raw(), 
                                             self.engine_builder(),
                                             geom_col = "geometry")
        return base
        
    def columns_ut(self):
        db = self.loader()
        col_db = db.columns
        #cdb = col_db.to_list()
        col_arcgis = self.arcgis_gdb.columns
        #adb = col_arcgis.to_list
        #cols = col_arcgis.query["GlobalID not in @col_db"]
        cols = col_arcgis[~col_arcgis.isin(col_db)]
        if cols.empty:
            return []
            #pass
        else:
            return cols.to_list()
    
    def update_ut(self, column, value, gid):
        try:
            conn = psycopg2.connect(user= self.username,
                                    password=self.password,
                                    host=self.host,
                                    port=self.port,
                                    database =self.gdb_name)
            conn.autocommit = True
            cur = conn.cursor()
            query = 'UPDATE "{}" SET "{}" = {} WHERE "GlobalID" ilike {};'.format(self.table,
                                                   column,
                                                   "'" + str(value) + "'",
                                                   "'" + gid + "'")
            cur.execute(query)
            cur.close()
            conn.close()
            
        except psycopg2.OperationalError:
            print("no such {} database, or wrong password".format(
                self.gdb_name))
            sys.exit("no such {} database, or wrong password".format(
                self.gdb_name))
        
        except psycopg2.errors.UndefinedTable:
            print("no such DATABASE!!")
    
    def main_update(self):
        raw = self.loader_raw()
        base = self.loader()
        r_d = raw[["GlobalID", "EditDate"]]
        b_d = base[["GlobalID", "EditDate"]]
        joined = b_d.join(r_d.set_index("GlobalID"), on = "GlobalID", rsuffix = "_")
        joined = joined.drop_duplicates(subset = "GlobalID")
        #print(joined.columns)
        #sys.exit()
        target = joined.loc[np.where(joined["EditDate"] != joined["EditDate_"])]
        if target.empty:
            print("No need to update table")
            pass
        else:
            #cols = target.columns.to_list()
            df = raw.drop(["geometry"], axis = 1)
            for col in df.columns.to_list():
                if col == "GlobalID":
                    pass
                else:
                    for gid in target["GlobalID"]:
                        v = df.loc[df["GlobalID"] == gid]
                        for index, row in v.iterrows():
                            self.update_ut(column = col,
                                           #value = v[col].iloc[0], 
                                           value = row[col],
                                           gid= gid)
                #you need to optimize this process
                #each element in the for loop need to connect to the databse
                #it is not efficient
        
    
    def main(self):
        self.post_gis_writer_raw(gdb= self.arcgis_gdb)
        try:
            conn = psycopg2.connect(user= self.username,
                                    password=self.password,
                                    host=self.host,
                                    port=self.port,
                                    database =self.gdb_name)
            conn.autocommit = True
            cur = conn.cursor()
            #cur.execute("select * from {};".format(self.table))
            #gdb = cur.fetchall()
            #if it passes here, need to compare gdb and gdb_arcgis
            #gid_arc = self.arcgis_gdb["GlobalID"].to_list()
            gid_base = self.loader()
            b_gid = gid_base["GlobalID"].to_list()
            keep_gdf = self.arcgis_gdb.query("GlobalID not in @b_gid")
            #NEED TO COMPARE THE COLUMNS
            #SOME NEW ENTRIES MAY BE ADDED 
            if keep_gdf.empty:
                pass
            else:
                cols = self.columns_ut()
                if len(cols) == 0:
                    self.post_gis_writer(gdb = keep_gdf)
                else:
                    #need to add columns and parse each new values inside
                    #the new columns
                    for el in cols:
                        query = 'ALTER TABLE "{}" ADD COLUMN "{}" varchar(255);'.format(self.table,
                                                                       el)
                        print("create column named {}".format(el))
                        cur.execute(query)
                    
                    cur.close()
                    conn.close()
                    print("all columns created!")
                    
                    self.post_gis_writer(gdb = keep_gdf)
            
            
        except psycopg2.OperationalError:
            print("no such {} database, or wrong password".format(
                self.gdb_name))
            sys.exit("no such {} database, or wrong password".format(
                self.gdb_name))
        
        except psycopg2.errors.UndefinedTable:
            print("creating a new table named : {}".format(self.table))
            self.post_gis_writer_first(gdb = self.arcgis_gdb)
            
        except sqlalchemy.exc.ProgrammingError:
            print("creating a new table named : {}".format(self.table))
            self.post_gis_writer_first(gdb = self.arcgis_gdb)
            

class postgis_loader:
    def __init__(self, gdb_name, password, table_name):
        self.gdb_name = gdb_name
        self.password = password
        self.table_name = table_name
    
    def engine_builder(self):
        c = "postgresql://postgres:{}@localhost:5432/{}".format(self.password,
                                                                self.gdb_name)
        eng = create_engine(c)
        return eng
    
    def query_builder(self):
        query = "select * from {};".format(self.table_name)
        return query
    
    def loader(self):
        try:
            base = gpd.GeoDataFrame.from_postgis(self.query_builder(),
                                                 self.engine_builder(),
                                                 geom_col = "geometry")
            return base
        except psycopg2.errors.UndefinedTable as err:
            print(err)
            return 0
            pass
        

class Mighty_ARCGIS_extractor:
    def __init__(self, layer_name, layer_id, dir_, now_, base_dir,
                 username, password):
        '''
        layer_name = le nom de la couche : feature layer (Hosted)
        layer_id = id du Feature host, accessible dans AGOL, sur Details
        dir_ = dossier de download pour raw fichier en format zip
        now_ = str(datetime.datetime.now().date())
        base_dir = dossier ou se trouve le fichier shapefile a telecharger, 
                   ici raw_
        usernmame = usernmae d'ArcGis Online
        password = ArcGis online password
        '''
        self.layer_name = layer_name
        self.layer_id = layer_id
        self.dir_ = dir_
        self.now_ = now_
        self.base_dir = base_dir
        self.username = username
        self.password = password
    
    def get_layer(self):
        gis = GIS(username = self.username, 
                  password = self.password)
        search_results = gis.content.search("title: {}".format(self.layer_name),
                                            "Feature layer Collection")
        items = []
        for el in search_results:
            if el.id == self.layer_id:
                items.append(el)
                break
            else:
                #return "Empty"
                pass
        return items[0]
    
    def get_layer_(self):
        #get the feature layer for downloading
        #make sure that the layer is editable first
        gis = GIS(username = self.username, 
                  password = self.password)
        portal_item = gis.content.get(self.layer_id)
        ports_layer = portal_item.layers[0]
        return ports_layer
    
    def export(self):
        item = self.get_layer()
        if type(item) == list:
            sys.exit("NO LAYER IS ACTUALLY AVAIBLE FOR {}".format(self.layer_name))
        else:
            pass
        item = item.export("{}_{}".format(self.now_, self.layer_name),
                               "Shapefile", wait =True)
        time.sleep(random.randint(3, 7))
        item.download(self.dir_)
        time.sleep(random.randint(3, 7))
        item.delete()
        result = self.dir_ +"/" + "{}_{}".format(self.now_, self.layer_name) + ".zip"
        return result
    
    def unzipping(self):
        zip_ = self.export()
        n = zipfile.ZipFile(zip_, "r")
        shutil.unpack_archive(zip_, self.base_dir)
        for el in n.zipfile.ZipFile():
            if el.endswith(".shp"):
                return self.base_dir + el
            else:
                pass
    
        