# requirments #

import sys
import subprocess

#requirements

def install_package(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])


libraires = [
"geopandas", "pandas", "sqlalchemy", "psycopg2", "arcgis", "shutil", "fiona", "zipfile", "json", "numpy"
]

for lib in librairies:
    try:
        import lib
    except ImportError as e:
        install_package(lib)

'''
import geopandas as gpd
import pandas as pd
import datetime
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
import sqlalchemy
'''