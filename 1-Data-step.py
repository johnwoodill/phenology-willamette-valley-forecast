import geopandas as gpd

from libs import *


import rasterio

if LooseVersion(rasterio.__gdal_version__) >= LooseVersion("3.0.0"):
    rio_crs = rasterio.crs.CRS.from_wkt(pyproj_crs.to_wkt())
else:

rio_crs = rasterio.crs.CRS.from_wkt(pyproj_crs.to_wkt("WKT1_GDAL"))



# Import shapefile
fp = gpd.read_file('data/shapefiles/Willamette_Valley/willamette_valley.shp', crs="EPSG:4326")
filename = "willamette_valley_ccsm4.csv"

download_ccsm4("2023")

dat = proc_ccsm4(fp, "willamette_valley_ccsm4.csv")

