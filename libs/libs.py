#%%
import numpy as np
import pandas as pd
import xarray as xr 
import rioxarray as rxr
import geopandas as gpd
import glob as glob
from datetime import datetime as dt
import os
from shapely.geometry import Polygon, MultiPolygon


# Set random seed for TF
# tf.random.set_seed(1234)

#%%
def convert_3D_2D(geometry):
    '''
    Takes a GeoSeries of 3D Multi/Polygons (has_z) and returns a list of 2D Multi/Polygons
    '''
    new_geo = []
    for p in geometry:
        if p.has_z:
            if p.geom_type == 'Polygon':
                lines = [xy[:2] for xy in list(p.exterior.coords)]
                new_p = Polygon(lines)
                new_geo.append(new_p)
            elif p.geom_type == 'MultiPolygon':
                new_multi_p = []
                for ap in p:
                    lines = [xy[:2] for xy in list(ap.exterior.coords)]
                    new_p = Polygon(lines)
                    new_multi_p.append(new_p)
                new_geo.append(MultiPolygon(new_multi_p))
    return new_geo


#%%
def download_ccsm4(year):
    ccsm4_lst = [
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/pr_day_ccsm4_{year}0101_r10i1p1_{year}0101-{year}1231.nc",
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/pr_day_ccsm4_{year}0101_r1i1p1_{year}0101-{year}1231.nc", 
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/pr_day_ccsm4_{year}0101_r2i1p1_{year}0101-{year}1231.nc",   
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/pr_day_ccsm4_{year}0101_r3i1p1_{year}0101-{year}1231.nc",   
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/pr_day_ccsm4_{year}0101_r4i1p1_{year}0101-{year}1231.nc",   
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/pr_day_ccsm4_{year}0101_r5i1p1_{year}0101-{year}1231.nc",   
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/pr_day_ccsm4_{year}0101_r6i1p1_{year}0101-{year}1231.nc",   
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/pr_day_ccsm4_{year}0101_r7i1p1_{year}0101-{year}1231.nc",   
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/pr_day_ccsm4_{year}0101_r8i1p1_{year}0101-{year}1231.nc",   
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/pr_day_ccsm4_{year}0101_r9i1p1_{year}0101-{year}1231.nc",   
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/tasmax_day_ccsm4_{year}0101_r5i1p1_{year}0101-{year}1231.nc",
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/tasmax_day_ccsm4_{year}0101_r6i1p1_{year}0101-{year}1231.nc",
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/tasmax_day_ccsm4_{year}0101_r7i1p1_{year}0101-{year}1231.nc",
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/tasmax_day_ccsm4_{year}0101_r8i1p1_{year}0101-{year}1231.nc",
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/tasmax_day_ccsm4_{year}0101_r9i1p1_{year}0101-{year}1231.nc",
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/tasmax_day_ccsm4_{year}0101_r10i1p1_{year}0101-{year}1231.nc",  
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/tasmax_day_ccsm4_{year}0101_r4i1p1_{year}0101-{year}1231.nc",        
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/tasmax_day_ccsm4_{year}0101_r1i1p1_{year}0101-{year}1231.nc",
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/tasmax_day_ccsm4_{year}0101_r3i1p1_{year}0101-{year}1231.nc",  
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/tasmax_day_ccsm4_{year}0101_r2i1p1_{year}0101-{year}1231.nc",  
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/tasmin_day_ccsm4_{year}0101_r6i1p1_{year}0101-{year}1231.nc",
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/tasmin_day_ccsm4_{year}0101_r7i1p1_{year}0101-{year}1231.nc",
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/tasmin_day_ccsm4_{year}0101_r9i1p1_{year}0101-{year}1231.nc",
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/tasmin_day_ccsm4_{year}0101_r8i1p1_{year}0101-{year}1231.nc",
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/tasmin_day_ccsm4_{year}0101_r10i1p1_{year}0101-{year}1231.nc",
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/tasmin_day_ccsm4_{year}0101_r1i1p1_{year}0101-{year}1231.nc",
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/tasmin_day_ccsm4_{year}0101_r3i1p1_{year}0101-{year}1231.nc",
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/tasmin_day_ccsm4_{year}0101_r2i1p1_{year}0101-{year}1231.nc",
        f"https://www.ncei.noaa.gov/data/north-american-multi-model-ensemble/access/ccsm4/{year}/tasmin_day_ccsm4_{year}0101_r4i1p1_{year}0101-{year}1231.nc"
        ]

    # Loop through CCSM4 and save to data/
    for file_ in ccsm4_lst:
        filename = file_.split("/")[-1]
        os.system(f"wget {file_} -O data/CCSM4/{filename}")
        print(file_)



#%%
def proc_ccsm4(fp, filename):
    #----------------------------------------------------
    # Create variable name dict
    var_dict = {'tasmin': 'tmmn_mean', 'tasmax': 'tmmx_mean', 'pr': 'pr_mean'}

    # If 3d dimensions convert 2d
    if fp.geometry.has_z[0] == True:
        fp.geometry = convert_3D_2D(fp.geometry)

    # Get glob of *.nc
    # https://www.ncei.noaa.gov/products/weather-climate-models/north-american-multi-model
    files = glob.glob("data/CCSM4/*.nc")

    # Loop through each glob and filter DTL
    lst = []
    for file_ in files:
        # Get model and variable name
        model = file_.split("/")[2].split("_")[4]
        var_name = file_.split("/")[2].split("_")[0]
        var_name = np.where(var_name == "tasmin", "tmmn_mean", var_name).ravel()[0]
        var_name = np.where(var_name == "tasmax", "tmmx_mean", var_name).ravel()[0]
        var_name = np.where(var_name == "pr", "pr_mean", var_name).ravel()[0]

        # Load nc file
        ndat = rxr.open_rasterio(file_, crs="EPSG:4326")
        
        # Convert to -180 to 180
        ndat.coords['x'] = (ndat.coords['x'] + 180) % 360 - 180
        ndat = ndat.sortby(ndat.x)
        ndat = ndat.rio.write_crs("epsg:4326")
        
        # Clip to Detroit Lake and convert to dataframe
        indat = ndat.rio.clip(fp.geometry.values, all_touched=True, drop=True, invert=False, from_disk=True) 
        indat = indat.to_dataframe().reset_index()

        # Fix offset and scales
        add_offset = ndat.add_offset
        scale_factor = ndat.scale_factor
        indat = indat.rename(columns={indat.columns[-1]: var_name,
            "y": "lat", "x": "lon", "TIME": "date"})
        indat[f"{var_name}"] = (indat[f"{var_name}"]*scale_factor) + add_offset 

        # Clean up dataframe
        indat = indat.drop(columns='spatial_ref')
        indat = indat.assign(model = model)
        indat['date'] = indat.date.apply(lambda x: dt.strptime(str(x), "%Y-%m-%d %H:%M:%S"))
        indat['date'] = pd.to_datetime(indat['date']).dt.strftime("%Y-%m-%d")

        # Gather data
        indat = pd.melt(indat, id_vars=['date', 'lat', 'lon', 'model'], value_vars=[f"{var_name}"])

        # bind data
        lst.append(indat)
        print(file_)

    outdat = pd.concat(lst)

    # Drop fill values
    outdat = outdat[outdat['value'] != 9.969209968386869e+36]

    # Spread data frame
    ddat = outdat.pivot(index= ['date', 'lat', 'lon', 'model'], columns='variable',values='value').reset_index()

    # Get average temp
    ddat = ddat.assign(tmma_mean = (ddat['tmmx_mean'] + ddat['tmmn_mean'] ) / 2)
    
    # Convert Kelvin to C
    ddat = ddat.assign(tmma_mean = ddat['tmma_mean'] - 273.15)
    ddat = ddat.assign(tmmx_mean = ddat['tmmx_mean'] - 273.15)
    ddat = ddat.assign(tmmn_mean = ddat['tmmn_mean'] - 273.15)

    ddat = ddat.assign(year = pd.to_datetime(ddat['date']).dt.strftime('%Y'),
        week = pd.to_datetime(ddat['date']).dt.strftime('%U').astype(int),
        month = pd.to_datetime(ddat['date']).dt.strftime('%m').astype(int))

    # Rolling 7 day max
    ddat = ddat.sort_values('date')
    ddat = ddat.set_index(['date', 'lat', 'lon']).groupby('model').rolling(7).mean().reset_index().dropna()

    # Convert lwe prcip rate to m then multiple by 1000 to get mm
    ddat = ddat.assign(pr_mean = ddat['pr_mean']*86400*1000)

    # Fllor month and get day of year (%j)
    ddat = ddat.assign(month = np.floor(ddat['month']).astype(int),
        day = pd.to_datetime(ddat['date']).dt.strftime('%j').astype(int))

    ddat = ddat.drop(columns=['lat', 'lon'])

    ddat.to_csv(f"data/processed/{filename}", index=False)
    print(f"Saved: data/processed/{filename}")

    return ddat


