# -*- coding: utf-8 -*-
"""
A script to create GeoSpatial datasets of recorded crimes within a specified 
 buffer of all NaPTAN stops!

Crime data source:  https://data.police.uk/data/
NaPTAN data source: https://beta-naptan.dft.gov.uk/Download/National
"""
# # # # IMPORTS # # # #
# sys
import pathlib 
import os
from typing import Tuple, Dict 
from datetime import datetime

# 3rd party
import pandas as pd 
import geopandas as gpd
from tqdm import tqdm

    
# # # # CONSTANTS # # # # 
CRIME_DATA_PATH = pathlib.Path(r"E:\Data\Crime Data")
NAPTAN_DIR = pathlib.Path(r"E:\Data\NaPTAN")

GEOMETRY_COL = "geometry"

NAPTAN_BUFFER_M = 100  # Buffer around NaPTAN stops (metres)

GROUPBY_COLS = ["NaptanCode", "Crime type"]

# # # # CLASSES # # # # 
# TODO(JH): Class to handle all data loading?


# # # # FUNTIONS # # # # 


def read_naptan_data(
        data_dir: pathlib.Path,
        x_col: str,
        y_col: str,
        crs: str,
        export_geo: bool=False,
    ) -> gpd.GeoDataFrame:
    """
    Reads in NaPTAN data and converts to GeoSpatial format
    
    Exports GeoSpatial NaPTAN data if `export_geo` is True
    """
    
    stops_files = [
        filename 
        for filename in os.listdir(data_dir)
        if ".csv" in filename
        ]
    
    for file in stops_files:
        
        print(f"Reading NaPTAN: {file}")
        stop_data = pd.read_csv(
            data_dir / file,
            low_memory=False,
            )
        
        geo_stop_data = create_geospatial_dataset(
            df=stop_data, 
            x_col=x_col,
            y_col=y_col,
            crs=crs,
            )
        
        if export_geo:
            filename = "geo_" + file.replace(".csv", ".gpkg")
            
            print(f"Exporting Geo NaPTAN data: {filename}")
            geo_stop_data.to_file(data_dir / filename)
        
        return geo_stop_data
        
        
def read_and_create_geo_data(
        data_dir: pathlib.Path,
        x_col: str,
        y_col: str,
        crs: str,
        export_geo: bool=False,
    ) -> Tuple[pd.DataFrame, Dict]:
    
    """
    Reads in all data from crime data downloads
    
    Converts data to GeoSpatial format. 
    
    If `export_geo`, each GeoSpatial version of crime data will be exported 
        to the directory containing the raw crime data
     
    All data is returned as either:
        all_data: Dict
            Dictionary of individual crime datasets in GeoSpatial format

        combined_data: gpd.GeoDataFrame:
            GeoDataFrame of each individal crime dataset all concatonated 
            together into a single GeoDataFrame
    """
    
    # Get subdirectories of data
    crime_data_directories = os.listdir(data_dir)
    
    # Variables to store data
    all_data = {}
    combined_data = pd.DataFrame()
    
    pbar = tqdm(
        total=len(crime_data_directories),
        desc="Reading data",
        ncols=100,
        leave=True,
        position=0,
        )
    
    # Loads data
    for directory in crime_data_directories:

        # Get year from directory name (format: YYYY-MM)
        year = directory[:4]
        
        if year not in all_data.keys():
            all_data[year]= dict()

        # Create year-month specific path
        path = CRIME_DATA_PATH / directory
        
        crime_data_files = [
            filename  
            for filename in os.listdir(path)
            if ".csv" in filename
        ]
        
        # Read all crime data files for sub directory
        for file in crime_data_files:
            pbar.set_description(f"Reading: {file}")
            
            data = pd.read_csv(path / file)
            
            data = create_geospatial_dataset(
                df=data,
                x_col=x_col,
                y_col=y_col,
                crs=crs,
                )

            if export_geo:
                data.to_file(path / ("geo_" + file.replace(".csv", ".gpkg")))
            
            # Store the data
            all_data[year][file.replace(".csv", "")] = data       
            combined_data = pd.concat([combined_data, data])
        
        # Update progress bar
        pbar.update(n=1)
    
    return all_data, combined_data

    
def create_geospatial_dataset(
        df: pd.DataFrame,
        x_col: str,
        y_col: str,
        crs: str,
    ) -> gpd.GeoDataFrame:
    
    # Create geometry from coordinates
    df["geometry"] = gpd.points_from_xy(
        x=df[x_col],
        y=df[y_col],
        crs=crs,
        ) 
    
    # Create GeoDataFrame
    geo_df = gpd.GeoDataFrame(
        data=df,
        geometry=df["geometry"],
        crs=crs,
        )
    
    if geo_df.crs != crs:
        geo_df = geo_df.to_crs(crs)
    
    return geo_df


def apply_point_buffer(
        gdf: gpd.GeoDataFrame,
        buffer_m: int,
        geom_col: str,
    ) -> gpd.GeoDataFrame:
    """
    Converts to EPSG:27700 and applies a buffer around each point 
      within `gdf` with radius `radius_m`
    """
    
    print(f"Applying point buffer of {buffer_m} metres")
    
    if gdf.crs != "EPSG:27700":
        gdf = gdf.to_crs("EPSG:27700")
        
    if gdf.crs != "EPSG:27700":
        raise f"Error: GDF could not be converted from {gdf.crs} to EPSG:27700"
    else:
        gdf[f"buffered_{geom_col}"] = gdf[geom_col].buffer(buffer_m)
        print(f"Buffer created on {len(gdf):,} points from '{geom_col}' as 'buffered_{geom_col}'")
        return gdf


def ct()-> str:
    """Returns current time as string"""
    return datetime.now().strftime("%H:%M:%S")


def check_crs(gdf: gpd.GeoDataFrame, crs: str) -> gpd.GeoDataFrame:
    """
    Checks that `gdf` has passed `crs`
    else, tries to cast to `crs` before returning
    """
    
    if gdf.crs == crs:
        return gdf
    
    if gdf.crs != crs:
        print(f"Detected crs:{gdf.crs}, transforming to: {crs}")
        gdf = gdf.to_crs(crs)
        
    if gdf.crs == crs:
        return gdf
    
    gdf = gdf.to_crs(crs)
    
    if gdf.crs == crs:
        return gdf
    else:
        raise f"ERROR: Could not convert gdf from {gdf.crs} to {crs}"


# # # # PROCESS # # # #

def main():
    """Main"""
    
    #### Load crime data and convert to GeoSpatial
    crime_data, combined_crime_data = read_and_create_geo_data(
        data_dir=CRIME_DATA_PATH,
        x_col="Longitude",
        y_col="Latitude",
        crs="EPSG:4326",
        export_geo=False, 
        )    
    print(f"Crime data created for {len(combined_crime_data):,} offences.")
    
    #### Load NaPTAN data and convert to GeoSpatial
    naptan_data = read_naptan_data(
        data_dir=NAPTAN_DIR, 
        x_col="Longitude",
        y_col="Latitude",
        crs="EPSG:4326",
        export_geo=False,
        )
    print(f"NaPTAN data loaded for {len(naptan_data):,} PT stops.")
    
    #### Clean data (remove NaN's etc) - remove NaN's within functions?
    # TODO(JH): Check columns like "status" in naptan data
    naptan_data = naptan_data[naptan_data[GEOMETRY_COL].notna()]
    combined_crime_data = combined_crime_data[combined_crime_data[GEOMETRY_COL].notna()]
    
    # Remove crime data > 12 months old
    print("Removing combined crime data for crimes > 12 months ago.")
    
    
    #### Buffer NaPTAN stops with radius
    naptan_data = apply_point_buffer(
        gdf=naptan_data,
        buffer_m=NAPTAN_BUFFER_M,
        geom_col=GEOMETRY_COL,
        )    
    
    
    #### Perform spatial join
    
    # Currently has two geometries, drop one and set buffered as geometry
    naptan_data = naptan_data.drop(columns=[GEOMETRY_COL])
    naptan_data = gpd.GeoDataFrame(
        data=naptan_data,
        geometry=f"buffered_{GEOMETRY_COL}",
        )
    
    naptan_data = check_crs(gdf=naptan_data, crs="EPSG:27700")
    combined_crime_data = check_crs(gdf=combined_crime_data, crs="EPSG:27700")
    
    print(f"Commening sjoin: {ct()}")
    crime_near_naptan_stops = gpd.sjoin(
        how="inner",
        left_df=combined_crime_data,
        right_df=naptan_data,
        predicate="within",
        )
    crime_near_naptan_stops["crime_count"] = 1
    print(f"sjoin complete: {ct()}")
       
    
    crimes_per_stop = crime_near_naptan_stops.drop(
        columns=[GEOMETRY_COL]
        ).groupby(GROUPBY_COLS).sum()
    
    crimes_per_stop_filtered = crimes_per_stop[
        [
            "crime_count", "Crime ID", "Month", "Reported by", "Falls within",
            "Location", "LSOA code", "LSOA name", "Last outcome category",
            "ATCOCode", "CommonName", "Landmark", "Street", "NptgLocalityCode",
            "LocalityName", "Status", "CreationDateTime", "ModificationDateTime"
            ]
        ]
    
    print(f"\n{len(crimes_per_stop_filtered):,} stops have crime within "
          f"{NAPTAN_BUFFER_M} metres of NaPTAN stops {ct()}")
    
if __name__ == "__main__":
    main()
