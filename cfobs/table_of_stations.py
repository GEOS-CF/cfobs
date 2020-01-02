#!/usr/bin/env python
# ****************************************************************************
# table_of_stations.py 
# 
# DESCRIPTION:
# Keep track of the observation stations per observation type.

# HISTORY:
# 20191220 - christoph.a.keller at nasa.gov - Initial version 
# ****************************************************************************
import os
import numpy as np
import datetime as dt
import pandas as pd


def read_table_of_stations(stationsfile,obskey=""):
    '''
    Read a previously saved stations table. 
    '''
    ifile = stationsfile.replace('%t',obskey)
    if os.path.isfile(ifile):
        stationstable = pd.read_csv(ifile)
    else:
        stationstable = pd.DataFrame()
    return stationstable


def update_stations_info(df,st,lats,lons):
    '''
    Update the stations information in both the observation data frame and the stations list.
    The stations table is a collection of all locations. Because the station names are not 
    always unique and can contain non-ascii characters, a new, unique station name is defined 
    for each location. In addition, each station is also mapped onto the 'reduced' grid, and 
    the corresponding lat/lon coordinates and a unique station name for this grid location are 
    also provided. 
    This function compares the observation data frame against the passed station table, and 
    adds any new locations to the stations table. Once the stations table has been updated, 
    the stations meta data is added to the observations data frame.
    To faciliate indexing, the latitude and longitude values are transformed to a single float
    value using (lat+90.0)*1.e7 + (lon+180.0).
    '''
    # Add dummy station if entry is empty. This makes the handling below easier
    if st.shape[0]==0:
        st['location']          = ['unknown']
        st['original_station_name']      = ['unknown']
        st['lat']               = [-999.0]
        st['lon']               = [-999.0]
        st['latlon_id']         = [-999.0]
        st['lat_gridded']       = [-999.0]
        st['lon_gridded']       = [-999.0]
        st['latlon_id_gridded'] = [-999.0]
        st['location_gridded']  = ['unknown']
        nstat                   = 0
    else:
        nstat = st.shape[0]
    # create a new entry that contains the lat/lon in one float.
    df['lat']       = df['lat'].values.round(4)
    df['lon']       = df['lon'].values.round(4)
    df['latlon_id'] = [(i+90.0)*1.0e7+(j+180.0) for i,j in zip(df['lat'].values,df['lon'].values)]
    # Get missing stations
    df = df.set_index('latlon_id')
    st = st.set_index('latlon_id')
    missing = df.index.difference(st.index) 
    st = st.reset_index()
    df = df.reset_index()
    if len(missing) > 0:
        idf = df.loc[df['latlon_id'].isin(missing),['original_station_name','lat','lon','latlon_id']].groupby(['latlon_id']).min().reset_index()
        # Add unique station name. this is simply 'StationXX' where XX is a unique number
        idf['location'] = ['Station'+str(i+nstat).zfill(7) for i in range(idf.shape[0])]
        # Grid data onto grid and assign station name to it.
        if lats is not None and lons is not None:
            idf['lon_gridded']       = [lons[np.abs(lons-i).argmin()] for i in idf.lon.values] 
            idf['lat_gridded']       = [lats[np.abs(lats-i).argmin()] for i in idf.lat.values]
            idf['location_gridded']  = ['Station_{0:07.2f}E_{1:06.2f}N'.format(i,j) for i,j in zip(idf['lon_gridded'].values,idf['lat_gridded'].values)]
        else:
            idf['lon_gridded']       = np.zeros((len(missing),))*np.nan
            idf['lat_gridded']       = np.zeros((len(missing),))*np.nan
            idf['location_gridded']  = ['unknown' for i in range(len(missing))]
        idf['latlon_id_gridded'] = [(i+90.0)*1.0e7+(j+180.0) for i,j in zip(idf['lat_gridded'].values,idf['lon_gridded'].values)]
        # Add to stations file
        st = pd.concat([st,idf],sort=True)
        # Eventually remove dummy station
        st = st.loc[st['original_station_name']!='unknown']
    # add extended stations information to main dataset
    df = df.merge(st[['latlon_id','location','location_gridded','lon_gridded','lat_gridded','latlon_id_gridded']],on='latlon_id')
    df = df.sort_values(by="ISO8601")
    return df, st


def write_table_of_stations(stationstable,stationsfile,obskey=""):
    '''
    Writes the stations file to disk
    '''
    if stationstable.shape[0]==0:
        return
    ofile = stationsfile.replace('%t',obskey)
    stationstable = stationstable[['location','lat','lon','latlon_id','original_station_name','location_gridded','lat_gridded','lon_gridded','latlon_id_gridded']]
    stationstable.to_csv(ofile,index=False,header=True,float_format='%.4f')
    return


def get_lat_lon_of_regular_grid(gridres=1.0):
    '''
    Returns the latitudes and longitudes on the gridfile.
    '''
    assert(gridres>0.0), 'Invalid grid resolution: {}'.format(gridres)
    gridres_half = gridres / 2.0
    lats = np.arange(-90.0+gridres_half,90.0,gridres)
    lons = np.arange(-180.0+gridres_half,180.0,gridres)
    return lats,lons
