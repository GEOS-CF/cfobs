#!/usr/bin/env python
# ****************************************************************************
# read_obs.py 
# 
# DESCRIPTION:
# Read observation data to a pandas DataFrame compatible with the cfobs obj.

# HISTORY:
# 20191220 - christoph.a.keller at nasa.gov - Initial version 
# ****************************************************************************
import os
import sys
import numpy as np
import datetime as dt
import pandas as pd
import logging

import cfobs.read_obs_data.read_openaq as openaq
import cfobs.read_obs_data.read_aeronet as aeronet
import cfobs.read_obs_data.read_rio as rio 

from .cfobs_save import save as cfobs_save
from .table_of_stations import update_stations_info 
from .table_of_stations import read_table_of_stations 
from .table_of_stations import write_table_of_stations 
from .table_of_stations import get_lat_lon_of_regular_grid 
from .parse_string import parse_key


# define read functions here 
read_functions = {
        "openaq": openaq.read_openaq,
       "aeronet": aeronet.read_aeronet,
        "rio": rio.read_rio,
        }


def read_obs(obskey,startday,endday=None,read_freq='1D',time_delta=-1,save=False,ofile='output_!k_%Y_%m_%d.csv',append_to_ofile=False,nfloats=-1,return_data=True,track_list_of_stations=False,stationsfile=None,gridres=1.0,location_name_prefix=None,**kwargs):
    '''
    Read native observation data and returns a cfobs-compatible data frame. 
    Also save the data to a csv file if specified so.
    This function is a generic wrapper that calls down to individual read 
    functions tailored toward various data sources (e.g., Aeronet, OpenAQ, 
    etc). If specified so, the read function is called multiple times 
    between the provided time interval.
    After calling the read function, this function updates the meta data
    for each observation point. In particular, it assigns a unique location
    name to it and also maps all observations onto a regular grid (the original
    observation location information is preserved).

    All read functions read the observation-type specific data sets and 
    'translate' them into a Pandas data frame compatible with this module. 
    The read functions all have the following form:
       df = function(date=type(dt.datetime),**kwargs).
    The returned Pandas data frame must contain all observations in separate
    lines, and have at least the following columns:
    'ISO8601': date and time stamp of the observation in UTC (type dt.datetime)
    'obstype': observation type identifier (type str)
    'value': observation (type float)
    'original_station_name': original station name (type str)
    'lat' : latitude of observation point, in degrees North (type float)
    'lon' : longitude of observation point, in degrees East (type float)

    Arguments
    ---------
    obskey : str
        Observation key attribute, specifies which read function is being called.
    startday : dt.datetime
        Start day for reading the data.
    endday : dt.datetime
        End day for reading the data.
    read_freq : str
        How often the read function shall be called between the specified time
        interval. If set to None, the read function will only be called once.
    time_delta : int
        Toss all observations outside the specified time range +/- time_delta.
        In hours. Only used if >=0.
    save : bool
        If true, saves the data frame to a csv file.
    ofile : str
        File name to save data to. Only used if save set to True.
    append_to_ofile : bool
        Append data to existing file. Only used if save set to True.
    nfloats : int
        Number of floating points to save. Only used if save set to True.
        Ignored if negative.
    return_data : bool
        Return the data frame with the function.
    track_list_of_stations : bool
        If true, write out the meta data of all observation sites into a
        separate file.
    stationsfile : str
        File with stations information. Must be provided if 
        track_list_of_stations is enabled.
    gridres : float
        Resolution of regular grid onto which the observations are mapped onto. 
    location_name_prefix : str 
        Prefix to be used for station name identifier. Passed to 'update_stations_info'.
    **kwargs : dict
        Additional arguments passed to the reading function.
    '''

    log = logging.getLogger(__name__)
    # check if requested observation key exists in funcs and get function for it
    assert(obskey in read_functions.keys()), 'Invalid observation key: {}'.format(obskey)
    readfunc = read_functions.get(obskey)
    # list of opened files
    opened_files = []
    # read stations data
    if track_list_of_stations:
        stationstable = read_table_of_stations(stationsfile,obskey)
    else:
        stationstable = pd.DataFrame()
    # get latitudes/longitudes to grid data to
    lats,lons = get_lat_lon_of_regular_grid(gridres)
    # get sequence of days to read
    startday = startday if startday is not None else dt.datetime(2018,1,1)
    if endday is None:
        endday = startday
    if read_freq is None:
        datelist = [startday]
    else:
        datelist = pd.date_range(start=startday,end=endday,freq=read_freq).tolist()
    # prepare return value
    fulldf = pd.DataFrame() if return_data else None
#---read data day by day
    for idate in datelist:
        log.info('working on '+idate.strftime('%Y-%m-%d'))
        sys.stdout.flush()
        # read data based on obs-specific function, as set at the beginning
        df = readfunc(idate,**kwargs)
        if df is None:
            df = pd.DataFrame()
        # remove all invalid entries before writing out
        if df.shape[0]>0:     
            df = df.loc[~np.isnan(df['lat'])]
            df = df.loc[~np.isnan(df['lon'])]
            df = df.loc[~np.isnan(df['value'])]
            # update location information
            df, stationstable = update_stations_info(df,stationstable,lats,lons,location_name_prefix)
            # remove all data outside the provide date range.
            df = _check_dates(df,idate,time_delta)
        else:
            log.warning('No data found for '+idate.strftime('%Y-%m-%d'))
        # save to csv file
        if df.shape[0]>0 and save:
            opened_files = cfobs_save(df=df,file=parse_key(ofile,obskey),iday=idate,opened_files=opened_files,append=append_to_ofile,nfloats=nfloats)
        # add to return frame
        if return_data and df.shape[0]>0:
            fulldf = fulldf.append(df)
#---cleanup
    if track_list_of_stations:
        write_table_of_stations(stationstable,stationsfile,obskey)
    return fulldf


def _check_dates(df,idate,time_delta=-1):
    '''
    Tosses all observations that are outside the day +- time-delta.
    '''

    if time_delta<0:
        return df
    log = logging.getLogger(__name__)
    # time delta
    delta = dt.timedelta(hours=time_delta)
    mindate = idate - delta 
    maxdate = idate + dt.timedelta(hours=24) + delta
    ncols1 = df.shape[0]
    df = df.loc[(df['ISO8601']>=mindate) & (df['ISO8601']<maxdate)].copy()
    ncols2 = df.shape[0]
    log.info('{:,} of {:,} observations removed because they are outside the specified time range!'.format(ncols1-ncols2,ncols1))
    return df
