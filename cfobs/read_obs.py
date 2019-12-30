#!/usr/bin/env python
# ****************************************************************************
# read_obs.py 
# 
# DESCRIPTION:
# Read observation data to a pandas DataFrame compatible with the cfobs obj.
# This is the parent function for a list of read functions tailored toward
# various data sources. Additional read functions can be added to the list
# of read functions below. 

# HISTORY:
# 20191220 - christoph.a.keller at nasa.gov - Initial version 
# ****************************************************************************
import os
import sys
import numpy as np
import datetime as dt
import pandas as pd

import cfobs.read_orig_data.read_openaq as openaq
#import read_migris as migris
from .cfobs_save import save as cfobs_save
from .table_of_stations import update_stations_info 
from .table_of_stations import read_table_of_stations 
from .table_of_stations import write_table_of_stations 
from .table_of_stations import get_lat_lon_of_regular_grid 
from .parse_string import parse_key


# define read functions here 
read_functions = {
#        "migris": migris.read_migris,
        "openaq": openaq.read
        }


def read_obs(obskey,startday,endday=None,read_freq='1D',time_delta=-1,verbose=0,save=False,ofile='output_!k_%Y_%m_%d.csv',return_data=True,stationsfile=None,track_list_of_stations=False,gridres=1.0,append_to_ofile=0,nfloats=-1,**kwargs):
    '''
    Read native observation data and returns a cfobs-compatible data frame. 
    Also save the data to a csv file if specified so.
    '''
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
    if endday is None:
        endday = startday
    daylist = pd.date_range(start=startday,end=endday,freq=read_freq).tolist()
    # prepare return value
    fulldf = pd.DataFrame() if return_data else None
#---read data day by day
    for iday in daylist:
        if verbose>0:
            print('working on '+iday.strftime('%Y-%m-%d'))
        sys.stdout.flush()
        # read data based on obs-specific function, as set at the beginning
        df = readfunc(iday=iday,verbose=verbose,**kwargs)
        if df is None:
            df = pd.DataFrame()
        # remove all invalid entries before writing out
        if df.shape[0]>0:     
            df = df.loc[~np.isnan(df['lat'])]
            df = df.loc[~np.isnan(df['lon'])]
            df = df.loc[~np.isnan(df['value'])]
            # update location information
            df, stationstable = update_stations_info(df,stationstable,lats,lons)
            # remove all data outside the provide date range.
            df = _check_dates(df,iday,verbose,time_delta)
        else:
            print('Warning: no data found for '+iday.strftime('%Y-%m-%d'))
        # save to csv file
        if df.shape[0]>0 and save:
            opened_files = cfobs_save(df=df,file=parse_key(ofile,obskey),iday=iday,opened_files=opened_files,verbose=verbose,append=append_to_ofile,nfloats=nfloats)
        # add to return frame
        if return_data and df.shape[0]>0:
            fulldf = fulldf.append(df)
#---cleanup
    if track_list_of_stations:
        write_table_of_stations(stationstable,stationsfile,obskey)
    return fulldf


def _check_dates(df,iday,verbose,time_delta=-1):
    '''
    Tosses all observations that are outside the day +- time-delta.
    '''
    # nothing to do if not defined
    if time_delta<0:
        return df
    # time delta
    delta = dt.timedelta(hours=time_delta)
    mindate = iday - delta 
    maxdate = iday + dt.timedelta(hours=24) + delta
    ncols1 = df.shape[0]
    df = df.loc[(df['ISO8601']>=mindate) & (df['ISO8601']<maxdate)].copy()
    ncols2 = df.shape[0]
    if verbose>1 or (verbose>0 and (ncols2<ncols1)):
        print('{:,} of {:,} observations removed because they are outside the specified time range!'.format(ncols1-ncols2,ncols1))
    return df
