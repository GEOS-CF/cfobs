#!/usr/bin/env python
# ****************************************************************************
# read_spartan.py 
#
# DESCRIPTION: 
# Read observations downloaded from the SPARTAN global particulate matter
# network.
#
# DATA SOURCE:
# https://spartan-network.org
#
# HISTORY:
# 20200311 - christoph.a.keller at nasa.gov - Initial version 
# ****************************************************************************

# requirements
import logging
import os 
import glob
import argparse
import numpy as np
import datetime as dt
import pytz
import pandas as pd
import yaml
 
from ..regions import get_timezone

def read_spartan(iday=None,ifiles='unknown',**kwargs):
    '''
    Read observations downloaded from the SPARTAN global 
    particulate matter network (https://spartan-network.org/)
    '''
    log = logging.getLogger(__name__)
    # files to read 
    files = glob.glob(ifiles)
    if len(files)==0:
        log.warning('No files found in {}'.format(ifiles))
        return None
    df = pd.DataFrame()
    # read all files
    for ifile in files:
        idf = _read_single_file(ifile,**kwargs)
        if idf is not None:
            df = df.append(idf)
    df = df.sort_values(by="ISO8601")
    return df


def _read_single_file(ifile,firstday=None,lastday=None):
    '''Read a single WDCGG file.'''
    source = 'SPARTAN network'
    log = logging.getLogger(__name__)
    log.info('Reading {}'.format(ifile))
    ds = pd.read_csv(ifile,skiprows=1)
    # extract fields 
    if 'City' not in ds.keys():
        log.warning('keyword "City" is missing in {} - skip file'.format(ifile))
    if 'Latitude' not in ds.keys():
        log.warning('keyword "Latitude" is missing in {} - skip file'.format(ifile))
    if 'Longitude' not in ds.keys():
        log.warning('keyword "Longitude" is missing in {} - skip file'.format(ifile))
    if 'Year_local' not in ds.keys():
        log.warning('keyword "Year_local" is missing in {} - skip file'.format(ifile))
    if 'Month_local' not in ds.keys():
        log.warning('keyword "Month_local" is missing in {} - skip file'.format(ifile))
    if 'Day_local' not in ds.keys():
        log.warning('keyword "Day_local" is missing in {} - skip file'.format(ifile))
    if 'hour_local' not in ds.keys():
        log.warning('keyword "hour_local" is missing in {} - skip file'.format(ifile))
    if 'Value' not in ds.keys():
        log.warning('keyword "Value" is missing in {} - skip file'.format(ifile))
    if 'Units' not in ds.keys():
        log.warning('keyword "Units" is missing in {} - skip file'.format(ifile))
    # get lat & lon first, needed to convert to UTC time
    lats = ds['Latitude'].values
    lons = ds['Longitude'].values
    if len(np.unique(lats)) > 1:
        log.warning('More than one latitude value found - will use most common value for all entries')
    lat = max(set(list(lats)),key=list(lats).count)
    if len(np.unique(lons)) > 1:
        log.warning('More than one longitude value found - will use most common value for all entries')
    lon = max(set(list(lons)),key=list(lons).count)
    # station name
    names = list(ds['City'].values)
    if len(np.unique(names)) > 1:
        log.warning('More than one station name value found - will use most common value for all entries')
    name = max(set(list(names)),key=list(names).count)
    # extract (local) date and time
    year = ds['Year_local'].values
    month = ds['Month_local'].values
    day = ds['Day_local'].values
    hour = ds['hour_local'].values
    dates_local = [dt.datetime(y,m,d,h,0,0) for y,m,d,h in zip(year,month,day,hour)] 
    # convert to UTC
    this_timezone = get_timezone(lat,lon)
    localtz = pytz.timezone(this_timezone)
    log.debug('Location,timezone: {},{}'.format(name,this_timezone))
    utc = pytz.utc
    dates = [localtz.localize(i).astimezone(utc) for i in dates_local]
    nrow = len(dates)
    # Extract values and units
    values = ds['Value'].values
    units = ds['Units'].values 
    if len(np.unique(units)) > 1:
        log.warning('More than one unit value found - will use most common value for all entries')
    unit = max(set(list(units)),key=list(units).count)
    unit = 'ugm-3' if 'Micrograms per cubic meter' in unit else unit
    log.debug('Will use unit: {}'.format(unit))
    # Extract obstype
    if 'Parameter_Name' not in ds.keys():
        log.warning('keyword "Parameter_Name" is missing in {} - will set it to "pm25"')
        obstypes = ["pm25" for i in range(nrow)]
    else:
        obstypes = ds['Parameter_Name'].values 
        obstypes = ['pm25' if 'Estimated PM2.5 mass' in i else i for i in obstypes]
    # Set output dataframe 
    idf = pd.DataFrame()
    idf['ISO8601'] = dates
    idf['localtime'] = dates_local
    idf['original_station_name'] = [name for i in range(nrow)] 
    idf['lat'] = [lat for i in range(nrow)] 
    idf['lon'] = [lon for i in range(nrow)] 
    idf['obstype'] = obstypes 
    idf['unit'] = units
    idf['value'] = values
    idf['source'] = [source for i in range(nrow)]
    # Eventually reduce to specified time range
    if firstday is not None:
        log.debug('Only use data after {}'.format(firstday))
        idf = idf.loc[idf['ISO8601'] >= utc.localize(firstday)]
    if lastday is not None:
        log.debug('Only use data before {}'.format(lastday))
        idf = idf.loc[idf['ISO8601'] < utc.localize(lastday)]
    return idf
