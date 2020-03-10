#!/usr/bin/env python
# ****************************************************************************
# read_gaw_wdcrg.py 
#
# DESCRIPTION: 
# Read observations downloaded from the GAW World Data Centre for Reactive Gases (WDCRG)
#
# DATA SOURCE:
# http://ebas.nilu.no/ 
#
# COMMENTS:
# This module requires nappy: https://github.com/cedadev/nappy
#
# HISTORY:
# 20200304 - christoph.a.keller at nasa.gov - Initial version 
# ****************************************************************************

# requirements
import logging
import os 
import glob
import argparse
import numpy as np
import datetime as dt
from pytz import timezone
import pytz
import pandas as pd
import yaml


def read_gaw_wdcrg(iday=None,ifiles='unknown',**kwargs):
    '''
    Read observations downloaded from the GAW World Data Centre for 
    Reactive Gases (WDCRG): http://ebas.nilu.no/. 
    '''
    log = logging.getLogger(__name__)
    # read configuration file and get files to read
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


def _read_single_file(ifile,firstday=None,lastday=None,time_offset=0):
    '''Read a single GAW WDCRG file.'''
    import nappy
    log = logging.getLogger(__name__)
    log.info('Reading {}'.format(ifile))
    ds = nappy.openNAFile(ifile)
    ds.readData()
    keys = ds.getNADict().keys()
    # check for all required entries
    if 'DATE' not in keys:
        log.warning('Cannot get reference time - skip entry: {}'.format(ifile))
        return None
    if 'VNAME' not in keys:
        log.warning('Cannot get variable names - skip entry: {}'.format(ifile))
        return None
    if 'X' not in keys:
        log.warning('Cannot get dates - skip entry: {}'.format(ifile))
        return None
    if 'V' not in keys:
        log.warning('Cannot get values - skip entry: {}'.format(ifile))
        return None
    if 'NCOM' not in keys:
        log.warning('Cannot get comments - skip entry: {}'.format(ifile))
        return None
    idf = pd.DataFrame()
    # reference date 
    refdate_list = ds['DATE']
    refdate = dt.datetime(refdate_list[0],refdate_list[1],refdate_list[2])
    # parse start dates, round to nearest hour
    offset = dt.timedelta(minutes=time_offset)
    start = [round_to_nearest_hour(refdate+dt.timedelta(days=i)) for i in ds['X']] 
    vnames = ds['VNAME']
    # parse end dates, round to nearest hour
    if 'end_time of measurement' in vnames[0]:
        end = [round_to_nearest_hour(refdate+dt.timedelta(days=i)) for i in ds['V'][0]] 
    else:
        end = start
    # Observation is middle of time stamp
    idf['ISO8601'] = [refdate+((i-refdate)+(j-refdate))/2+offset for i,j in zip(start,end)]
    nobs = idf.shape[0]
    # get station information
    station_name = 'unknown'
    station_lat = np.nan
    station_lon = np.nan
    for c in ds['NCOM']:
        if 'Station name' in c:
            station_name = c.split(':')[1].replace(' ','')
        if 'Station latitude' in c:
            station_lat = np.float(c.split(':')[1].replace(' ',''))
        if 'Station longitude' in c:
            station_lon = np.float(c.split(':')[1].replace(' ',''))
    if station_name == 'unknown':
        log.warning('Unknown station name for file {}'.format(ifile))
    if np.isnan(station_lat):
        log.warning('Unknown station latitude for file {}'.format(ifile))
    if np.isnan(station_lon):
        log.warning('Unknown station longitude for file {}'.format(ifile))
    idf['lat'] = [station_lat for i in range(nobs)]
    idf['lon'] = [station_lon for i in range(nobs)]
    idf['original_station_name'] = [station_name for i in range(nobs)]
    # get observation type, unit, and values. This is currently hard-coded,
    # could probably be done better.
    ocol = -1
    for i,v in enumerate(vnames):
        # Skip standard deviation 
        if 'stddev' in v:
            continue
        if 'numflag' in v:
            continue
        # Species check
        vals = v.split(',')
        ofnd = False
        if 'ozone' in vals[0]:
            obstype = 'o3'
            ocol = i
            ofnd = True
        if 'nitrogen_dioxide' in vals[0]:
            obstype = 'no2'
            ocol = i
            ofnd = True
        # Unit check
        if ofnd:
            u = vals[1]
            if 'nmol/mol' in u:
                obsunit = 'ppbv'
                scal = 1.0 
            if 'mmol/mol' in u:
                obsunit = 'ppmv'
                scal = 1.0 
            if 'ug/m3' in u:
                obsunit = 'ugm-3'
                scal = 1.0 
            if 'ug N/m3' in u:
                obsunit = 'ugm-3'
                if obstype=='no2':
                    scal = 46./14.
                if obstype=='no':
                    scal = 30./14.
    if ocol<0:
        log.warning('Cannot find proper obstype - skip entry: {}'.format(ifile))
        return None
    log.debug('species, unit, scalefactor: {}, {}, {}'.format(obstype,obsunit,scal))
    log.debug('Will read concentration data from column: "{}"'.format(vnames[ocol]))
    obs  = np.array(ds['V'][ocol])*scal
    # Check for flags
    if 'numflag' in vnames[-1]:
        flag = np.array(ds['V'][-1])
        obs[np.where(flag!=0.0)] = np.nan
    idf['obstype'] = [obstype for i in range(nobs)]
    idf['unit'] = [obsunit for i in range(nobs)]
    idf['value'] = obs
    # Eventually reduce to specified time range
    if firstday is not None:
        log.info('Only use data after {}'.format(firstday))
        idf = idf.loc[idf['ISO8601'] >= firstday]
    if lastday is not None:
        log.info('Only use data before {}'.format(lastday))
        idf = idf.loc[idf['ISO8601'] < lastday]
    return idf


def round_to_nearest_hour(t):
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)+dt.timedelta(hours=t.minute//30)) 
