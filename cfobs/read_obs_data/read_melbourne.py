#!/usr/bin/env python
# ****************************************************************************
# read_melbourne.py 
#
# DESCRIPTION: 
# Reads AQ observation data from Melbourne 
#
# DATA SOURCE:
#
# HISTORY:
# 20200527 - christoph.a.keller at nasa.gov - Initial version 
# ****************************************************************************


# requirements
import glob
import logging
import os 
import argparse
import numpy as np
import datetime as dt
from pytz import timezone
import pytz
import pandas as pd
import yaml

from ..parse_string import parse_date
from ..systools import load_config
from ..cfobs_save import save as cfobs_save


def read_melbourne(iday=None,configfile=None,ifile=None,spec=None,firstday=None,lastday=None,**kwargs):
    '''
    Read Melbourne data 
    '''
    log = logging.getLogger(__name__)
    config = load_config(configfile)
    df = _read_file(ifile,config,spec,**kwargs)
    # filter by days
    if firstday is not None:
        log.info('Only use data after {}'.format(firstday))
        df = df.loc[df['ISO8601'] >= firstday]
    if lastday is not None:
        log.info('Only use data before {}'.format(lastday))
        df = df.loc[df['ISO8601'] < lastday]
    # write out stations if specified so
    return df


def _read_file(ifile,config,spec,time_offset=0,ofile_local=None,ofile_local_append=True,**kwargs):
    '''
    Read a single file
    '''
    log = logging.getLogger(__name__)
    log.info('Reading {}'.format(ifile))
    tb = pd.read_csv(ifile,sep=",")
    keys = list(tb.keys())
    tb = tb.rename(columns={keys[0]:'datetime'})
    # get dates
    dates = [dt.datetime.strptime(i,"%Y-%m-%d %H:%M:%S") for i in tb['datetime']]
    nrow = len(dates)
    # get variable information
    varunit = config.get('vars').get(spec).get('unit')
    varscal = config.get('vars').get(spec).get('scal')
    # do for all locations
    alldat = []
    for c in tb.keys():
        if c in ['datetime','mean','std']:
            continue
        # get station info
        name,lat,lon = _get_station(config,c,**kwargs)
        if name is None:
            continue 
        idf = pd.DataFrame()
        idf['ISO8601'] = dates
        idf['original_station_name'] = [name for i in range(nrow)] 
        idf['lat'] = [lat for i in range(nrow)] 
        idf['lon'] = [lon for i in range(nrow)] 
        idf['obstype'] = [spec for i in range(nrow)]
        idf['unit'] = [varunit for i in range(nrow)]
        idf['value'] = [i*varscal for i in tb[c].values]
        idf = idf.loc[~np.isnan(idf['value'])]
        if idf.shape[0]>0:
            alldat.append(idf)
            if ofile_local is not None:
                ofile = ofile_local.replace('%l',name)
                _ = cfobs_save(idf,ofile,dt.datetime(2018,1,1),append=ofile_local_append)
    df = pd.concat(alldat) if len(alldat)>0 else None
    return df


def _get_station(config,id,default_lat=None,default_lon=None,prefix=None):
    '''
    Get station information for the given ID
    '''
    log = logging.getLogger(__name__)
    locations = config.get('locations')
    name = '_'.join((prefix,str(id))) if prefix is not None else None
    lat = default_lat
    lon = default_lon 
    for l in locations:
        if locations.get(l).get('id') == id:
            name = l 
            lat = locations.get(l).get('lat')
            lon = locations.get(l).get('lon')
            break
    if name is None:
        log.warning('No station entry found for ID {}'.format(id))
        return None,None,None
    if name is None or lat is None or lon is None:
        log.warning('At least one entry missing for station ID {}'.format(id))
        return None,None,None
    return name,lat,lon
