#!/usr/bin/env python
# ****************************************************************************
# read_japan.py 
#
# DESCRIPTION: 
# Reads AQ observation data from Japan 
#
# DATA SOURCE:
# http://soramame.taiki.go.jp/Index.php 
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


def read_japan(iday=None,configfile=None,data_dir=None,firstday=None,lastday=None,stationsfile_local=None,**kwargs):
    '''
    Wrapper to read Rio data in two different forms
    '''
    log = logging.getLogger(__name__)
    config = load_config(configfile)
    idirs = sorted(glob.glob(data_dir))
    if len(idirs)==0:
        log.warning('No files found {}'.format(data_dir))
        return None
    # dictionary of stations if specified so
    stations = None
    if stationsfile_local is not None:
        if os.path.isfile(stationsfile_local):
            with open(stationsfile_local,'r') as f:
                stations = yaml.load(f, Loader=yaml.FullLoader)
        else:
            stations = {}
    for idir in idirs:
        ifiles = glob.glob(idir+'/*.csv')
        if len(ifiles)==0:
            log.warning('No files found {}'.format(idir))
            continue 
        df, stations =  _read_data(config,ifiles,stations,**kwargs)
    # filter by days
    if firstday is not None:
        log.info('Only use data after {}'.format(firstday))
        df = df.loc[df['ISO8601'] >= firstday]
    if lastday is not None:
        log.info('Only use data before {}'.format(lastday))
        df = df.loc[df['ISO8601'] < lastday]
    # write out stations if specified so
    if stationsfile_local is not None:
        with open(stationsfile_local,'w') as file:
            yaml.dump(stations, file)
        log.info('Written YAML file: {}'.format(stationsfile_local))
    return df


def _read_data(config,ifiles,stations,**kwargs):
    '''
    Read all data from a directory
    '''
    log = logging.getLogger(__name__)
    dats = []
    for ifile in ifiles:
        idat, stations = _read_file(ifile,config,stations,**kwargs)
        if idat is not None:
            dats.append(idat)
    df = pd.concat(dats,ignore_index=True) if len(dats)>0 else pd.DataFrame() 
    return df, stations


def _read_file(ifile,config,stations,time_offset=0,ofile_local=None,ofile_local_append=True,**kwargs):
    '''
    Read a single file
    '''
    log = logging.getLogger(__name__)
    log.info('Reading {}'.format(ifile))
    tb = pd.read_csv(ifile,sep=",",encoding="ISO-8859-1")
    keys = list(tb.keys())
    tb = tb.rename(columns={keys[0]:'station',keys[1]:'date',keys[2]:'hour'})
    # get station info
    if len(tb.station.unique())>1:
        log.warning('More than one station ID found in {}'.format(ifile))
    name,lat,lon = _get_station(config,tb.station.values[0],**kwargs)
    if name is None:
        return None
    # get dates
    offset = dt.timedelta(minutes=time_offset)
    days = [dt.datetime.strptime(i,"%Y/%m/%d") for i in tb['date']]
    hour = [i if i<=23 else 0 for i in tb['hour']]
    dates = [dt.datetime(i.year,i.month,i.day,j,0,0) for i,j in zip(days,hour)]
    dates = [i+dt.timedelta(hours=24) if i.hour==0 else i for i in dates]
    nrow = len(dates)
    alldat = []
    vars = config.get('vars')
    for v in vars:
        name_on_file = vars.get(v).get('name_on_file',v)
        scal = vars.get(v).get('scal',1.0)
        ounit = vars.get(v).get('out_unit','NaN')
        if name_on_file not in tb:
            log.warning('Not found in file - skip: {}'.format(name_on_file))
            continue
        idf = pd.DataFrame()
        idf['ISO8601'] = dates
        idf['original_station_name'] = [name for i in range(nrow)] 
        idf['lat'] = [lat for i in range(nrow)] 
        idf['lon'] = [lon for i in range(nrow)] 
        idf['obstype'] = [v for i in range(nrow)]
        idf['unit'] = [ounit for i in range(nrow)]
        idf['value'] = [i*scal for i in tb[name_on_file].values]
        idf = idf.loc[~np.isnan(idf['value'])]
        if idf.shape[0]>0:
            alldat.append(idf)
    df = pd.concat(alldat) if len(alldat)>0 else None
    if df is not None and ofile_local is not None:
        ofile = ofile_local.replace('%l',name)
        _ = cfobs_save(df,ofile,dt.datetime(2018,1,1),append=ofile_local_append)
    # eventually update stations entry
    if stations is not None:
        if name not in stations:
            stations[name] = {'lat':'{:.4f}'.format(lat),'lon':'{:.4f}'.format(lon)}
    return df, stations


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
