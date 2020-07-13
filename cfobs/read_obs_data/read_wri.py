#!/usr/bin/env python
# ****************************************************************************
# read_wri.py 
#
# DESCRIPTION: 
# Read AQ observation data as prepared by Armando Retama (WRI). 
#
# DATA SOURCE:
# Armando Retama: armando.retama@gmail.com 
#
# HISTORY:
# 20200710 - christoph.a.keller at nasa.gov - Initial version 
# ****************************************************************************


# requirements
import logging
import os 
import argparse
import numpy as np
import datetime as dt
from pytz import timezone
import pytz
import pandas as pd
import yaml

from ..units import conv_ugm3_to_ppbv_std
from ..systools import load_config 


def read_wri(iday=None,data=[],sites=[],locfile=None,**kwargs):
    '''
    Read AQ observations prepared by Armando Retama.
    Observations and site information is provided in
    separate files.
    '''
    log = logging.getLogger(__name__)
    # output array
    dat = pd.DataFrame()
    # do for every entry
    data = [data] if type(data) == type('') else data
    sites = [sites] if type(sites) == type('') else sites
    locations = {}
    if locfile is not None:
        if os.path.isfile(locfile):
            locations = load_config(locfile)
    for d,s in zip(data,sites):
        idat, ilocs = _read_file ( d, s, **kwargs )
        dat = dat.append(idat)
        locations.update(ilocs)
    # eventually write out locations to yaml file
        with open(locfile, 'w') as ofile:
            yaml.dump(locations, ofile)
        log.info('locations written to {}'.format(locfile))
    return dat


def _read_file(data,site,firstday=None,lastday=None,skipnan=True,remove_negatives=True,fmt="%m/%d/%Y %H:%M"):
    '''Read individual file.'''
    log = logging.getLogger(__name__)
    # parse site information 
    if not os.path.exists(site):
        log.warning('site file not found: {}'.format(site))
        return None
    log.info('Reading {}'.format(site))
    st = pd.read_csv(site,encoding="ISO-8859-1")
    st = st.rename(columns={'Latitude':'lat','Longitude':'lon'})
    st['original_station_name'] = ['_'.join(('WRI',i,j,k)).replace(' ','_').replace('-','').replace('.','') for i,j,k in zip(st['City'],st['SiteName'],st['SiteID'])]
    # read data 
    if not os.path.exists(data):
        log.warning('data file not found: {}'.format(data))
        return None
    log.info('Reading {}'.format(data))
    df = pd.read_csv(data,encoding="ISO-8859-1")
    df = df.merge(st[['SiteID','lat','lon','original_station_name']],on='SiteID')
    # read dates 
    df['ISO8601'] = [dt.datetime.strptime(i,fmt) for i in df['UTCDatetime']]
    df['localtime'] = [dt.datetime.strptime(i,fmt) for i in df['LocalDatetime']]
    # accumulate by species
    idfs = []
    for k in df.keys():
        obstype = None; unit = None
        k = [k for i in ['O3(','CO(','NO2(','PM2.5('] if i in k]
        if len(k)!=1:
            continue 
        k = k[0]
        obstype = k.split('(')[0].lower().replace('.','')
        df[k] = [str(i).strip() for i in df[k]]
        df.loc[df[k]=='',k] = 'nan'
        df[k] = [np.float(i) for i in df[k]]
        idf = df.loc[~np.isnan(df[k])].copy()
        if idf.shape[0]==0:
            continue
        ldat = idf[['ISO8601','localtime','original_station_name','lat','lon']].copy()
        unit = _get_unit(k)
        ldat['obstype'] = [obstype for i in range(ldat.shape[0])]
        ldat['value'] = idf[k]
        # convert standard atmospheric O3 and NO2 to ppbv
        if obstype in ['o3','no2'] and 'g/sm3' in unit:
            mw = 48.0 if obstype=='o3' else 46.0
            conv = conv_ugm3_to_ppbv_std(mw)
            ldat['value'] = [i*conv for i in ldat['value'].values]
            unit = 'ppbv'
            log.info('Converted standard atmosphere {} ug/m3 to ppbv using factor of {:.3f}'.format(obstype,conv))
        ldat['unit'] = [unit for i in range(ldat.shape[0])]
        idfs.append(ldat) 
    # merge individual data sets
    if len(idfs) > 0:
        idat = pd.concat(idfs)
    else:
        idat = None 
        log.warning('No valid data found!')
    if idat is not None:
        if firstday is not None:
            log.info('Only use data after {}'.format(firstday))
            idat = idat.loc[idat['ISO8601'] >= firstday].copy()
        if lastday is not None:
            log.info('Only use data before {}'.format(lastday))
            idat = idat.loc[idat['ISO8601'] < lastday].copy()
        idat = idat.sort_values(by='ISO8601')
        # locations info
        locations = {}
        locs = idat.groupby('original_station_name').mean().reset_index()
        for l in locs.original_station_name.unique():
            ilat = locs.loc[locs.original_station_name==l].lat.values[0]
            ilon = locs.loc[locs.original_station_name==l].lon.values[0]
            locations[l] = {'lat':'{:.4f}'.format(ilat),'lon':'{:.4f}'.format(ilon)}
    return idat, locations


def _get_unit(k):
    unit = k.split('(')[1].split(')')[0]
    if 'ppb' in unit:
        unit = 'ppbv'
    if 'ppm' in unit:
        unit = 'ppmv'
    if 'g/m3' in unit:
        unit = 'ug/m3'
    if 'g/sm3' in unit:
        unit = 'ug/m3'
    return unit
