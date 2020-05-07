#!/usr/bin/env python
# ****************************************************************************
# read_milan.py 
#
# DESCRIPTION: 
# Reads AQ observation data from Lombardy, Italy. 
#
# DATA SOURCE:
# https://www.arpalombardia.it/Pages/Aria/Richiesta-Dati.aspx 
#
# HISTORY:
# 20200506 - christoph.a.keller at nasa.gov - Initial version 
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
import glob

from ..parse_string import parse_date
from ..systools import load_config

OBSTYPE_MAP = {
 'Ozono':'o3',
 'Particelle sospese PM2.5':'pm25',
 'Biossido di Azoto':'no2',
}


def read_milan(iday=None,configfile='unknown',**kwargs):
    '''
    Read AQ observations from Lombardy, Italy, as obtained from
    https://www.arpalombardia.it/Pages/Aria/Richiesta-Dati.aspx 
    '''
    log = logging.getLogger(__name__)
    # read configuration file
    config = load_config(configfile)
    # output array
    dat = pd.DataFrame()
    # get list of files 
    files = config.get('input_files')
    if type(files)==type(""):
        files = [files]
    for ifiles in files:
        file_list = glob.glob(ifiles)
        for ifile in file_list:
            idat = _read_file ( config, ifile, **kwargs )
            if idat is not None:
                dat = dat.append(idat)
    return dat


def _read_file(config,ifile,firstday=None,lastday=None,skipnan=True,remove_negatives=True):
    '''Read individual file.'''
    log = logging.getLogger(__name__)
    # open file
    if not os.path.exists(ifile):
        log.warning('file not found: {}'.format(ifile))
        return None
    log.info('Reading {}'.format(ifile))
    # open header. These are the first 4 rows
    with open(ifile,'r', encoding='utf-8-sig') as f:
        lines = f.readlines()[:4]
    # get station name, pick lat/lon for that entry from configuration file
    sname = lines[0].replace('Stazione, ','').replace('\n','').replace('-','_').replace(' ','')
    if sname not in config.get('locations'):
        log.warning('No station information found for : {}'.format(sname))
        return None
    slat = config.get('locations').get(sname).get('lat')
    slon = config.get('locations').get(sname).get('lon')
    oname = config.get('locations').get(sname).get('name',sname)
    # get species and unit
    tmp = lines[3].replace('Data/Ora, ', '').split(' - ') 
    obstype_name = tmp[0]
    if obstype_name not in OBSTYPE_MAP:
        log.warning('obstype not found in mapping table: {}'.format(obstype_name))
        return None
    obstype = OBSTYPE_MAP[obstype_name]
    sunit = tmp[1].replace('\n','')
    sunit = 'ugm-3' if 'g/m' in sunit else sunit
    log.info('Read {} data (in units of {}) at location {}'.format(obstype,sunit,oname))
    df = pd.read_csv(ifile,delimiter=',',skiprows=4,header=None,names=['ISO8601','value'],na_filter=False) #,parse_dates=['ISO8601'],date_parser=lambda x: pd.datetime.strptime(x, '%Y/%m/%d %H:%M'))
    # parse dates. Different for PM2.5 and O3, NO2
    sfmt = '%Y/%m/%d %H:%M' if ':' in df['ISO8601'].values[0] else '%Y/%m/%d'
    dates_local = [dt.datetime.strptime(i,sfmt) for i in df['ISO8601']]
    # read dates and convert to UTC
    milan = timezone('Europe/Rome')
    utc = pytz.utc
    dates_utc = [milan.localize(i).astimezone(utc) for i in dates_local]
    # output data
    nrow = df.shape[0]
    idat = pd.DataFrame()
    idat['ISO8601'] = dates_utc 
    idat['localtime'] = dates_local 
    idat['original_station_name'] = [oname for i in range(nrow)]
    idat['lon'] = [slon for i in range(nrow)]
    idat['lat'] = [slat for i in range(nrow)]
    idat['unit'] = [sunit for i in range(nrow)]
    idat['obstype'] = [obstype for i in range(nrow)]
    idat['value'] = df['value']
    # Scan for first/last entry
    if skipnan:
        idat = idat.loc[~np.isnan(idat.value)]
    if remove_negatives:
        idat = idat.loc[idat.value>=0.0]
    if firstday is not None:
        firstday_tzaware = dt.datetime(firstday.year,firstday.month,firstday.day,tzinfo=pytz.utc)
        log.info('Only use data after {}'.format(firstday_tzaware))
        idat = idat.loc[idat['ISO8601'] >= firstday_tzaware]
    if lastday is not None:
        lastday_tzaware = dt.datetime(lastday.year,lastday.month,lastday.day,tzinfo=pytz.utc)
        log.info('Only use data before {}'.format(lastday_tzaware))
        ldat = ldat.loc[ldat['ISO8601'] < lastday_tzaware]
    idat = idat.sort_values(by='ISO8601')
    return idat
