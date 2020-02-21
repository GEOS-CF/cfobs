#!/usr/bin/env python
# ****************************************************************************
# read_quito.py 
#
# DESCRIPTION: 
# Reads AQ observation data from Quito, Ecuador and converts it to a csv table.
#
# DATA SOURCE:
# http://www.quitoambiente.gob.ec/ambiente/index.php/datos-horarios-historicos
#
# HISTORY:
# 20200221 - christoph.a.keller at nasa.gov - Initial version 
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

from ..parse_string import parse_date
from ..systools import load_config


def read_quito(iday=None,configfile='unknown',**kwargs):
    '''
    Read AQ observations from Quito, as obtained from
    http://www.quitoambiente.gob.ec/ambiente/index.php/datos-horarios-historicos
    '''
    log = logging.getLogger(__name__)
    # read configuration file
    config = load_config(configfile)
    # output array
    dat = pd.DataFrame()
    # read file for every variable
    vars = config.get('vars')
    for var in vars:
        idat = _read_file ( config, var, **kwargs )
        dat = dat.append(idat)
    return dat


def _read_file(config,var,firstday=None,lastday=None,skipnan=True,remove_negatives=True):
    '''Read individual file.'''
    log = logging.getLogger(__name__)
    idat = pd.DataFrame()
    varname = config.get('vars').get(var).get('name_on_file',var)
    varunit = config.get('vars').get(var).get('unit')
    # open file and parse Excel
    ifile_template = config.get('ifile_template','unknown')
    ifile = ifile_template.replace('%v',varname)
    if not os.path.exists(ifile):
        log.warning('file not found: {}'.format(ifile))
        return None
    log.info('Reading {}'.format(ifile))
    xl = pd.ExcelFile(ifile)
    df = xl.parse(xl.sheet_names[0])
    # read dates and convert to UTC
    datecol = df.keys()[0]
    dates_local = df[datecol].values[1:]
    quito = timezone('America/Bogota')
    utc = pytz.utc
    dates_utc = [quito.localize(i).astimezone(utc) for i in dates_local]
    ndates = len(dates_utc)
    # accumulate data by location in dataframe
    idat = pd.DataFrame()
    for iloc in config.get('locations'):
        if iloc not in df.keys():
            log.warning('Location "{}" not found in file - will skip it'.format(iloc))
            continue
        station_name = config.get('locations').get(iloc).get('name',iloc)
        lat = config.get('locations').get(iloc).get('lat',np.nan)
        lon = config.get('locations').get(iloc).get('lon',np.nan)
        log.info('Parsing {} (name={}; lat={}, lon={})'.format(iloc,station_name,lat,lon))
        ldat = pd.DataFrame()
        ldat['ISO8601'] = dates_utc
        ldat['localtime'] = dates_local
        values_as_char = [ str(i).replace(' ','') for i in df[iloc].values[1:] ]
        ldat['value'] = [ np.nan if i=='' else np.float(i) for i in values_as_char ]
        if skipnan:
            ldat = ldat.loc[~np.isnan(ldat.value)]
        if remove_negatives:
            ldat = ldat.loc[ldat.value>=0.0]
        if firstday is not None:
            firstday_tzaware = dt.datetime(firstday.year,firstday.month,firstday.day,tzinfo=pytz.utc)
            log.info('Only use data after {}'.format(firstday_tzaware))
            ldat = ldat.loc[ldat['ISO8601'] >= firstday_tzaware]
        if lastday is not None:
            lastday_tzaware = dt.datetime(lastday.year,lastday.month,lastday.day,tzinfo=pytz.utc)
            log.info('Only use data before {}'.format(lastday_tzaware))
            ldat = ldat.loc[ldat['ISO8601'] < lastday_tzaware]
        nrow = ldat.shape[0]
        ldat['original_station_name'] = [station_name for i in range(nrow)]
        ldat['lat'] = [lat for i in range(nrow)]
        ldat['lon'] = [lon for i in range(nrow)]
        # add to data frame
        idat = idat.append(ldat)
    # add species information
    idat = idat.sort_values(by='ISO8601')
    nrow_full = idat.shape[0]
    idat['obstype'] = [var for i in range(nrow_full)]
    idat['unit'] = [varunit for i in range(nrow_full)]
    return idat
