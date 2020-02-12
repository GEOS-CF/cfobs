#!/usr/bin/env python
# ****************************************************************************
# read_rio.py 
#
# DESCRIPTION: 
# Reads AQ observation data from Rio de Janeiro and converts it to a csv table.
# 
# HISTORY:
# 20190212 - christoph.a.keller at nasa.gov - Adapted from older code 
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


def read_rio(iday=None,configfile=None):
    '''
    Reads and merges the AQ observations from Rio de Janeiro, as prepared by 
    Felipe Mandarino (felipe.mandarino@rio.rj.gov.br).
    '''

    log = logging.getLogger(__name__)
    # read configuration file
    config = load_config(configfile)
    ifile_template = config.get('ifile_template')
    idate = iday if iday is not None else dt.datetime(2018,1,1)
    # try to read all files and write to data frame 
    df = pd.DataFrame()
    for location in config.get('locations'):
        idf = _read_file(config,ifile_template,idate,location)
        if idf is not None:
            df = df.append(idf)
    # sort data
    log.debug('df keys: {}'.format(df.keys().tolist()))
    df = df[['ISO8601','original_station_name','lat','lon','obstype','unit','value']]
    df = df.sort_values(by="ISO8601")
    # write data frame
#    if ofile_template is not None:
#        ofile = parse_date(ofile_template,idate)
#        df.to_csv(ofile,date_format='%Y-%m-%dT%H:%M:%SZ',index=False,float_format='%g',na_rep='NaN')
#        log.info('{} values written to {} (from {} files)'.format(df.shape[0],ofile,len(files)))
    return df


def _read_file(config,ifile_template,idate,location):
    '''Read an individual file'''
    log = logging.getLogger(__name__)
    ifile = ifile_template.replace('%l',location)
    ifile = parse_date(ifile,idate)
    if not os.path.exists(ifile):
        log.warning('file not found: {}'.format(ifile))
        return None
    # read data
    log.info('Reading {}'.format(ifile))
    tb = pd.read_csv(ifile,sep=";",encoding="ISO-8859-1")
    # variables
    vars = config.get('vars')
    nrow = tb.shape[0]
    nrow_tot = nrow * len(vars)
    # output dataframe 
    df = pd.DataFrame()
    # get dates
    offset = dt.timedelta(minutes=-30)
    dates = [dt.datetime.strptime(i,"%d/%m/%Y %H:%M")+offset for i in tb['Data']]
    rio = timezone('America/Buenos_Aires')
    utc = pytz.utc
    dates = [rio.localize(i).astimezone(utc) for i in dates]
    # get station information
    sname = config.get('locations').get(location).get('shortname',location)
    slat  = config.get('locations').get(location).get('lat',np.nan)
    slon  = config.get('locations').get(location).get('lon',np.nan)
    # read values and add to dataframe
    for v in vars:
        name_on_file = vars.get(v).get('name_on_file',v)
        idf = pd.DataFrame()
        idf['ISO8601'] = dates
        idf['obstype'] = [v for x in range(nrow)]
        idf['unit'] = [vars.get(v).get('unit','NaN') for x in range(nrow)]
        idf['value'] = [np.float(str(x).replace(",",".")) for x in tb[name_on_file].values]
        df = df.append(idf)
    # add station information
    df['original_station_name'] = [location for x in range(nrow_tot)]
    df['lat'] = [slat for x in range(nrow_tot)]
    df['lon'] = [slon for x in range(nrow_tot)]
    return df
