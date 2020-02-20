#!/usr/bin/env python
# ****************************************************************************
# read_rio.py 
#
# DESCRIPTION: 
# Reads AQ observation data from Rio de Janeiro and converts it to a csv table.
#
# DATA SOURCE:
# data.rio/datasets/dados-horarios-do-monitoramento-da-qualidade-do-ar-monitorar/data
#
# HISTORY:
# 20190212 - christoph.a.keller at nasa.gov - Adapted from older code 
# 20200220 - christoph.a.keller at nasa.gov - Update to new data format (from online file)
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


def read_rio(iday=None,configfile=None,firstday=None,lastday=None,time_offset=0):
    '''
    Read AQ observations from Rio de Janeiro, as obtained from
    data.rio/datasets/dados-horarios-do-monitoramento-da-qualidade-do-ar-monitorar/data
    '''
    log = logging.getLogger(__name__)
    # read configuration file
    config = load_config(configfile)
    ifile = config.get('source_file','unknown')
    if not os.path.exists(ifile):
        log.warning('file not found: {}'.format(ifile))
        return None
    log.info('Reading {}'.format(ifile))
    tb = pd.read_csv(ifile,sep=",") ##,encoding="ISO-8859-1")
    # get dates
    offset = dt.timedelta(minutes=time_offset)
    tb['ISO8601'] = [dt.datetime.strptime(i,"%Y-%m-%dT%H:%M:00.000Z")+offset for i in tb['Data']]
    if firstday is not None:
        log.info('Only use data after {}'.format(firstday))
        tb = tb.loc[tb['ISO8601'] >= firstday]
    if lastday is not None:
        log.info('Only use data before {}'.format(lastday))
        tb = tb.loc[tb['ISO8601'] < lastday]
    #rio = timezone('America/Buenos_Aires')
    #utc = pytz.utc
    #dates = [rio.localize(i).astimezone(utc) for i in dates]
    # output dataframe 
    df = pd.DataFrame()
    # read values and add to dataframe
    nrow = tb.shape[0]
    vars = config.get('vars')
    for v in vars:
        name_on_file = vars.get(v).get('name_on_file',v)
        idf = pd.DataFrame()
        idf['ISO8601'] = tb['ISO8601']
        idf['obstype'] = [v for x in range(nrow)]
        idf['unit'] = [vars.get(v).get('unit','NaN') for x in range(nrow)]
        idf['value'] = [np.float(str(x).replace(",",".")) for x in tb[name_on_file].values]
        idf['lat'] = tb['Lat'].values
        idf['lon'] = tb['Lon'].values
        idf['original_station_name'] = tb['Estação'].values
        df = df.append(idf)
    # map station names 
    for iloc in config.get('locations'):
        longname = config.get('locations').get(iloc).get('longname',iloc)
        df['original_station_name'] = [longname if x==iloc else x for x in df['original_station_name'].values]
    return df
