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


def read_rio(iday=None,configfile=None,type='api',firstday=None,lastday=None,**kwargs):
    '''
    Wrapper to read Rio data in two different forms
    '''
    log = logging.getLogger(__name__)
    config = load_config(configfile)
    if type=='api':
        df = read_rio_api(iday,config,**kwargs)
    if type=='bruno':
        df = read_rio_bruno(iday,config,**kwargs)
    # filter by days
    if firstday is not None:
        firstday_tzaware = dt.datetime(firstday.year,firstday.month,firstday.day,tzinfo=pytz.utc)
        log.info('Only use data after {}'.format(firstday_tzaware))
        df = df.loc[df['ISO8601'] >= firstday_tzaware]
    if lastday is not None:
        lastday_tzaware = dt.datetime(lastday.year,lastday.month,lastday.day,tzinfo=pytz.utc)
        log.info('Only use data before {}'.format(lastday_tzaware))
        df = df.loc[df['ISO8601'] < lastday_tzaware]
    # map station names 
    for iloc in config.get('locations'):
        longname = config.get('locations').get(iloc).get('longname',iloc)
        df['original_station_name'] = [longname if x==iloc else x for x in df['original_station_name'].values]
    return df


def read_rio_api(iday,config,time_offset=0):
    '''
    Read AQ observations from Rio de Janeiro, as obtained from
    data.rio/datasets/dados-horarios-do-monitoramento-da-qualidade-do-ar-monitorar/data
    '''
    log = logging.getLogger(__name__)
    # read configuration file
    ifile = config.get('source_file','unknown')
    if not os.path.exists(ifile):
        log.warning('file not found: {}'.format(ifile))
        return None
    log.info('Reading {}'.format(ifile))
    tb = pd.read_csv(ifile,sep=",") ##,encoding="ISO-8859-1")
    # get dates
    offset = dt.timedelta(minutes=time_offset)
    #local_time = [dt.datetime.strptime(i,"%Y-%m-%dT%H:%M:00.000Z")+offset for i in tb['Data']]
    local_time = [dt.datetime.strptime(i,"%Y/%m/%d %H:%M:00+00")+offset for i in tb['Data']]
    rio = timezone('America/Buenos_Aires')
    utc = pytz.utc
    dates = [rio.localize(i).astimezone(utc) for i in local_time]
    tb['ISO8601'] = dates
    tb['local_time'] = local_time 
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
    return df


def read_rio_bruno(iday,config,ifiles='unknown',time_offset=0):
    '''
    Reads and merges the AQ observations from Rio de Janeiro, as prepared by 
    Felipe Mandarino (felipe.mandarino@rio.rj.gov.br) / Bruno Franca.
    '''
    # get all files
    log = logging.getLogger(__name__)
    files = glob.glob(ifiles) 
    if len(files)==0:
        log.warning('No files found: {}'.format(ifile_template))
        return
    # read all files and write to data frame 
    df = pd.DataFrame()
    for ifile in files:
        idf = _read_file_bruno(ifile,config,time_offset)
        if idf.shape[0] > 0: 
            df = df.append(idf)
    # sort data
    df = df.sort_values(by="ISO8601")
    return df


def _read_file_bruno(ifile,config,time_offset):
    '''
    Read a single file as prepared by Bruno Franca'
    '''
    log = logging.getLogger(__name__)
    log.info('reading {}'.format(ifile))
    tb = pd.read_csv(ifile,sep=";",encoding="ISO-8859-1")
    nrow = tb.shape[0]
    # output dataframe 
    df = pd.DataFrame()
    # get dates
    offset = dt.timedelta(minutes=time_offset)
    local_time = [dt.datetime.strptime(i,"%d/%m/%Y %H:%M")+offset for i in tb['Data']]
    rio = timezone('America/Buenos_Aires')
    utc = pytz.utc
    dates = [rio.localize(i).astimezone(utc) for i in local_time]
    # get station information
    sname = tb[tb.keys()[-1]][0]
    locations = config.get('locations')
    if sname not in locations:
        log.warning('Location not found in config - skip: {}'.format(sname))
        return df 
    slat = locations.get(sname).get('lat')
    slon = locations.get(sname).get('lon')
    # read values and add to dataframe
    vars = config.get('vars')
    for v in vars:
        vname = vars.get(v).get('name_on_file')
        if vname not in tb:
            log.warning('Variable not found in data - cannot read it: {}'.format(vname))
            continue
        vunit = vars.get(v).get('unit')
        idf = pd.DataFrame()
        idf['ISO8601'] = dates
        idf['local_time'] = local_time 
        idf['original_station_name'] = [sname for i in range(nrow)]
        idf['lat'] = [slat for i in range(nrow)]
        idf['lon'] = [slon for i in range(nrow)]
        idf['obstype'] = [v for i in range(nrow)]
        idf['unit'] = [vunit for i in range(nrow)]
        idf['value'] = [np.float(str(i).replace(",",".")) for i in tb[vname].values]
        df = df.append(idf)
    return df

