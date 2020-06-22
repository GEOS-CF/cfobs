#!/usr/bin/env python
# ****************************************************************************
# read_eea.py 
#
# DESCRIPTION: 
# Reads AQ observation data from the European Environmental Agency (EEA) as
# obtained from https://discomap.eea.europa.eu/map/fme/AirQualityExport.htm 
#
# DATA SOURCE: EEA, https://discomap.eea.europa.eu/map/fme/AirQualityExport.htm
#
# HISTORY:
# 20200524 - christoph.a.keller at nasa.gov - Initial version 
# ****************************************************************************


# requirements
import logging
import os 
import argparse
import numpy as np
import datetime as dt
import pandas as pd
import pytz
import yaml
import glob
from dateutil import parser

from ..cfobs_save import save as cfobs_save


def read_eea(iday=None,idir=None,metafile=None,specs=['o3','no2','pm25'],stationsfile_local=None,ofile_local=None,ofile_local_append=True,**kwargs):
    '''
    Read AQ observations from the EEA.
    '''
    log = logging.getLogger(__name__)
    if idir is None or metafile is None:
        log.warning('Must specify idir and metafile - return')
        return None
    # read metadata file
    meta = _read_meta(metafile)
    # dictionary of stations if specified so
    stations = None
    if stationsfile_local is not None:
        if os.path.isfile(stationsfile_local):
            with open(stationsfile_local,'r') as f:
                stations = yaml.load(f, Loader=yaml.FullLoader)
        else:
            stations = {}
    # read data for each species
    dfs = []
    for s in specs:
        ifiles = glob.glob(idir.replace('%s',s))
        for ifile in ifiles:
            idf,locname,stations = _read_file(ifile,meta,stations,**kwargs)
            if idf is None:
                continue
            # add to list of dataframes
            dfs.append(idf)
            # write to individual files if specied so
            if ofile_local is not None:
                ofile = ofile_local.replace('%l',locname)
                _ = cfobs_save(idf,ofile,iday,append=ofile_local_append)
    # write out stations if specified so
    if stationsfile_local is not None:
        with open(stationsfile_local,'w') as file:
            yaml.dump(stations, file)
        log.info('Written YAML file: {}'.format(stationsfile_local))
    # merge all data into one
    df = pd.concat(dfs,ignore_index=True) if len(dfs)>0 else None 
    return df


def _read_file(ifile,meta,stations,time_offset=0,firstday=None,lastday=None):
    log = logging.getLogger(__name__)
    locname = 'unknown'
    if not os.path.exists(ifile):
        log.warning('file not found: {}'.format(ifile))
        return None,locname,stations
    log.info('Reading {}'.format(ifile))
    origtb = pd.read_csv(ifile,sep=",") ##,encoding="ISO-8859-1")
    # data should be for one station only:
    all_stations = list(origtb.SamplingPoint.unique())
    if len(all_stations)>1:
        a = origtb.groupby('SamplingPoint').count().reset_index()
        istation = a.loc[a['Concentration']==a['Concentration'].values.max(),'SamplingPoint'].values[0]
        tbnew = origtb.loc[origtb['SamplingPoint']==istation].copy()
        origtb = tbnew.copy()
        log.warning('More than one station found in file {} - will only read data for location with most values ({})'.format(ifile,istation))
    # only read valid entries
    tb = origtb.loc[~np.isnan(origtb['Concentration'].values)].copy()
    del(origtb)
    if tb.shape[0]==0:
        log.warning('No valid concentration entries found in {}'.format(ifile))
        return None,locname,stations
    # get meta data for that station
    thisstation = tb.SamplingPoint.unique()[0]
    istat = meta.loc[meta['SamplingPoint']==thisstation]
    if istat.shape[0]==0:
        log.warning('No meta data found for sampling point: {} - cannot read data')
        return None,locname,stations
    locname = str(istat['AirQualityStation'].values[0].replace('STA.','').replace('STA-','').replace('STA_',''))
    loclat  = np.round(np.float(istat['Latitude'].values[0]),4)
    loclon  = np.round(np.float(istat['Longitude'].values[0]),4)
    loctype = istat['AirQualityStationType'].values[0]
    loccountry = istat['Countrycode'].values[0]
    locname = '_'.join(('EEA',loccountry,locname))
    # read dates
    offset = dt.timedelta(minutes=time_offset)
    utc = pytz.utc
    df = pd.DataFrame()
    df['localtime'] = [parser.parse(i) for i in tb['DatetimeBegin']]
    df['ISO8601'] = [i.astimezone(utc)+offset for i in df['localtime']]
    # add station information
    df['original_station_name'] = [locname for i in range(df.shape[0])]
    df['lat'] = [loclat for i in range(df.shape[0])]
    df['lon']= [loclon for i in range(df.shape[0])]
    df['country']= [loccountry for i in range(df.shape[0])]
    df['station_type']= [loctype for i in range(df.shape[0])]
    # add observations
    df['obstype'] = tb['AirPollutant'].values
    df['unit'] = tb['UnitOfMeasurement'].values
    df['value'] = tb['Concentration'].values
    # replace with 'standard' values
    df.loc[df['unit']==u'\u00b5g/m3','unit'] = 'ugm-3'
    df.loc[df['obstype']=='NO2','obstype'] = 'no2'
    df.loc[df['obstype']=='O3','obstype'] = 'o3'
    df.loc[df['obstype']=='PM2.5','obstype'] = 'pm25'
    # filter by days
    if firstday is not None:
        firstday_tzaware = dt.datetime(firstday.year,firstday.month,firstday.day,tzinfo=pytz.utc)
        log.info('Only use data after {}'.format(firstday_tzaware))
        df = df.loc[df['ISO8601'] >= firstday_tzaware]
    if lastday is not None:
        lastday_tzaware = dt.datetime(lastday.year,lastday.month,lastday.day,tzinfo=pytz.utc)
        log.info('Only use data before {}'.format(lastday_tzaware))
        df = df.loc[df['ISO8601'] < lastday_tzaware]
    # sort data
    df = df.sort_values(by="ISO8601")
    # eventually update stations entry
    if stations is not None:
        if locname not in stations:
            stations[locname] = {'country':loccountry,'lat':'{:.4f}'.format(loclat),'lon':'{:.4f}'.format(loclon)}
    return df,locname,stations


def _read_meta(metafile):
    '''Read meta data'''
    log = logging.getLogger(__name__)
    log.info('Reading metadata from {}'.format(metafile))
    meta = pd.read_csv(metafile,sep='\t')
    return meta
