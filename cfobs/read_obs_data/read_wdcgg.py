#!/usr/bin/env python
# ****************************************************************************
# read_wdcgg.py 
#
# DESCRIPTION: 
# Read observations downloaded from the World Data Centre for Greenhouse Gases
# (WDCGG) 
#
# DATA SOURCE:
# https://gaw.kishou.go.jp/
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


def read_wdcgg(iday=None,ifiles='unknown',**kwargs):
    '''
    Read observations downloaded from the World Data Centre for 
    Greenhouse Gases (https://gaw.kishou.go.jp/)
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


def _read_single_file(ifile,firstday=None,lastday=None):
    '''Read a single WDCGG file.'''
    log = logging.getLogger(__name__)
    log.info('Reading {}'.format(ifile))
    with open(ifile) as f:
        lines = f.readlines()
    nhdr,para,unit,name,lat,lon = _read_header(lines)
    dates,values = _read_data(lines,nhdr)
    # eventually convert mg/m3 to ug/m3
    if 'mg/m3' in unit:
        unit = 'ug/m3'
        values = [i*1000.0 for i in values]
    # construct array
    nrow = len(dates)
    idf = pd.DataFrame()
    idf['ISO8601'] = dates
    idf['original_station_name'] = [name for i in range(nrow)]
    idf['lat'] = [lat for i in range(nrow)]
    idf['lon'] = [lon for i in range(nrow)]
    idf['obstype'] = [para for i in range(nrow)]
    idf['unit'] = [unit for i in range(nrow)]
    idf['value'] = values
    # Eventually reduce to specified time range
    if firstday is not None:
        log.debug('Only use data after {}'.format(firstday))
        idf = idf.loc[idf['ISO8601'] >= firstday]
    if lastday is not None:
        log.debug('Only use data before {}'.format(lastday))
        idf = idf.loc[idf['ISO8601'] < lastday]
    return idf


def _read_header(lines):
    '''Read header and parse metadata'''
    log = logging.getLogger(__name__)
    parameter='unknown'
    unit='unknown'
    location_name='unknown'
    location_lon=np.nan
    location_lat=np.nan
    header_lines = np.int(_parse_line(lines[0]))
    for i,iline in enumerate(lines):
        if i==header_lines:
            break
        if 'dataset_parameter :' in iline:
            parameter = _parse_line(iline)
        if 'site_name :' in iline:
            location_name = _parse_line(iline)
        if 'site_latitude :' in iline:
            location_lat = np.float(_parse_line(iline))
        if 'site_longitude :' in iline:
            location_lon = np.float(_parse_line(iline))
        if 'site_longitude :' in iline:
            location_lon = np.float(_parse_line(iline))
        if 'value:units :' in iline:
            unit = _parse_line(iline,elem=2)
    return header_lines,parameter,unit,location_name,location_lat,location_lon


def _read_data(lines,nhdr):
    '''Read observation data and corresponding dates'''
    log = logging.getLogger(__name__)
    hdr = lines[nhdr-1].replace('\n','').replace('# ','').split()
    cyr = hdr.index('year')
    cmt = hdr.index('month')
    cdy = hdr.index('day')
    chr = hdr.index('hour')
    cmn = hdr.index('minute')
    cvl = hdr.index('value')
    cqc = hdr.index('QCflag')
    dates = []
    values = []
    for iline in lines[nhdr:]:
        ivals = iline.replace('\n','').split()
        # Check for quality flag - only accept quality flags 1 and 2
        qc =  np.int(ivals[cqc])
        if qc < 1 or qc > 2:
            continue
        idate = dt.datetime(np.int(ivals[cyr]),np.int(ivals[cmt]),np.int(ivals[cdy]),np.int(ivals[chr]),np.int(ivals[cmn]),0)
        values.append(np.float(ivals[cvl]))
        dates.append(idate)
    i = len(dates)
    j = len(lines)-nhdr
    log.info('Read {} valid entries from {} total entries ({}%)'.format(i,j,np.float(i)/np.float(j)*100.0))
    return dates,values


def _parse_line(iline,elem=1):
    return iline.replace('\n','').split(':')[elem].replace(' ','')
