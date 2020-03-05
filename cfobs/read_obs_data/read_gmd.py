#!/usr/bin/env python
# ****************************************************************************
# read_gmd.py 
#
# DESCRIPTION: 
# Read observations downloaded from the NOAA Global Monitoring Division (GMD) 
#
# DATA SOURCE:
# wget ftp://ftp.cmdl.noaa.gov/data/trace_gases/co/flask/co_flask_surface_2019-08-29.zip
#
# HISTORY:
# 20200305 - christoph.a.keller at nasa.gov - Initial version 
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


def read_gmd_co(iday=None,ifiles='unknown',**kwargs):
    '''
    Read GMD CO observations. 
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
    '''Read a single GMD file.'''
    log = logging.getLogger(__name__)
    log.info('Reading {}'.format(ifile))
    with open(ifile) as f:
        lines = f.readlines()
    nhdr,para,unit = _read_header(lines)
    dates,values,names,lats,lons = _read_data(lines,nhdr)
    # construct array
    nrow = len(dates)
    idf = pd.DataFrame()
    idf['ISO8601'] = dates
    idf['original_station_name'] = names
    idf['lat'] = lats 
    idf['lon'] = lons 
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
    parameter='co'
    unit='ppbv'
    header_lines = np.int(_parse_line(lines[0]))
    return header_lines,parameter,unit


def _read_data(lines,nhdr):
    '''Read observation data and corresponding dates'''
    log = logging.getLogger(__name__)
    hdr = lines[nhdr-1].replace('\n','').replace('# data_fields: ','').split()
    cyr = hdr.index('sample_year')
    cmt = hdr.index('sample_month')
    cdy = hdr.index('sample_day')
    chr = hdr.index('sample_hour')
    cmn = hdr.index('sample_minute')
    cvl = hdr.index('analysis_value')
    cqc = hdr.index('analysis_flag')
    cnm = hdr.index('sample_site_code')
    clt = hdr.index('sample_latitude')
    cln = hdr.index('sample_longitude')
    dates = []
    values = []
    names = []
    lats = []
    lons = []
    for iline in lines[nhdr:]:
        ivals = iline.replace('\n','').split()
        # Check for quality flag - only accept quality flags that begin with '..'
        qc =  ivals[cqc]
        if qc[0:2] != '..':
            continue
        idate = dt.datetime(np.int(ivals[cyr]),np.int(ivals[cmt]),np.int(ivals[cdy]),np.int(ivals[chr]),np.int(ivals[cmn]),0)
        dates.append(idate)
        values.append(np.float(ivals[cvl]))
        names.append(ivals[cnm])
        lats.append(np.float(ivals[clt]))
        lons.append(np.float(ivals[cln]))
    i = len(dates)
    j = len(lines)-nhdr
    log.info('Read {} valid entries from {} total entries ({}%)'.format(i,j,np.float(i)/np.float(j)*100.0))
    return dates,values,names,lats,lons


def _parse_line(iline,elem=1):
    return iline.replace('\n','').split(':')[elem].replace(' ','')
