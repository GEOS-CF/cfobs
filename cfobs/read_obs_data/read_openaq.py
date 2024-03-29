#!/usr/bin/env python
# ****************************************************************************
# read_openaq.py 
#
# DESCRIPTION: 
# Reads OpenAQ data (https://openaq.org) and converts it to a csv table.
# For now, it assumes that the daily OpenAQ data is locally available, as
# e.g. downloaded from AWS:
# aws s3 ls "openaq-fetches/realtime/${Y}-${M}-${D}/" --no-sign-request | awk '{print $4}' | xargs -I'{}' aws s3 cp "s3://openaq-fetches/realtime/${Y}-${M}-${D}/{}" - --no-sign-request >> $ofile 
# 
# NOTES:
# - To read data back in, use:
#   df = pd.read_csv(filename,parse_dates=['ISO8601'],date_parser=lambda x: pd.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ'))
#
# HISTORY:
# 20191115 - christoph.a.keller at nasa.gov - Adapted from older code 
# ****************************************************************************

import os
import numpy as np
import datetime as dt
import pandas as pd
import json
import logging

from ..parse_string import parse_date 


def read_openaq(iday,json_tmpl=None,csv_tmpl=None):
    '''
    Reads the native OpenAQ data file for the given day and returns a cfobs-compatible data frame. 
    '''

    log = logging.getLogger(__name__)
    # try to read json file first 
    df = None
    nlines = 0
    nerrs  = 0
    jsonfile = parse_date(json_tmpl,iday)
    if os.path.isfile(jsonfile): 
        df,nlines,nerrs = read_openaq_ndjson(jsonfile)
    # if not successful, try to read csv file instead 
    if nlines==0:
        assert(csv_tmpl is not None), 'Error: json file could not be read and csv template is not provided!'
        csvfile = parse_date(csv_tmpl,iday)
        if os.path.isfile(csvfile):
            df,nlines,nerrs = read_openaq_csv(csvfile)
    log.info('Read: {:,}, thereof tossed: {:,}'.format(nlines,nerrs))
    return df


def getv(j,name,rc,inst=None):
    """
    Wrapper for reading json variable.
    """
    val = j.get(name,None)
    if inst is not None:
        if not isinstance(val,inst):
            val = None
    if val is None:
        rc += 1
    return val,rc


def get_unit(orig_unit):
    '''
    Helper routine to parse the original unit string.
    '''
    #ascii = orig_unit.encode("ascii","ignore")
    ascii = orig_unit
    if 'g/m' in ascii:
        unit = 'ugm-3'
    if 'ppb' in ascii:
        unit = 'ppbv'
    if 'ppm' in ascii:
        unit = 'ppmv'
    return unit


def read_json_line(line,dct):
    '''
    Helper routine to read a line from the OpenAQ ndjson file. 
    '''
    err = 0
    # Read line
    try:
        j = json.loads(line)
    except:
        err = 1 
    # Get all values
    rc = 0
    if err==0:
        loc,rc = getv(j,"location",rc,type(u""))
        ctr,rc = getv(j,"country",rc,type(u""))
        par,rc = getv(j,"parameter",rc,type(u""))
        unt,rc = getv(j,"unit",rc,type(u""))
        val,rc = getv(j,"value",rc)
        val    = np.float(val) if val is not None else None
        dat,rc = getv(j,"date",rc)
        if rc==0:
            utc,rc = getv(dat,"utc",rc)
            if rc==0:
                try:
                    utc_date = dt.datetime.strptime(utc,'%Y-%m-%dT%H:%M:%S.000Z')
                except:
                    utc_date = None
                    rc += 1
            lcl,rc = getv(dat,"local",rc)
            if rc==0:
                try:
                    lcl_date = dt.datetime.strptime(lcl[0:19],'%Y-%m-%dT%H:%M:%S')
                except:
                    lcl_date = None
                    rc += 1
        cor,rc = getv(j,"coordinates",rc)
        if rc==0:
            lat,rc = getv(cor,"latitude",rc)
            lon,rc = getv(cor,"longitude",rc)
        if rc==0:
            lerr = 0
            attr, lerr = getv(j,"attribution",lerr)
            if lerr==0:
                attr = attr[0] if type(attr)==type([]) else attr
                src,lerr = getv(attr,'name',lerr,type(u""))
            if lerr != 0:
                src = 'unknown'
            src = ': '.join(['OpenAQ ndjson',src])
    if rc>0:
        err = 1
    # don't allow negative values:
    if err==0 and val < 0.0:
        err = 1
    # Populate dataframe 
    if err==0:
        dct['ISO8601'].append(utc_date)
        dct['localtime'].append(lcl_date)
        dct['original_station_name'].append(loc)
        dct['country'].append(ctr)
        dct['lat'].append(np.float(lat))
        dct['lon'].append(np.float(lon))
        dct['obstype'].append(par)
        dct['unit'].append(get_unit(unt))
        dct['value'].append(val)
        dct['source'].append(src)
    else:
        df = None
    # All done
    return dct,err


def read_openaq_ndjson(ifile):
    '''
    Reads an OpenAQ ndjson file and writes its content to a data frame.
    '''

    log = logging.getLogger(__name__)
    # read data into dictionary
    dct = dict({'ISO8601':[],
                'localtime':[],
                'original_station_name':[],
                'country':[],
                'lat':[],
                'lon':[],
                'obstype':[],
                'unit':[],
                'value':[], 
                'source':[], 
                })
    nline = 0
    nerr  = 0
    log.info('reading '+ifile)
    with open(ifile,"r",encoding="ISO-8859-1") as f:
        for line in f:
            nline += 1
            dct,err = read_json_line(line,dct)
            nerr += err 
            log.debug('tossed: '+line)
    # pass to dataframe
    df = pd.DataFrame()
    if len(dct['ISO8601'])>0:
        for k,v in zip(dct.keys(),dct.values()):
            df[k] = v
        # sort data
        df = df.sort_values(by="ISO8601")
        # strip empty spaces
        df['original_station_name'] = [i.replace(" ","") for i in df['original_station_name']]
    return df,nline,nerr


def read_openaq_csv(ifile):
    '''
    Reads an OpenAQ csv file and writes its content to a data frame.
    '''
    log = logging.getLogger(__name__)
    log.info('reading {}'.format(ifile))
    ds = pd.read_csv(ifile,sep=",")
    # pass to dataframe
    df = pd.DataFrame()
    df['ISO8601']   = [dt.datetime.strptime(i,'%Y-%m-%dT%H:%M:%S.000Z') for i in ds['utc']]
    df['localtime'] = [dt.datetime.strptime(i[0:19],'%Y-%m-%dT%H:%M:%S') for i in ds['local']]
    df['original_station_name'] = ds['location'] 
    df['country']   = ds['country']
    df['lat']       = [np.float(i) for i in ds['latitude']]
    df['lon']       = [np.float(i) for i in ds['longitude']]
    df['obstype']   = ds['parameter']
    df['unit']      = [get_unit(i) for i in ds['unit']]
    df['value']     = [np.float(i) for i in ds['value']]
    df['source']    = ['OpenAQ csv: '+i.split('"name":')[1].split(',')[0] if type(i)==type('') else 'OpenAQ csv' for i in ds['attribution']]
    # cleanup
    nline = df.shape[0]
    nerr  = 0
    if nline>0:
        # sort data
        df = df.sort_values(by="ISO8601")
        # strip empty spaces
        df['original_station_name'] = [i.replace(" ","") for i in df['original_station_name']]
    return df,nline,nerr

