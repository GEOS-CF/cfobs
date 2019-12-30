#!/usr/bin/env python
# ****************************************************************************
# cfobs_load.py 
# 
# DESCRIPTION:
# Load cfobs data from a csv file, previously saved using cfobs_save.

# HISTORY:
# 201901220- christoph.a.keller at nasa.gov - Initial version 
# ****************************************************************************
import os
import numpy as np
import datetime as dt
import pandas as pd
from pandas.api.types import is_numeric_dtype, is_string_dtype

from .parse_string import parse_date 


def load(file_template,startday,endday=None,read_freq='1D',verbose=1,file_not_found_ok=False,**kwargs):
    '''
    Load data from file for the time period from startday to endday.
    '''
    if endday is None:
        endday = startday
    timesteps = pd.date_range(start=startday,end=endday,freq=read_freq).tolist()
    dat = pd.DataFrame()
    for idatetime in timesteps:
        ifile = parse_date(file_template,idatetime)
        if not os.path.isfile(ifile):
            if file_not_found_ok:
                if verbose>0:
                    print("Warning: file not found: {}".format(ifile))
                continue
            else:
                print("Error: file not found: {}".format(ifile))
                raise
        idat = _load_single_file(ifile,verbose=verbose,**kwargs)
        dat  = dat.append(idat)
    return dat


def _load_single_file(ifile,verbose=1,to_float=False,round_minutes=False):
    '''
    Load data from single file. 
    '''
    if verbose>0:
        print('Loading {}'.format(ifile)) 
    # determine date columns
    datecols = ['ISO8601']
    file_hdr = pd.read_csv(ifile,nrows=1)
    if 'localtime' in file_hdr:
        datecols.append('localtime')
    dat = pd.read_csv(ifile,parse_dates=datecols,date_parser=lambda x: pd.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ'))
    if round_minutes:
        dat['ISO8601'] = [dt.datetime(i.year,i.month,i.day,i.hour,0,0) for i in dat['ISO8601']]
    # backward compatibility:
    if 'Location' in dat.keys():
        dat = dat.rename(columns={"Location": "location"})
    if 'station' in dat.keys():
        dat = dat.rename(columns={"station": "location"})
    if 'Value' in dat.keys():
        dat = dat.rename(columns={"Value": "value"})
    if 'Lat' in dat.keys():
        dat = dat.rename(columns={"Lat": "lat"})
    if 'Lon' in dat.keys():
        dat = dat.rename(columns={"Lon": "lon"})
    # everything should be numeric except for a few fields
    if to_float:
        for k in list(dat.keys()):
            if k in ['ISO8601','unit','obstype','location']:
                continue 
            if not is_numeric_dtype(dat[k]):
                if verbose > 1:
                    print("Converting to float: "+k)
                dat[k] = dat[k].astype(float)
    return dat
