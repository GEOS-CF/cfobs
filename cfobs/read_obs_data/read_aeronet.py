#!/usr/bin/env python
# ****************************************************************************
# read_aeronet.py 
#
# DESCRIPTION: 
# Read the original aeronet data file into a Pandas DataFrame.
#
# HISTORY:
# 20191112 - christoph.a.keller at nasa.gov - Initial version 
# ****************************************************************************

import glob
import numpy as np
import datetime as dt
import pandas as pd
from dateutil import relativedelta
import requests
from tqdm import tqdm
import logging

from ..parse_string import parse_date 


# Parameter for remote data reading
LOCALFILE_SKIPROWS = 6
AERONET_WEBSERVICE = 'https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3'
NON_FLOAT_COLUMNS  = ['AERONET_Site','Date(dd:mm:yyyy)','Time(hh:mm:ss)','AERONET_Site_Name','Data_Quality_Level']


def read_aeronet(start,end=None,localfiles=None,**kwargs):
    '''
    Read AERONET data and calculate the AOD value at a given wavelength at each 
    station based on the available values. Simple interpolation is performed 
    between the closest available wavelengths.
    This routine accepts both reading local AERONET files and reading AERONET 
    data remotely using the AERONET web services tool. If reading locally, the
    AERONET files must be in the natively provided data format, e.g. when
    downloading all available daily averages:
    https://aeronet.gsfc.nasa.gov/data_push/V3/AOD/AOD_Level20_Daily_V3.tar.gz
    
    The obstype of the returned data frame is 'aod'<wl>, where wl is the AOD 
    wavelength in nm.
    '''

    log = logging.getLogger(__name__)
    if localfiles is not None:
        df = read_aeronet_locally(localfiles,start,end,**kwargs)
    else:
        end = end if end is not None else start + relativedelta.relativedelta(months=1)
        df = read_aeronet_remote(start,end,**kwargs)
    # sort data and strip empty spaces
    df = df.sort_values(by="ISO8601")
    df['original_station_name'] = [i.replace(" ","") for i in df['original_station_name']]
    df['source'] = ['Aeronet' for i in df.shape[0]]
    log.info('Read {:,} Aeronet observations'.format(df.shape[0]))
    return df


def read_aeronet_locally(localfiles,start,end,show_progress=True,**kwargs):
    '''Read Aeronet data from local files'''

    log = logging.getLogger(__name__)
    files = glob.glob(parse_date(localfiles,start))
    if len(files)==0:
        log.warning('No files found in '+args.idir)
        return
    # output dataframe
    df = pd.DataFrame()
    # read data station by station, merge into main dataframe
    for ifile in tqdm(files,disable=(not show_progress)):
        log.info('reading {}'.format(file))
        tb = pd.read_csv(file,sep=",",skiprows=LOCALFILE_SKIPROWS)
        idf = read_aeronet_table(tb,start,end,**kwargs)
        if idf.shape[0] > 0:
            df = df.append(idf)
    return df


def read_aeronet_remote(start,end,show_progress=True,data_type='AOD20',AVG=20,**kwargs):
    '''Read aeronet data remotely using the web data download tool'''

    
    log = logging.getLogger(__name__)
    url = AERONET_WEBSERVICE+start.strftime('?year=%Y&month=%-m&day=%-d')+end.strftime('&year2=%Y&month2=%-m&day2=%-d')+'&'+data_type+'=1&AVG='+str(AVG)
    # wget --no-check-certificate -q -O tmp.csv <url>
    log.info('Reading AERONET data remotely from {}'.format(url))
    r = requests.get(url)
    assert(r.status_code == requests.codes.ok), 'Error reading AERONET data from {}'.format(url)
    lines = r.content.decode("utf8").split('\n')
    tb = pd.DataFrame()
    header = lines[7].replace('<br>','').split(',')
    for l in tqdm(lines[8:],disable=(not show_progress)):
        if '</body></html>' in l:
            break
        tb = tb.append(pd.DataFrame([l.replace('<br>','').split(',')],columns=header))
    for k in tb.columns:
        if k in NON_FLOAT_COLUMNS:
            continue 
        tb[k] = tb[k].astype(float)
    df = read_aeronet_table(tb,start,end,**kwargs)
    return df


def read_aeronet_table(tb,start,end,wavelength=550,remove_nan=1,approximate_wavelength=1,wl_interpolation_method=2):
    '''Read an aeronet file and creates a Pandas dataframe containing it's values'''

    df = pd.DataFrame()
    # get dates & times (UTC)
    df['ISO8601'] = [dt.datetime.strptime(' '.join([i,j]),"%d:%m:%Y %H:%M:%S") for i,j in zip(tb['Date(dd:mm:yyyy)'],tb['Time(hh:mm:ss)'])]
    # add station information
    df['original_station_name'] = tb['AERONET_Site_Name'].values
    df['lat'] = tb['Site_Latitude(Degrees)'].values
    df['lon'] = tb['Site_Longitude(Degrees)'].values
    df['elev'] = tb['Site_Elevation(m)'].values
    # add observations
    nrow = tb.shape[0]
    varname       = 'aod'+str(wavelength)
    df['obstype'] = [varname for x in range(nrow)]
    df['unit']    = ['unitless' for x in range(nrow)]
    df['value']   = get_aod(tb,wavelength,approximate_wavelength,wl_interpolation_method)
    # set missing to NaN
    df.loc[df.value<0.0,'value'] = np.nan
    # eventually remove all NaN values
    if remove_nan == 1:
        df = df[~np.isnan(df.value)]
    # eventually slice for min/max date 
    if start is not None:
        df = df[df['ISO8601']>=start]
    if end is not None:
        df = df[df['ISO8601']<end]
    return df


def is_nan(x):
    return np.isnan(x), np.sum(~np.isnan(x))


def get_aod(tb,wavelength,approximate_wavelength,wl_interpolation_method):
    '''Return the AOD values for a given wavelength.'''
    # default output
    vals = np.array([np.nan for i in range(tb.shape[0])])
    # target wavelength
    wl = wavelength
    # return exact values
    if approximate_wavelength==0:
        varname = 'AOD_'+str(wl)+'nm'
        if varname in tb.keys():
            vals = tb[varname].values
    # try to interpolate between nearest values
    else:
        # get all wavelengths
        wls = sorted([ int(k.split('_')[1].replace('nm','')) for k in tb.keys() if k[0:4]=='AOD_' and 'nm' in k ])
        df = pd.DataFrame()
        for w in wls:
            df[w] = tb['AOD_'+str(w)+'nm'].values
            df[df<0.0] = np.nan
        x = df.keys().values
        # get interpolated value for each date
        for i in range(df.shape[0]):
            y = df.iloc[i].values
            nans,nvalid = is_nan(y)
            # can't interpolate if less than two values
            if nvalid<2:
                continue
            # method 1: use all values
            if wl_interpolation_method==1:
                ival = np.interp(wl,x[~nans],y[~nans])
            # method 2: only use two closest values 
            if wl_interpolation_method==2:
                xvalid = x[~nans]
                yvalid = y[~nans]
                diffs = np.abs(xvalid-wl).argsort()
                vals[i] = np.interp(wl,xvalid[diffs<2],yvalid[diffs<2])
    return vals
