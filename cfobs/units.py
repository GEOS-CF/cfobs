#!/usr/bin/env python
# ****************************************************************************
# units.py 
#
# DESCRIPTION: 
# Collection of routines to handle units. 
#
# HISTORY:
# 20191211 - christoph.a.keller at nasa.gov - Initial version
# ****************************************************************************
import numpy as np
import pandas as pd

# constants
RCONST  = 8.3145 # gas constant, J K−1 mol−1 
PPM2PPB = 1.0e+3 # conversion factor from ppmv to ppbv, unitless


def get_conv_ugm3_to_ppbv(dat,temperature_name='t10m',temperature_scal=1.0,pressure_name='ps',pressure_scal=1.0,mw=1.0):
    '''
    Returns the conversion factor from ug/m3 to ppbv.
    Expects as inputs the temperature in kelvin and
    pressure in Pa. Scale factors can be provided to 
    convert fields accordingly.
    '''
    if temperature_name not in dat or pressure_name not in dat:
        return None
    else:
        return dat[temperature_name]*temperature_scal*RCONST*1.0e-6/(dat[pressure_name]*pressure_scal*mw)*1.0e9


def to_ppbv(df,conv_ugm3_to_ppbv=None,convscal=1.0,temp=None,press=None,mw=1.0,
            idx=None,colname='conc_obs',unitcol='unit',loncol='lon',latcol='lat'):
    '''
    Converts the values in column `colname` of the data frame `df` to ppbv. The input
    units of the values must be given in a separate column, as specified in `unitcol`.
    The input array `conv` contains the conversion factor from ugm-3 to ppbv. This 
    value will be multiplied by the value provided in `mwscal`. This can be useful to 
    convert multiple species (serially) using the same conv array (produced using mw=1.0) 
    and then using mwscal to account for the individual MWs (the MW needs to be inverted 
    to account for the fact that mw is in the denominator.
    This function is designed to work on a subset of the df data frame, as given by the
    index `idx`. 
    `lons` and `lats` are the longitude and latitude arrays belonging to `conv`. `loncol`
    and `latcol` are the column names of the longitude and latitude values in df, 
    respectively.
    '''
    # check input 
    if idx is None:
        idx = range(df.shape[0])

#---ugm-3 to ppbv
    iidx = df.loc[idx].index[df.loc[idx,unitcol]=='ugm-3']
    if len(iidx>0):
        # conversion factor
        if conv_ugm3_to_ppbv is None:
            assert(temp is not None and press is not None), 'Cannot convert ugm-3 to ppbv - met information is missing'
            conv_ugm3_to_ppbv = get_conv_ugm3_to_ppbv(temperature,pressure,mw)
        lons = conv_ugm3_to_ppbv.lon.values
        lats = conv_ugm3_to_ppbv.lat.values
        lonidx = [np.abs(lons-i).argmin() for i in df.loc[iidx,loncol].values]
        latidx = [np.abs(lats-i).argmin() for i in df.loc[iidx,latcol].values]
        df.loc[iidx,colname] = df.loc[iidx,colname]*conv_ugm3_to_ppbv.values[latidx,lonidx]*convscal

#---ppmv to ppbv 
    iidx = df.loc[idx].index[(df.loc[idx,unitcol]=='ppmv') | (df.loc[idx,unitcol]=='ppm')]
    if len(iidx>0):
        df.loc[iidx,colname] = df.loc[iidx,colname]*PPM2PPB
    return df
