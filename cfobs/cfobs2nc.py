#!/usr/bin/env python
# ****************************************************************************
# cfobs2nc.py 
#
# DESCRIPTION: 
# Map the tabled data to a netCDF file. 
# 
# HISTORY:
# 20200407 - christoph.a.keller at nasa.gov - Initial version
# ****************************************************************************
import numpy as np
import datetime as dt
import pandas as pd
import os
import glob
import xarray as xr
import yaml
import logging
from multiprocessing.pool import ThreadPool
import dask
import itertools

from .parse_string import parse_date 
from .systools import load_config


#ifile='/discover/nobackup/projects/gmao/geos_cf_dev/ana/openaq/data/Y2020/openaq_20200303.csv'
#datecols=['ISO8601','localtime']
#df_in = pd.read_csv(ifile,parse_dates=datecols,date_parser=lambda x: pd.datetime.strptime(x,'%Y-%m-%dT%H:%M:%SZ'))


def cfobs2nc(df_in,ofile,res='1x1',obstypes=['o3','no2','pm25'],cols=['conc_mod','conc_obs'],times=None,unitcol='conc_unit',obs_filter_low=None,obs_filter_high=0.99):
    '''
    Write the CFobs data to a (gridded) netCDF file. 

    Arguments
    ---------
    df_in: pandas.DataFrame
        cfobs data frame to be gridded and saved to netCDF. 
    ofile: str
        netcdf file name to write to. 
    res: str 
        resolution of output grid. Format: '<lat>x<lon>.
    obstypes: list str
        observation types to write out 
    cols: list of str
        data columns to write out 
    times: list of datetime.datetime 
        time stamps to write out. If missing, all time stamps on the data frame will be used
    unitcol: str 
        column used for unit (only used for netCDF attribute) 
    obs_filter_low: float
        percentile of (low) values to be omitted 
    obs_filter_high: float
        percentile of (high) values to be omitted 
    '''

    dask.config.set(pool=ThreadPool(10))
    log = logging.getLogger(__name__)
    # coordinates
    dlat = np.float(res.split('x')[0])
    dlon = np.float(res.split('x')[1])
    lat = np.arange(-90.0,90.0001,step=dlat)    
    lon = np.arange(-180.0,180.0001,step=dlon)
    times = times if times is not None else sorted(df_in['ISO8601'].unique())
    tmp = pd.DatetimeIndex(times)
    times = [dt.datetime(i.year,i.month,i.day,i.hour,i.minute,0) for i in tmp]
    nlat = len(lat)
    nlon = len(lon)
    ntimes = len(times) 
    # get index on output grid 
    df = df_in.copy()
    # create array for every variable
    output_vars = {}
    for obstype in obstypes:
        idf_orig = df.loc[df['obstype']==obstype].copy()
        # get lat and lon index on output grid
        lon_out_idx = [np.abs(lon-i).argmin() for i in idf_orig.lon.values]
        lat_out_idx = [np.abs(lat-i).argmin() for i in idf_orig.lat.values]
        idf_orig['out_latlon'] = [tuple((i,j)) for i,j in zip(lat_out_idx,lon_out_idx)]
        for col in cols:
            var = '_'.join([col,obstype])
            idf = idf_orig.copy()
            outarr = np.ones((ntimes,nlat,nlon))*np.nan
            # eventually filter obs for outliers
            if 'obs' in col and obs_filter_high is not None:
                q_hi  = idf[col].quantile(obs_filter_high)
                idf = idf.loc[idf[col] < q_hi].copy()
            if 'obs' in col and obs_filter_low is not None:
                q_low = idf[col].quantile(obs_filter_low)
                idf = idf.loc[idf[col]>q_low].copy() 
            # aggregate by time and output lat/lon
            idfgrp = idf.groupby(['ISO8601','out_latlon']).mean().reset_index()
            for n,t in enumerate(times):
                latlon = idfgrp.loc[idfgrp['ISO8601']==t,'out_latlon'].values
                latidx = [i[0] for i in latlon]
                lonidx = [i[1] for i in latlon]
                idx = tuple((np.array(latidx),np.array(lonidx)))
                vals = idfgrp.loc[idfgrp['ISO8601']==t,col].values
                tmparr = np.ones((nlat,nlon))*np.nan
                if len(idx[0])>0: 
                    tmparr[idx] = vals
                    outarr[n,:,:] = tmparr
            # Create data array and add to dictionary
            idarr = xr.DataArray(data=outarr, dims=["time","lat","lon"], coords=[times,lat,lon])
            idarr.attrs['units'] = idf[unitcol].values[0]
            output_vars[var] = idarr 
    # Create dataset and write out
    da = xr.Dataset(data_vars=output_vars)
    da.lon.attrs['standard_name'] = 'longitude' 
    da.lon.attrs['units'] = 'degrees_east' 
    da.lat.attrs['standard_name'] = 'latitude' 
    da.lon.attrs['units'] = 'degrees_north' 
    da.to_netcdf(ofile)
    log.info('Data written to {}'.format(ofile))
    return

