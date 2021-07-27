#!/usr/bin/env python
# ****************************************************************************
# read_cf.py 
#
# DESCRIPTION: 
# Handles reading of CF data. 
# 
# HISTORY:
# 20191211 - christoph.a.keller at nasa.gov - Initial version
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

from .parse_string import parse_date 
from .systools import load_config

# Parameter
AVG_LABEL_BEFORE = 'average_offset_before'
AVG_LABEL_AFTER  = 'average_offset_after'
HR_TO_NS = 60*60*10**9

def read_cf_data_2d(idate,config=None,config_file=None,suppress_messages=False):
    '''
    Reads the CF output for the given datetime and returns all variables as specified
    in the configuration file.
    The configuration file must list all collections and variables therein to be read
    by this routine. For each collection, the file template and the list of variables
    to be read must be given, e.g.:

    met:
      template: '/path/to/file/filename.%Y%m%d_%H.nc4' 
      vars:
        var1:
          name_on_file: Variable1
        var2:
          name_on_file: Variable2
          scal: 100.0 

    The template name can contain an asterisk symbol, in which case multiple files 
    are read.
    Similarly, OpenDAP addresses are also supported, e.g.: 

    chem:
      template: 'https://opendap.nccs.nasa.gov/dods/gmao/geos-cf/assim/chm_tavg_1hr_g1440x721_v1'
      vars:
        o3:
          name_on_file: 'o3'
          scal: 1.0e+9
        no2:
          name_on_file: 'no2'
          scal: 1.0e+9

    Default behavior is to pick the closest available date on the file(s) (relative to 
    input argument idate). It is possible to return time averaged values by specifying
    a time averaging window (in hours) using keywords 'average_offset_before' and 
    'average_offset_after', respectively. For instance, assume that hourly netCDF
    files are available locally and one wants to obtain the daily averaged fields:

    met:
      template: '/path/to/file/filename.%Y%m%d_*.nc4' 
      average_offset_before: 12
      average_offset_after: 12
      vars:
        var1:
          name_on_file: Variable1
        var2:
          name_on_file: Variable2
          scal: 100.0 

    When calling this routine with idate set to Dec 1, 2019 at 12z, the routine
    will read all hourly files for that day and then average from idate-12 hours to
    idate+12 hours. 

    This routine returns a dictionary with the 2d arrays of all variables.
    '''
    dask.config.set(pool=ThreadPool(10))
    log = logging.getLogger(__name__)
    # Configuration
    if config is None:
        config = load_config(config_file)
    # Default output
    dat = {}
    rc  = 0
    # Make sure date is of correct type
    ts = (idate - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's')
    tmpdate = dt.datetime.utcfromtimestamp(ts)
    # loop over all collections specified in the configuration file 
    for collection in config:
        icol = config.get(collection)
        if 'template' not in icol:
            log.warning('`template` not defined for collection {} - will skip'.format(collection))
            continue
        if 'vars' not in icol:
            log.warning('No variable read from collection {} because `vars` key is missing'.format(collection))
            continue
        # Open collection
        ifile = parse_date(icol.get('template'),tmpdate)
        if not suppress_messages: 
            log.info('Reading {}'.format(ifile))
        if '*' in ifile:
            try:
                ds = xr.open_mfdataset(ifile)  #,combine='by_coords')
            except:
                log.error('Error reading {}.'.format(ifile), exc_info=True)
                return -1, dat
        else:
            try:
                ds = xr.open_dataset(ifile)
            except:
                log.error('Error reading {}.'.format(ifile), exc_info=True)
                return -1, dat
        # Reduce to 2D 
        if len(ds.time) > 1:
            startdate = None; enddate = None
            if AVG_LABEL_BEFORE in icol:
                startdate = idate - (icol.get(AVG_LABEL_BEFORE)*HR_TO_NS)
            if AVG_LABEL_AFTER in icol:
                enddate = idate + (icol.get(AVG_LABEL_AFTER)*HR_TO_NS)
            # average over selected time interval
            if startdate is not None and enddate is not None:
                ds = ds.sel(time=slice(startdate,enddate)).mean(dim='time')
            else:
                ds = ds.sel(time=tmpdate,method='nearest')
        else:
            ds = ds.squeeze('time')
        if 'lev' in ds.coords:
            ds = ds.squeeze('lev')
        # Get coordinates if they don't exist yet
        if 'lons' not in dat: 
            dat['lons']=ds.lon
        if 'lats' not in dat: 
            dat['lats']=ds.lat
        # Get values for all specified variable names
        ivars = icol.get('vars')
        for v in ivars:
            dat[v] = _get_2d_array(ds,ivars.get(v),v)
        # close file 
        ds.close()
    return rc, dat 


def _get_2d_array(ds,varinfo,varname):
    '''
    Returns the 2D data array for the given variable of a collection.
    '''
    assert('name_on_file' in varinfo), 'key `name_on_file` is missing for variable {}'.format(varname)
    var  = varinfo.get('name_on_file')
    scal = varinfo.get('scal',1.0)
    arr  = ds[var]*scal
    return arr


def _is_season(season,target):
    return (season==target)


def get_cf_map_taverage(ifiles,startday,endday,varnames,scale_factor,season_name=None):
    '''
    Return data array with temporally averaged values.
    '''
    log = logging.getLogger(__name__)
    log.info('Compute time-averaged CF global map from {}'.format(ifiles))
    if len(glob.glob(ifiles))>1:
        ds = xr.open_mfdataset(ifiles) #,combine='by_coords')
    else:
        ds = xr.open_dataset(ifiles)
        ds = ds.sel(time=slice(startday,endday))
    # eventually subselect months
    if season_name is not None:
        ds = ds.sel(time=_is_season(ds['time.season'],season_name))
    da = get_array_from_ds(ds,varnames)
    ds.close()
    # mean over time 
    da = da.mean(dim='time')
    if 'lev' in da.dims:
        da = da.squeeze('lev')
    if scale_factor != 1.0:
        da.values = da.values * scale_factor
    return da


def get_array_from_ds(ds,varnames):
    '''
    Get the variable from an xarray dataset. Performs special operations for selected variable names.
    '''
    if type(varnames) == type(''):
        varnames = [varnames]
    da = None
    for var in varnames:
        da = ds[var] if da is None else da + ds[var]
    return da
