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

from .parse_string import parse_date 


def get_cf_config(config_file):
    '''
    Read the configuration (collections & variables to be read) from a YAML file.
    '''
    if not os.path.isfile(config_file):
        print('Error - file not found: {}'.format(config_file))
        return 3,None
    with open(config_file,'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    return config


def read_cf_data_2d(idate,config=None,config_file=None,verbose=1):
    '''
    Reads the CF output for the given datetime and returns all variables as specified
    in the configuration file.
    The configuration file must list all collections and variables therein to be read
    by this routine. For each collection, the file template and a variable list must
    be given, i.e.:

    met:
      template: '/path/to/file/filename.%Y%m%d%H.nc4' 
      vars:
        var1:
          name_on_file: Variable1
        var2:
          name_on_file: Variable2
          scal: 100.0 

    This routine returns a dictionary with the 2d arrays of all variables.
    '''
    # Configuration
    if config is None:
        config = get_cf_config(config_file)
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
            print('Warning: `template` not defined for collection {} - will skip'.format(collection))
            continue
        if 'vars' not in icol:
            print('Warning: no variable read from collection {} because `vars` key is missing'.format(collection))
            continue
        # Open collection
        ifile = parse_date(icol.get('template'),tmpdate)
        if verbose>0:
            print('Reading {}'.format(ifile))
        try:
            ds = xr.open_dataset(ifile)
        except:
            print('Error reading {}.'.format(ifile))
            return -1, dat
        # Reduce to 2D 
        if len(ds.time) > 1:
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


def get_cf_map_taverage(ifiles,startday,endday,verbose,varname,varscal,season_name=None):
    '''
    Returns temporally averaged spatial values read from CF.
    '''
    if verbose>0:
        print('Reading CF global map from {}'.format(ifiles))
    if len(glob.glob(ifiles))>1:
        ds = xr.open_mfdataset(ifiles)
    else:
        ds = xr.open_dataset(ifiles)
        ds = ds.sel(time=slice(startday,endday))
    # eventually subselect months
    if season_name is not None:
        ds = ds.sel(time=_is_season(ds['time.season'],season_name))
    # mean over time 
    df = ds.mean(dim='time')
    if varname is 'AOD550_TOTAL':
        df = df[['AOD550_BC','AOD550_OC','AOD550_DUST','AOD550_SULFATE','AOD550_SALA','AOD550_SALC']]
        df['AOD550_TOTAL'] = df['AOD550_BC']+df['AOD550_OC']+df['AOD550_DUST']+df['AOD550_SULFATE']+df['AOD550_SALA']+df['AOD550_SALC']
    else:
        df = df[[varname]]
    if 'lev' in df[varname].dims:
        df = df.squeeze('lev')
    if varscal != 1.0:
        df[varname].values = df[varname].values * varscal
    return df
