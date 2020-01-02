#!/usr/bin/env python
# ****************************************************************************
# add_cf_to_obs.py 
#
# DESCRIPTION: 
# Add CF data to an observation data frame. At the very least, the
# input observation data frame must contain the following entries:
# ISO8601 (datetime), location (observation location), obstype 
# (observation type key), value (observation value), unit 
# (observation unit).
# Entries from a yaml configuration file determine how to map the
# CF output to the observations. For example, the following entry
# maps variable O3 from collection chm_tavg_1hr collection to obstype
# 'o3':  
#
#met:
#  template: '/discover/nobackup/projects/gmao/geos_cf/pub/GEOS-CF_NRT/ana/Y%Y/M%m/D%d/GEOS-CF.v01.rpl.met_tavg_1hr_g1440x721_x1.%Y%m%d_%H30z.nc4'
#  vars:
#    ps:
#      name_on_file: 'PS'
#    t10m:
#      name_on_file: 'T10M'
#
#chem:
#  template: '/discover/nobackup/projects/gmao/geos_cf/pub/GEOS-CF_NRT/ana/Y%Y/M%m/D%d/GEOS-CF.v01.rpl.chm_tavg_1hr_g1440x721_v1.%Y%m%d_%H30z.nc4'
#  vars:
#    o3:
#      name_on_file: 'O3'
#      scal: 1.0e+9
#      obstype: 'o3'
#      unit: 'ppbv'
#      mw: 48.0
#      modcol_suffix = '_test'
#
# Providing met variables `ps` and `t10m` ensures that unit conversion from
# ugm-3 to ppbv can be performed (along with the molecular weight defined for
# o3).
# By default, the CF model values will be written into the column specified
# in input arg `modcol`. The observation values, converted to model units, 
# will be copied (with unit conversion, if necessary) to column `obscol`.
# The model output can be written into a different column by specified a
# model column suffix, as in the example above (in the above example, the 
# mapped O3 output would be written into the default column name plus suffix
# '_test').
#
# HISTORY:
# 20191211 - christoph.a.keller at nasa.gov - Initial version
# ****************************************************************************
import numpy as np
import datetime as dt
import pandas as pd
import os
from tqdm import tqdm

from .systools import load_config 
from .read_cf import read_cf_data_2d
from .seasons import set_season
from .units import get_conv_ugm3_to_ppbv
from .units import to_ppbv 

def add_cf(df_in,configfile=None,verbose=1,modcol='conc_mod',obscol='conc_obs',unitcol='conc_unit'):
    '''
    Adds CF output to an (observations) data frame by sampling the data frame by datetime, longitude and latitude and
    read the corresponding CF value. This function currently only aggregates by hours, i.e. all minute information 
    is omitted. 
    '''
    rc = 0
    if verbose > 0:
        print('Matching CF output to observations...')
    cf_config,map_config = _read_config(verbose,configfile) 
    # 'Round' all time stamps to hours. Will only aggregate by hourly values
    df_in['ISO8601'] = [dt.datetime(i.year,i.month,i.day,i.hour,0,0) for i in df_in['ISO8601']]
    # Check for local time. If it exists, convert to seconds since lowest datetime to ensure proper
    # handling by the groupby function. Will be converted back to datetime at the end
    if 'localtime' in df_in.keys():
        reftime = df_in['ISO8601'].min()
        df_in['localtime'] = [(i-reftime)/np.timedelta64(1,'s') for i in df_in['localtime']]
    # Aggregate by datetime, location, observation type, and unit 
    df = df_in.groupby(['ISO8601','location','obstype','unit']).mean().reset_index()
    ncol = df.shape[0]
    # Add new columns with CF data and other ancilliary information, will be filled below
    df[modcol]           = np.zeros((ncol,))*np.nan
    df[obscol]           = np.zeros((ncol,))*np.nan
    df[unitcol]          = ['unknown' for i in range(ncol)]
    # Loop over all time stamps and add CF data 
    for idate in tqdm(list(df['ISO8601'].unique())):
        rc, df = _add_cf_data_to_df(df,idate,cf_config,map_config,verbose,obscol,modcol,unitcol)
        # error check
        if rc != 0:
            break
    if rc != 0:
        return None
    # Identify columns that were removed by the groupby call at the beginning (character type) 
    c2fill = []
    for c in list(df_in.keys()):
        if c not in df.keys():
            c2fill.append(c)
    # Re-add removed column data
    if len(c2fill)>0:
        if verbose>2:
            print('Re-add removed columns...')
        # group original data by location and all columns of interest. 
        # This is expected to return only one entry per location but sometimes the same station can have different character meta-data. In that case we will pick the one that occurs most often.
        grouped_by_station = df_in.groupby(['location']+c2fill).count().reset_index()
        grouped_by_station = grouped_by_station.groupby(['location']).max().reset_index()[['location']+c2fill]
        df = df.merge(grouped_by_station,on='location')
    # write out season for each entry
    df = set_season(df) 
    # convert localtime back to datetime
    if 'localtime' in df.keys():
        df['localtime'] = [reftime+dt.timedelta(seconds=i) for i in df['localtime']]
    return df


def _add_cf_data_to_df(df,idate,cf_config,map_config,verbose,obscol,modcol,unitcol):
    '''
    Updates the observation data frame for a given datetime by reading CF data and
    adding the proper CF value to each observation. Also assigns a 'observation value'
    to each observation that has the same unit as the model value. The model value
    is labelled 'conc_mod', and the observation value is labelled 'conc_obs'. 
    '''
    # Read CF data, decrease verbose level to avoid excessive statements
    rc, dat = read_cf_data_2d(idate=idate,config=cf_config,verbose=verbose-1)
    if rc != 0:
        return rc, None
    # Get coordinate values 
    lons = dat['lons'].values
    lats = dat['lats'].values
    # Precompute conversion factor from ugm-3 to ppbv, using MW of 1.0
    conv = get_conv_ugm3_to_ppbv(dat,'t10m','ps',1.0)
    # Update for all species, use the model <-> observation pairs specified in the
    # configuration file 
    for ivar in map_config: 
        var_config = map_config.get(ivar)
        if 'obstype' not in var_config:
            print('Warning: no obstype defined for {} - skip variable'.format(ivar))
            continue
        idx = df.index[ (df['ISO8601']==idate) & (df['obstype']==var_config.get('obstype')) ]
        if len(idx)==0:
            continue
        # by default, set observation column to same as 'value'
        df.loc[idx,obscol] = df.loc[idx,'value']
        # get index values for each location
        lonidx = [np.abs(lons-i).argmin() for i in df.loc[idx,'lon'].values]
        latidx = [np.abs(lats-i).argmin() for i in df.loc[idx,'lat'].values]
        # update model value 
        imodcol = modcol+var_config.get('modcol_suffix',"")
        # eventually add new column if it does not yet exist in data frame. This should
        # only be the case for modcol_suffix values
        if imodcol not in df.keys():
            df[imodcol] = np.zeros((df.shape[0],))*np.nan
        # Add CF variables, ignore NaN's 
        if 'cfvars' not in var_config:
            if verbose > 0:
                print('Warning: no CF variables defined for {} - no CF data will be matched to observations'.format(ivar))
            continue
        cfvars = var_config.get('cfvars')
        if type(cfvars) == type(''):
            cfvars = [cfvars]
        for cfvar in cfvars:
            # set NaN's to zero first
            nanidx = df.loc[idx].index[np.isnan(df.loc[idx,imodcol])]
            if len(nanidx)>0:
                df.loc[nanidx,imodcol] = 0.0 
            # add CF data
            df.loc[idx,imodcol] = df.loc[idx,imodcol]+dat[cfvar].values[latidx,lonidx]
        # check for units
        unit = var_config.get('unit','unknown') 
        df.loc[idx,unitcol] = unit
        if unit=='ppbv':
            assert('mw' in var_config), 'Cannot convert to ppbv, please provide `mw` in configuration file: {}'.format(ivar)
            assert(conv is not None), 'Cannot convert to ppbv, conversion factor does not exist - please specifiy t10m and ps in configuration file'
            df = to_ppbv(df,idx=idx,conv_ugm3_to_ppbv=conv,convscal=1./var_config['mw'])
    return rc, df


def _read_config(verbose,configfile):
    '''
    Read the configuration file that define the CF variables to be read as well as the mapping between observations and model variables..
    '''
    master_config = load_config(configfile)
    assert('mapping' in master_config), print('key `mapping` must be provided in configuration file {}'.format(configfile))
    map_config = master_config.get('mapping')
    assert('cf_config' in master_config), print('key `cf_config` must be provided in configuration file {}'.format(configfile))
    cf_config = master_config.get('cf_config')
    if 'configuration_file' in cf_config:
        cf_configfile = cf_config.get('configuration_file')
        if not os.path.isfile(cf_configfile):    
            cf_configfile = '/'.join(configfile.split('/')[:-1])+'/'+cf_configfile
        cf_config = load_config(cf_configfile)
    return cf_config,map_config

