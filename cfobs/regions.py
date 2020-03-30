#!/usr/bin/env python
# ****************************************************************************
# regions.py 
#
# DESCRIPTION: 
# Handle the definition of regions.
# 
# HISTORY:
# 20191211 - christoph.a.keller at nasa.gov - Initial version
# ****************************************************************************
import os
import yaml
import numpy as np
import logging
import requests


def define_regions(regionsfile):
    '''
    Reads region information from a YAML file. This information
    is used to group the data into regional bins. 
    '''
    log = logging.getLogger(__name__)
    if not os.path.isfile(regionsfile):
        log.error('Region configuration file does not exist: {}'.format(regionsfile),exc_info=True)
        return None
    with open(regionsfile,'r') as f:
        regions = yaml.load(f, Loader=yaml.FullLoader)
    return regions


def set_regions(df,regions=None,regionsfile=""):
    '''
    Assigns regionID's and region names to the df object. 
    The regions information can be passed as a dictionary object.
    If not specified, it is read from a YAML file.
    '''
    df['regionID'] = np.zeros((df.shape[0]),).astype(int)
    df['region'] = ['unknown' for i in range(df.shape[0])]
    df['regionShortName'] = ['unknown' for i in range(df.shape[0])]
    if regions is None:
        regions = define_regions(regionsfile)
    assert(regions is not None), 'Error getting regions definition'
    for r in regions:
        idx = df.index[(df['lat']>=regions[r]['minlat'])&(df['lat']<regions[r]['maxlat'])&(df['lon']>=regions[r]['minlon'])&(df['lon']<regions[r]['maxlon'])]
        if len(idx)>0:
            df.loc[idx,'regionID'] = regions[r]['regionID']
            df.loc[idx,'region']   = r
            if 'region_shortname' in regions[r]:
                df.loc[idx,'regionShortName'] = regions[r].get('region_shortname')
    return df


def get_timezone(lat,lon):
    '''Return the timezone for the given lat/lon, using tzwhere'''
    from tzwhere import tzwhere
    tz = tzwhere.tzwhere(forceTZ=True)
    return tz.tzNameAt(lat,lon,forceTZ=True)
