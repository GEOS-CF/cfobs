#!/usr/bin/env python
# ****************************************************************************
# cfobs_save.py 
# 
# DESCRIPTION:
# Save cfobs data to a csv file 

# HISTORY:
# 20191220 - christoph.a.keller at nasa.gov - Initial version 
# ****************************************************************************
import os
import numpy as np
import datetime as dt
import pandas as pd
import logging

from .parse_string import parse_date 


def save(df,file,iday,append=False,nfloats=-1,opened_files=[]):
    '''
    Save the dataframe 'df' to a csv file.
    '''
    log = logging.getLogger(__name__)
    # File to save data to
    ofile = parse_date(file,iday)
    # Does file exist?
    hasfile = os.path.isfile(ofile)
    # Determine if we need to append to existing file or if a new one shall be created. Don't write header if append to existing file. 
    if not hasfile:
        wm    = 'w+'
        hdr   = True
    # File does exist:
    else:
        # Has this file been previously written in this call? In this case we always need to append it. Same is true if append option is enabled
        if ofile in opened_files or append:
            wm  = 'a'
            hdr = False
        # If this is the first time this file is written and append option is disabled: 
        else:
            wm  = 'w+'
            hdr = True
    # If appending, make sure order is correct. This will also make sure that all variable names match
    if wm == 'a':
        file_hdr = pd.read_csv(ofile,nrows=1)
        df = df[file_hdr.keys()]
    else:
        # reorder to put date and location first
        new_hdr = _get_new_header(df)
        df = df[new_hdr]
    # Eventually round floats
    df = _round_floats(df,nfloats)
    # Write to file
    df.to_csv(ofile,mode=wm,date_format='%Y-%m-%dT%H:%M:%SZ',index=False,header=hdr,float_format='%.4f')
    log.info('{:,} values written to {}'.format(df.shape[0],ofile))
    if ofile not in opened_files:
        opened_files.append(ofile)
    return opened_files


def _round_floats(df,nfloats):
    '''
    Rounds all float values to the number of digits specified in nfloats. 
    '''
    if nfloats>=0:
        for icol in df.keys():
            if df[icol].values.dtype == type(0.0):
                df[icol] = np.round(df[icol].values,nfloats)
    return df


def _get_new_header(df):
    '''
    Returns a list with all header names, sorted in a reasonable fashion. The date (ISO8601) is always placed first.
    '''
    # original header
    old_hdr = list(df.keys())
    new_hdr = []
    # check following elements and place them first in new header, remove from old header list once added
    elements = ['ISO8601','localtime','location','original_station_name','country','lat','lon','latlon_id','location_gridded','lat_gridded','lon_gridded','obstype','unit','value']
    for e in elements:   
        if e in old_hdr:
            new_hdr.append(e)
            old_hdr.remove(e)
    # add remaining elements of old header to new header
    for i in old_hdr:
        new_hdr.append(i)
    return new_hdr
