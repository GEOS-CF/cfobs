#!/usr/bin/env python
# ****************************************************************************
# popdensity.py 
#
# DESCRIPTION: 
# Use population density to categorize remote and urban areas.
# 
# HISTORY:
# 20200309 - christoph.a.keller at nasa.gov - Initial version
# ****************************************************************************
import logging
import os
import numpy as np
import xarray as xr
import datetime as dt


def set_popdens(df,densfile,densvar="PopulationDensity",timeslice=dt.datetime(2020,1,1),latcol='lat',loncol='lon',colname='popdens'):
    '''
    Maps population density - read from file densfile - to each observation. 

    Arguments
    ---------
    df : pd.DataFrame
        cfobs dataframe with observations. Population density will be added to this frame
    densfile : str
        name of netCDF file with population density information
    densvar : str
        population density variable name on netCDF file
    timeslice : dt.datetime
        population density time slice of interest. Only used if netCDF file has more than one
        time slice (closest time stamp will be taken)
    latcol : str
        latitude column name in df dataframe to be used for mapping
    loncol : str
        longitude column name in df dataframe to be used for mapping
    colname : str
        column name to be assigned to population density values

    Returns
    -------
    df : pd.DataFrame
        Updated cfobs dataframe with population density column added to it
    '''

    log = logging.getLogger(__name__)
    df[colname] = np.zeros((df.shape[0]),)*np.nan
    # read data
    log.debug('Reading {}'.format(densfile))
    ds = xr.open_dataset(densfile)
    pop = ds[densvar]
    if 'time' in pop.coords:
        pop = pop.sel(time=timeslice,method='nearest')
    pop_lats = pop.lat.values
    pop_lons = pop.lon.values
    # get lat/lon index for each entry in df
    lonidx = [np.abs(pop_lons-i).argmin() for i in df[loncol].values] 
    latidx = [np.abs(pop_lats-i).argmin() for i in df[latcol].values] 
    df[colname] = pop.values[latidx,lonidx]
    ds.close()
    return df



