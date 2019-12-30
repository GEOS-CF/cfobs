#!/usr/bin/env python
# ****************************************************************************
# statistics.py 
#
# DESCRIPTION: 
# Contains functions to compute model-observation statistics. 
# 
# HISTORY:
# 20192021 - christoph.a.keller at nasa.gov - Initial version
# ****************************************************************************
import numpy as np
import datetime as dt
import pandas as pd

from .seasons import reduce_data_to_season


def compute_unaggregated_metrics(dat,modcol='conc_mod',obscol='conc_obs'):
    '''
    Compute bias and absolute error on the unaggregated dataset.
    '''
    dat['bias']   = dat[modcol].values - dat[obscol].values
    dat['AbsErr'] = np.abs(dat['bias'])
    return dat


def compute_metrics_by_location(idat,season_number=-1,verbose=0,modcol='conc_mod',
                                obscol='conc_obs',loccol='location',minnobs=2):
    '''
    Compute location-aggregated metrics (bias, rmse, r2, etc.) and returns
    a data frame with these metrics grouped by location. 
    '''
    if verbose>0:
        print('Compute metrics...')
    # Make local copy of input array, since we will be messing around with it.
    # Subsamble to data for selected season if applicable. 
    if season_number <= 0:
        dat = idat.copy()
    else:
        dat = reduce_data_to_season(idat,season_number)
    # Compute some metrics on the full data array
    dat = compute_unaggregated_metrics(dat,modcol,obscol)
    dat['bias2'] = dat['bias']**2
    # Aggregate data by location (mean and sum)
    # df is going to be the output array
    group = dat.groupby(loccol)
    df     = group.mean()
    df_sum = group.sum().reset_index()
    # Toss locations that do not have enough observations, if a minimum
    # number of observations is specified. 
    if minnobs is not None:
        df_count = group.count()
        idx = df_count.index[df_count[obscol]>=minnobs]
        df = df.loc[idx]
    # Get RMSE and NMB
    df['RMSE'] = df['bias2']**0.5
    df['NMB'] = df_sum['bias'] / df_sum[obscol]
    # Compute Index of Agreement and Pearson correlation coefficient 
    # For this we first need to add the means (by location) and compute 
    # some additional statistics to the non-aggregated array
    # means are the mean observation and model value per location
    means = pd.DataFrame()
    means[loccol]     = df.index
    means['obsmean']  = df[obscol].values
    means['modmean']  = df[modcol].values
    # add these values to dat and compute metrics needed for IOA and R2
    dat = dat.merge(means,on=loccol)
    dat['mod-modmean']  = dat[modcol].values - dat['modmean'].values
    dat['mod-variance'] = dat['mod-modmean'].values**2
    dat['mod-obsmean']  = dat[modcol].values - dat['obsmean'].values
    dat['obs-obsmean']  = dat[obscol].values - dat['obsmean'].values
    dat['obs-variance'] = dat['obs-obsmean'].values**2
    dat['diffproduct']  = dat['mod-modmean'].values * dat['obs-obsmean'].values
    dat['abserrsum']    = np.abs(dat['mod-obsmean'].values)+np.abs(dat['obs-obsmean'].values)
    dat_sum = dat.groupby(loccol).sum().reset_index()
    # Calculate IOA in a new data frame, then merge with the output frame
    ioadat = dat_sum[[loccol,'abserrsum']].copy()
    ioadat = ioadat.merge(df_sum[[loccol,'AbsErr']],on=loccol)
    ioadat['IOA'] = 1.0 - ( ioadat['AbsErr'].values / ioadat['abserrsum'].values )
    df = df.merge(ioadat[[loccol,'IOA']],on=loccol)
    del(ioadat)
    # Calculate R2 in a new data frame, then merge with the output frame
    r2dat = dat_sum[[loccol,'diffproduct','mod-variance','obs-variance']].copy()
    r2dat['vars'] = np.sqrt(r2dat['mod-variance'].values) * np.sqrt(r2dat['obs-variance'].values)
    r2dat['R2'] = ( r2dat['diffproduct'] / r2dat['vars'] )**2
    df = df.merge(r2dat[[loccol,'R2']],on=loccol)
    del(r2dat)
    # All done
    return df
