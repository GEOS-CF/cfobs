#!/usr/bin/env python
# ****************************************************************************
# compact.py 
#
# DESCRIPTION: 
# Contains functions to create compact data arrays, particularly useful for 
# benchmarking 
# 
# HISTORY:
# 20210727 - christoph.a.keller at nasa.gov - Initial version
# ****************************************************************************
import numpy as np
import datetime as dt
import pandas as pd
import logging
import seaborn as sns
import glob
import os
import xarray as xr
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter


def compact_dat(idat,label,obstype=None,freq='1M',nmin=0,nmin_agg=None,remove_nan=True,set_to_first_of_month=True):
    '''
    Create "compact" data array by aggregating data from the input data frame into
    monthly (default, other frequency can be specified via freq argument) data. Set
    all data with less than nmin original data to nan. Similarly, remove all locations
    where there are less than nmin_agg aggregated (valid) samples.
    '''
    log = logging.getLogger(__name__)
    if obstype is not None:
        idat = idat.loc[idat['obstype']==obstype].copy()
    odat = idat.set_index('ISO8601').groupby(['original_station_name','obstype','unit']).resample(freq).mean().reset_index()
    odat_count = idat.set_index('ISO8601').groupby(['original_station_name','obstype','unit']).resample(freq).count()
    odat['count'] = odat_count['latlon_id'].values
    odat.loc[odat['count']<nmin,'value'] = np.nan
    odat['loc_name'] = ['{0} ({1:.1f}N {2:.1f}E)'.format(i,j,k) for i,j,k in zip(odat['original_station_name'],odat['lat'],odat['lon'])]
    odat = odat[['ISO8601','loc_name','lat','lon','obstype','unit','value']].copy()
    odat['label'] = [label for i in range(odat.shape[0])]
    if set_to_first_of_month:
        odat['ISO8601'] = [dt.datetime(i.year,i.month,1) for i in odat['ISO8601']]
    # remove nans
    if remove_nan:
        locs = odat[['loc_name','value']].groupby('loc_name').mean().reset_index() 
        locs = locs.loc[~np.isnan(locs['value'])].copy()
        odat = odat.loc[odat['loc_name'].isin(list(locs['loc_name']))].copy() 
    # reduce to locations with at least nmin entries
    if nmin_agg is not None:
        ilocs = odat.groupby('loc_name').count().reset_index()
        ilocs = list(ilocs.loc[ilocs['value']>=nmin_agg]['loc_name'])
        odat = odat.loc[odat['loc_name'].isin(ilocs)].copy()
    return odat


def compact_addcf(cdat,label,obstype,unit,file_template,var,scal,mindate,maxdate,freq='1M',set_to_first_of_month=True):
    '''
    Append CF data to a compact data frame.
    '''
    log = logging.getLogger(__name__)
    latidx=None; lonidx=None
    locs = cdat[['loc_name','lat','lon']].groupby('loc_name').mean().reset_index()     
    for idt in pd.date_range(mindate,maxdate,freq=freq):
        idate = dt.datetime(idt.year,idt.month,1) if set_to_first_of_month else dt.datetime(idt.year,idt.month,idt.day)
        ifile = glob.glob(idate.strftime(file_template))
        idat = locs.copy()
        idat['ISO8601'] = [idate for i in range(idat.shape[0])]
        idat['obstype'] = [obstype for i in range(idat.shape[0])]
        idat['unit'] = [unit for i in range(idat.shape[0])]
        idat['value'] = [np.nan for i in range(idat.shape[0])]
        idat['label'] = [label for i in range(idat.shape[0])]
        if len(ifile)>0:
            log.info('Reading {}'.format(ifile))
            if len(ifile)==1:
                ds = xr.open_dataset(ifile[0])
            else:
                ds = xr.open_mfdataset(ifile).mean(dim='time')
            if var in ds:
                ndim = len(ds[var].shape)
                if ndim==4:
                    sarr = ds[var][0,71,:,:]
                if ndim==3:
                    sarr = ds[var][0,:,:]
                if ndim==2:
                    sarr = ds[var][:,:]
                if latidx is None:
                    latidx = xr.DataArray([np.abs(ds.lat.values-i).argmin() for i in locs['lat'].values],dims='z')
                    lonidx = xr.DataArray([np.abs(ds.lon.values-i).argmin() for i in locs['lon'].values],dims='z')
                ivals=sarr.isel(lat=latidx,lon=lonidx)
                idat['value'] = ivals.values*scal
            else:
                log.warning('Variable not found in file: {}'.format(var))
        else:
            log.warning('No files found: {}'.format(idate.strftime(file_template)))
        cdat = cdat.append(idat[cdat.columns])
    return cdat


def compact_plot(cdat,xlabel="",ylabel="",ofile='fig.png',stat=None,obslabel=None,group_all=False,**kwargs):
    '''
    Plot compact data
    '''
    log = logging.getLogger(__name__)
    # data array to be plotted: either copy data array or compute statistics first
    if stats is not None:
        carr = compact_stats(cdat,obslabel,metrics=stats,group_all=group_all) 
    else:
        carr = cdat.copy()
    # prepare data: set nan values to inf to make sure that the lines are not being connected by relplot below
    carr.loc[np.isnan(carr['value']),'value'] = np.inf
    locs = carr[['loc_name','lat']].groupby('loc_name').mean().reset_index()     
    colorder=list(locs.sort_values(by='lat',ascending=False)['loc_name'])
    # make plot
    sns.set(style='darkgrid')
    g = sns.relplot(data=carr,x="ISO8601", y="value", col="loc_name", hue="label",col_order=colorder,**kwargs)
    #g.set(ylim=(0.25, 1.75))
    (g.set_axis_labels(xlabel, ylabel)
      .set_titles("{col_name}"))
    for ax in g.axes.flat:
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
        ax.xaxis.set_major_formatter(DateFormatter('%Y-%m'))
    #plt.tight_layout(w_pad=0)
    plt.savefig(ofile)
    plt.close()
    log.info('Figure written to {}'.format(ofile))
    return


def compact_stats(cdat,obslabel,metrics='RMSE',group_all=False):
    '''
    Calculate statistics on compact data
    '''
    log = logging.getLogger(__name__)
    supported_metrics = ['RMSE','NMB','MAPE']
    if metrics not in supported_metrics:
        log.error('metrics not supported: {}. Must be one of {}'.format(metrics,supported_metrics))
        return None 
    # select observations
    obsdat = cdat.loc[cdat['label']==obslabel].copy()
    obsdat['obs'] = obsdat['value']
    obsdat = obsdat[['ISO8601','loc_name','obstype','obs']]
    stats = pd.DataFrame()
    group_vars = ['ISO8601','loc_name','obstype'] if not group_all else ['ISO8601','obstype']
    for ilabel in cdat['label'].unique():
        if ilabel==obslabel:
            continue
        idat = cdat.loc[cdat['label']==ilabel].copy()
        mdat = idat.merge(obsdat,on=['ISO8601','loc_name','obstype'],how='inner') 
        mdat['bias']  = mdat['value']-mdat['obs']
        mdat['bias2'] = mdat['bias'].values**2
        mdat['ape']   = np.abs(mdat['bias'].values) / mdat['obs']
        istat = mdat.groupby(group_vars).mean().reset_index()
        idf = istat[group_vars].copy()
        idf['lat'] = istat['lat'] # need latitude to order locations when plotting
        if metrics=='RMSE':
            idf['value'] = np.sqrt(istat['bias2'].values)
        if metrics=='NMB':
            idf['value'] = istat['bias'].values / istat['obs'].values
        if metrics=='MAPE':
            idf['value'] = istat['ape'].values
        # add to statistics array
        if group_all:
            idf['loc_name'] = ['all_locations' for i in range(idf.shape[0])]
        idf['label'] = [':_'.join([metrics,ilabel]) for i in range(idf.shape[0])]
        stats = stats.append(idf)
    return stats
