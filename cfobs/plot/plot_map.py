#!/usr/bin/env python
# ****************************************************************************
# plot_map.py 
#
# DESCRIPTION: 
# Comparison of GEOS-CF against observations data.
#
# HISTORY:
# 20191113 - christoph.a.keller at nasa.gov - Initial version
# ****************************************************************************
import numpy as np
import datetime as dt
import pandas as pd
import os
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import matplotlib.dates as mdates
from matplotlib.gridspec import GridSpec
from matplotlib.cm import get_cmap
import cartopy.crs as ccrs
import cartopy.feature 
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter
import logging

from ..parse_string import parse_date
from ..parse_string import parse_vars
from ..statistics import compute_metrics_by_location
from ..read_cf import get_cf_map_taverage


def plot(orig_df,iday,endday=None,obstype='o3',plot_by_season=0,mapfiles='',modvar='O3',mapvar='O3',mapvarscal=1.0,title='!v (%Y-%m-%d)',
         modcol='conc_mod',obscol='conc_obs',loccol='location',minnobs=2,ofile='map_!v_%Y%m%d.png',statistic='IOA',maplabel='!v',**kwargs):
    '''
    Create a three-panel figure with global maps of (1) model values with observation data overlaid to it; (2) mean model-observation bias at each observation location; (3, optional) another statistical measure for model-observation fit at each location, as specified in the input arguments (default is to show Index of Agreement).
    '''

    log = logging.getLogger(__name__)
    if endday is None:
        endday = iday + dt.timedelta(days=1)
    df = orig_df.loc[orig_df['obstype']==obstype].copy()
    if df.shape[0] == 0:
        log.warning('Warning: no data of obstype {} found!'.format(obstype))
        return
    if plot_by_season>0:
        ncol=4
    else:
        ncol=1
    if statistic is None or statistic=="":
        nrow = 2
    else:
        nrow = 3
    fig = plt.figure(figsize=(5*ncol,3*nrow))
    gs  = GridSpec(nrow,ncol)
    mapfiles_parsed = parse_date(mapfiles,iday)
    for i in range(ncol):
        # Compute metrics
        iseason = i+1 if plot_by_season>0 else 0 
        df_agg = compute_metrics_by_location(
                  idat = df,
                  season_number=iseason,
                  modcol=modcol,
                  obscol=obscol,
                  loccol=loccol,
                  minnobs=minnobs)
        # Read CF annual map
        season_name = seasons.get_season_name(iseason) if iseason>0 else None
        cfmap = get_cf_map_taverage(mapfiles_parsed,iday,endday,mapvar,mapvarscal,season_name)
        # Plot CF annual map, overlay with observation data
        _plot_map_and_statistics(
         fig=fig,
         gs=gs,
         idx=i,
         cf=cfmap,
         dat=df_agg,
         season_name=season_name,
         obscol=obscol, 
         statistic=statistic, 
         maplabel=parse_vars(maplabel,obstype,modvar),
         **kwargs)
        del(df_agg)
    title = parse_vars(title,obstype,modvar)
    title = parse_date(title,iday)
    fig.suptitle(title)
    fig.tight_layout(rect=[0, 0.03, 1, 0.97])
    ofile = parse_vars(ofile,obstype,modvar)
    ofile = parse_date(ofile,iday)
    plt.savefig(ofile,bbox_inches='tight')
    plt.close()
    log.info('Figure written to '+ofile)
    return


def _plot_map_and_statistics(fig,gs,idx,cf,dat,season_name=None,statistic='IOA',maxbias=50.0,maxstat=50.0,minval=0.0,maxval=80.0,dotedgecolor=None,dotsize=10,latcol='lat',loncol='lon',obscol='conc_obs',maplabel='None',colormap='rainbow'):
    '''
    Plots a global map with the annual average model field and the observations overlaid to it.
    '''

    log = logging.getLogger(__name__)
    log.info('Plot map...')
    proj = ccrs.PlateCarree()
    # plot map, overlay observations 
    colormap = get_cmap(colormap)
    ax = fig.add_subplot(gs[0,idx],projection=proj)
    cf1 = ax.add_feature(cartopy.feature.BORDERS, edgecolor="grey")
    cf2 = ax.add_feature(cartopy.feature.COASTLINE, edgecolor="black")
    flev = np.linspace(minval,maxval,51)
    cp = ax.contourf(cf.lon.values,cf.lat.values,cf.values,transform=proj,cmap=colormap,levels=flev,extend='max')
    # Overlay obs 
    sc = _add_obs_to_map(ax,dat[loncol].values,dat[latcol].values,dat[obscol].values,dotsize,dotedgecolor,colormap,proj,minval,maxval)
    cbar = fig.colorbar(cp,ax=ax,shrink=1.0,extend='max',pad=0.02,ticks=np.linspace(minval,maxval,11))
    tc = cbar.ax.set_ylabel(maplabel)
    # Eventually set title
    if season_name is not None:
        ax.set_title(season_name)
    # Maps with difference between model and observation 
    _plot_statistics(fig,gs[1,idx],'bias',proj,dat,maxbias,maxstat,loncol,latcol,dotsize,dotedgecolor)
    if statistic is not None and statistic != "":
        _plot_statistics(fig,gs[2,idx],statistic,proj,dat,maxbias,maxstat,loncol,latcol,dotsize,dotedgecolor)
    return


def _plot_statistics(fig,this_gs,stat,proj,dat,maxbias,maxstat,loncol,latcol,dotsize,dotedgecolor):
    '''
    Makes a plot of the given statistical metric
    '''
    assert(stat in ['bias','RMSE','NMB','IOA','MAE','R2']), 'Invalid statistic parameter: '+stat
    if stat == 'bias':
        sname = 'bias'
        minv = maxbias*-1.0
        maxv = maxbias
        flev = np.linspace(minv,maxv,101)
        colormap = get_cmap('seismic')
        ylab = 'Bias (model - observation)'
        extd = 'both'
    if stat == 'NMB':
        sname = 'NMB'
        minv = maxstat*-1.0
        maxv = maxstat
        flev = np.linspace(minv,maxv,101)
        colormap = get_cmap('seismic')
        ylab = 'Normalized mean bias'
        extd = 'both'
    if stat == 'RMSE':
        sname = 'RMSE'
        minv =  0.0 
        maxv =  maxstat 
        flev = np.linspace(minv,maxv,51)
        colormap = get_cmap('coolwarm')
        ylab = 'Root mean square error'
        extd = 'max'
    if stat == 'IOA':
        sname = 'IOA'
        minv =  0.0 
        maxv =  1.0
        flev = np.linspace(minv,maxv,51)
        colormap = get_cmap('gist_rainbow_r')
        ylab = 'Index of agreement'
        extd = 'neither'
    if stat == 'R2':
        sname = 'R2'
        minv =  0.0 
        maxv =  1.0
        flev = np.linspace(minv,maxv,51)
        colormap = get_cmap('gist_rainbow_r')
        ylab = 'R$^{2}$'
        extd = 'neither'
    if stat == 'MAE':
        sname = 'AbsErr'
        minv =  0.0 
        maxv =  maxstat
        flev = np.linspace(minv,maxv,51)
        colormap = get_cmap('coolwarm')
        ylab = 'Mean absolute error'
        extd = 'max'
    ax = fig.add_subplot(this_gs,projection=proj)
    ax.set_global()
    cf1 = ax.add_feature(cartopy.feature.BORDERS, edgecolor="grey")
    cf2 = ax.add_feature(cartopy.feature.COASTLINE, edgecolor="black")
    sc = _add_obs_to_map(ax,dat[loncol].values,dat[latcol].values,dat[sname].values,dotsize,dotedgecolor,colormap,proj,minv,maxv)
    cbar = fig.colorbar(sc,ax=ax,shrink=1.0,extend=extd,pad=0.02,ticks=np.linspace(minv,maxv,11))
    tc = cbar.ax.set_ylabel(ylab)
    return

def _add_obs_to_map(ax,lons,lats,vals,dotsize,dotedgecolor,colormap,proj,minv,maxv):
    '''Overlay observation data to a previously drawn map.'''
    return ax.scatter(lons,lats,c=vals,s=dotsize,cmap=colormap,transform=proj,vmin=minv,vmax=maxv,edgecolors=dotedgecolor)

