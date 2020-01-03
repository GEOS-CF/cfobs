#!/usr/bin/env python
# ****************************************************************************
# plot_boxplot.py 
#
# DESCRIPTION: 
# Make a boxplot of CF vs. observations. 
#
# HISTORY:
# 20191223 - christoph.a.keller at nasa.gov - Initial version
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
import logging

from ..parse_string import parse_date 
from ..parse_string import parse_vars
from ..regions import set_regions
from ..statistics import compute_metrics_by_location
from ..statistics import compute_unaggregated_metrics


def plot(orig_df,iday,obstype='o3',modvar=None,plot_by_season=0,plot_by_region=1,regionsfile=None,title='!t (%Y-%m-%d)',modcol='conc_mod',obscol='conc_obs',loccol='location',ofile='boxplot_!t_%Y%m%d.png',statistic='bias',aggregate_by_location=0,minnobs=2,ylabel='!t',**kwargs):
    '''
    Make boxplot of CF vs observation. 
    '''

    log = logging.getLogger(__name__)
    modvar = modvar if modvar is not None else obstype
    df = orig_df.loc[orig_df['obstype']==obstype].copy()
    if df.shape[0] == 0:
        log.warning('No data of obstype {} found!'.format(obstype))
        return
    nrow = 1
    if plot_by_season>0:
        df = seasons.set_season(df)
        ncol=4
    else:
        ncol=1
    if plot_by_region==1:
        df = set_regions(df,regionsfile=regionsfile) 
    fig = plt.figure(figsize=(5*ncol,5*nrow))
    for i in range(ncol):
        # Compute metrics
        iseason = i+1 if plot_by_season>0 else 0
        if iseason>0:
            idf = seasons.reduce_data_to_season(df,iseason)
        if aggregate_by_location==1:
            df_stats = compute_metrics_by_location(
                       idat = idf,
                       season_number=-1,
                       modcol=modcol,
                       obscol=obscol,
                       loccol=loccol,
                       minnobs=minnobs)
        else:
            df_stats = compute_unaggregated_metrics(df,modcol,obscol)
        # Make plot
        ax = fig.add_subplot(nrow,ncol,i+1)
        ax = make_boxplot(ax,df_stats,statistic,plot_by_region,iseason,parse_vars(ylabel,obstype,modvar),**kwargs)
        del(df_stats)
    title = parse_vars(title,obstype,modvar) 
    title = parse_date(title,iday)
    fig.suptitle(title)
    fig.tight_layout(rect=[0, 0.03, 1, 0.97])
    ofile = parse_date(parse_vars(ofile,obstype,modvar),iday)
    plt.savefig(ofile,bbox_inches='tight')
    plt.close()
    log.info('Figure written to '+ofile)
    return


def make_boxplot(ax,df_stats,statistic,plot_by_region,season_number,ylabel,minval=None,maxval=None):
    '''Make the boxplot at the given axis.'''

    #groupby = 'regionShortName' if plot_by_region else None 
    groupby = 'region' if plot_by_region else None 
    df_stats.boxplot(column=statistic,by=groupby,ax=ax,rot=90)
    ax.set_xlabel('')
    if minval is not None and maxval is not None:
        ax.set_ylim(minval,maxval)
    ax.set_ylabel(ylabel)
    if season_number > 0:
         season_name = seasons.get_season_name(season_number)
         ax.set_title(season_name)
    else:
         ax.set_title("")
    return ax
