#!/usr/bin/env python
# ****************************************************************************
# plot_locations.py 
#
# DESCRIPTION: 
# Plot a map with the locations data 
#
# HISTORY:
# 20190310 - christoph.a.keller at nasa.gov - Initial version
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
import matplotlib.patches as mpatches
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
from ..regions import define_regions


def plot(df,iday,types=None,typeby='obstype',groupby='original_station_name',labels=None,markers='o',markersize=5,ofile='locations.png',title='Observation locations',colors=['red','blue','pink','yellow','green'],regionsfile=None,colormap=True,**kwargs):
    '''Make a map with the locations of all measurement sites.'''

    log = logging.getLogger(__name__)
    fig = plt.figure(figsize=(10,5))
    gs  = GridSpec(1,1)
    proj = ccrs.PlateCarree()
    ax = fig.add_subplot(gs[0,0],projection=proj)
    if colormap:
        _ = ax.stock_img()
    _ = ax.coastlines()
    types = list(df[typeby].unique()) if types is None else types
    orig_labels = []
    handles = []
    for cnt,t in enumerate(types):
        idf = df.loc[df[typeby]==t].groupby(groupby).mean()
        imarker = markers[cnt] if type(markers)==type([]) else markers
        isize = markersize[cnt] if type(markersize)==type([]) else markersize
        sc1 = ax.scatter(x=idf.lon.values,y=idf.lat.values,s=isize,c=colors[cnt],marker=imarker,zorder=2,**kwargs)
        handles.append(sc1)
        orig_labels.append(t)
    labels = labels if labels is not None else orig_labels
    fig.legend( handles=handles, labels=labels, ncol=len(labels), loc='lower center' )
    # eventually add regions
    if regionsfile is not None:
        regions = define_regions(regionsfile)
        for r in regions:
            x1 = regions.get(r).get('minlon')
            x2 = regions.get(r).get('maxlon')
            y1 = regions.get(r).get('minlat')
            y2 = regions.get(r).get('maxlat')
            ax.add_patch(mpatches.Rectangle(xy=[x1,y1],width=x2-x1,height=y2-y1,facecolor='red',fill=False,edgecolor='black',lw=2.0,alpha=1.0,transform=proj))
            ty = y1+3.0 if y1 < 0.0 else y2-3.0
            verta = 'bottom' if y1 < 0.0 else 'top'
            ax.text(x1+5.0,ty,s=r,ha='left',va=verta)
    fig.suptitle(title)
    fig.tight_layout(rect=[0, 0.05, 1, 0.97])
    #fig.tight_layout()
    plt.savefig(ofile,bbox_inches='tight')
    #plt.savefig(ofile)
    plt.close()
    log.info('Figure written to '+ofile)
    return

