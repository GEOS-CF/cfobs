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


def plot(df,iday,types=None,typeby='obstype',groupby='original_station_name',labels=None,
         markers='o',ofile='locations.png',title='Observation locations',**kwargs):
    '''Make a map with the locations of all measurement sites.'''

    log = logging.getLogger(__name__)
    fig = plt.figure(figsize=(5,3))
    gs  = GridSpec(1,1)
    proj = ccrs.PlateCarree()
    ax = fig.add_subplot(gs[0,0],projection=proj)
    types = list(df[typeby].unique()) if types is None else types
    orig_labels = []
    handles = []
    for cnt,t in enumerate(types):
        idf = df.loc[df[by]==t].groupby(groupby).mean()
        imarker = markers[cnt] if type(markers)==type([]) else markers
        _ = ax.scatter(x=idf.lon.values,y=idf.lat.values,c=colors[cnt],marker=imarker,**kwargs)
        #handles.append(sc1)
        orig_labels.append(t)
#    _ = ax.add_feature(cartopy.feature.BORDERS, edgecolor="grey")
#    _ = ax.add_feature(cartopy.feature.COASTLINE, edgecolor="black")
    _ = ax.stock_img()
    _ = ax.coastlines()
    labels = labels if labels is not None else orig_labels
    fig.legend( labels=labels, loc='upper  left', ncol=len(labels), bbox_to_anchor=(1,0) )
    fig.suptitle(title)
    fig.tight_layout(rect=[0, 0.03, 1, 0.97])
    plt.savefig(ofile,bbox_inches='tight')
    plt.close()
    log.info('Figure written to '+ofile)
    return

