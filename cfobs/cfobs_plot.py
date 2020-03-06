#!/usr/bin/env python
# ****************************************************************************
# cfobs_plot.py 
# 
# DESCRIPTION:
# Parent routine to plot observations against model data. This is a wrapper
# function for the individiual plotting routines. 
# 
# HISTORY:
# 20191220 - christoph.a.keller at nasa.gov - Initial version 
# ****************************************************************************
import numpy as np
import datetime as dt
import pandas as pd

import cfobs.plot.plot_map as pltmp 
import cfobs.plot.plot_boxplot as bxplt 
import cfobs.plot.plot_timeseries as ts 
from .parse_string import parse_key 


# define plot functions here 
plot_functions = {
        "map": pltmp.plot,
        "boxplot": bxplt.plot,
        "timeseries": ts.plot,
        }


def plot(df,plotkey,iday,ofile='figures/figure_!k_%Y%m%d.csv',**kwargs):
    '''
    Plot handler. This is a wrapper that calls down to the individual plot functions 
    '''
    # check if requested observation key exists in funcs and get function for it
    assert(plotkey in plot_functions), 'Invalid plottype key: {}'.format(plotkey)
    plotfunc = plot_functions.get(plotkey)
    if df is not None:
        _ = plotfunc(df,iday,ofile=parse_key(ofile,plotkey),**kwargs)
    return 
