#!/usr/bin/env python
# ****************************************************************************
# read_dos.py 
#
# DESCRIPTION: 
# Reads AQ observation data from the Department of State (at embassy stations)
# and converts it to a csv table.
#
# DATA SOURCE:
# 
#
# HISTORY:
# 20191018 - christoph.a.keller at nasa.gov - Initial version 
# ****************************************************************************


# requirements
import logging
import os 
import argparse
import numpy as np
import datetime as dt
import pandas as pd


def read_dos(iday=None,ifile=None,firstday=None,lastday=None,time_offset=0):
    '''
    Read AQ observations from the Department of State.
    '''
    log = logging.getLogger(__name__)
    # read configuration file
    if not os.path.exists(ifile):
        log.warning('file not found: {}'.format(ifile))
        return None
    log.info('Reading {}'.format(ifile))
    tb = pd.read_csv(ifile,sep=",") ##,encoding="ISO-8859-1")
    # get dates
    offset = dt.timedelta(minutes=time_offset)
    tb['ISO8601'] = [dt.datetime.strptime(i,"%m/%d/%Y %H:%M")+offset for i in tb['date_GMT']]
    # subset data if selected so
    if firstday is not None:
        log.info('Only use data after {}'.format(firstday))
        tb = tb.loc[tb['ISO8601'] >= firstday]
    if lastday is not None:
        log.info('Only use data before {}'.format(lastday))
        tb = tb.loc[tb['ISO8601'] < lastday]
    # write to output frame
    df = pd.DataFrame()
    df['ISO8601'] = tb['ISO8601']
    df['localtime'] = [dt.datetime.strptime(" ".join(i.split(" ")[0:2]),"%m/%d/%Y %H:%M")+offset for i in tb['date_and_timezone']]
    df['timezone'] = [i.split(" ")[2] for i in tb['date_and_timezone']]
    # add station information
    df['original_station_name'] = tb['Site']
    df['lat'] = tb['Latitude']
    df['lon'] = tb['Longitude']
    # add observations
    nrow = tb.shape[0]
    df['obstype'] = ['pm25' for x in range(nrow)]
    df['unit'] = ['ugm-3' for x in range(nrow)]
    df['value'] = tb['PM2.5_conc_ugperm3']
    df['AQI'] = tb['AQI_value']
    # sort data
    df = df.sort_values(by="ISO8601")
    # strip empty spaces
    df['original_station_name'] = [i.replace(" ","") for i in df['original_station_name']]
    return df
