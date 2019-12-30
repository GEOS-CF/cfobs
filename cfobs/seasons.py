#!/usr/bin/env python
# ****************************************************************************
# seasons.py 
#
# DESCRIPTION: 
# Controls season definition and selection. 
# 
# HISTORY:
# 20191222 - christoph.a.keller at nasa.gov - Initial version
# ****************************************************************************
import datetime as dt
import pandas as pd


season_lookup_table = {
 1:"DJF",
 2:"MAM",
 3:"JJA",
 4:"SON"
}

def set_season(df):
    '''
    Set the season for the provided data frame. The season numbers are:
    1 = DJF 
    2 = MAM
    3 = JJA
    4 = SON
    '''
    # write out season for each entry
    df['season'] = [(i.month%12+3)//3 for i in df['ISO8601']]
    return df


def get_season_name(i):
    '''Get the season abbrevation based on the season number.'''
    return season_lookup_table.get(i%4)

def reduce_data_to_season(df,season_number):
    '''Reduce the data frame to the season provided in the input argument.'''
    if 'season' not in df.keys():
        df = df.set_season(df)
    return df.loc[df['season']==season_number].copy()
