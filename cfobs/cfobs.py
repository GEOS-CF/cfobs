#!/usr/bin/env python
# ****************************************************************************
# cfobs.py 
# 
# DESCRIPTION:
# Contains the definition of the cfobs object, which compares observations 
# agains GEOS-CF model output.
# 
# HISTORY:
# 20191216 - christoph.a.keller at nasa.gov - Initial version 
# ****************************************************************************
import os
import numpy as np
import pandas as pd

from .cfobs_load import load as cfobs_load
from .cfobs_save import save as cfobs_save
from .cfobs_plot import plot as cfobs_plot
from .read_obs import read_obs
from .regions import set_regions
from .add_cf_to_obs import add_cf as addcf 


class CFObs(object):
    '''
    Compare observations against GEOS-CF output.
    
    :param ifile: str, file name to read from
    :param verbose: int, verbose mode
    '''
    def __init__(self, ifile=None, **kwargs):
        self._startday = None 
        self._endday   = None 
        self._data     = pd.DataFrame()
        if ifile is not None:
            self.load(ifile,**kwargs)


    def load(self, ifile, **kwargs):
        '''Load previously saved data from a csv file.'''
        self.__delete()
        self.add(ifile, **kwargs)


    def add(self, file_template, **kwargs):
        '''Add previously saved data from a csv file to existing data.'''
        data, startday, endday = cfobs_load(file_template,**kwargs)
        self._data = self._data.append(data)
        self._startday = startday if self._startday is None else min(startday,self._startday)
        self._endday   = endday if self._endday is None else max(endday,self._endday)


    def __delete(self):
        '''Delete data from the object.'''
        del self._data
        self._data = pd.DataFrame()
        self._startday = None 
        self._endday   = None 


    def save(self, ofile, **kwargs):
        '''Save data to a csv file.'''
        _ = cfobs_save(self._data, ofile, self._endday, **kwargs)


    def read_obs(self, obskey, startday, endday=None, **kwargs):
        '''Read observation data from original data source.''' 
        self._data = read_obs(obskey, startday, endday, **kwargs)
        self._startday = startday
        self._endday = endday if endday is not None else startday 


    def add_cf(self, **kwargs):
        '''Add CF information to the data frame.'''
        self._data = addcf(self._data, **kwargs)


    def update_regions(self, regionsfile):
        '''Update regions information based on information in the regionsfile.'''
        self._data = set_regions(self._data, None, regionsfile)


    def plot(self, plotkey, **kwargs):
        '''Make a plot of the data.'''
        cfobs_plot(self._data, plotkey, self._endday, **kwargs)

