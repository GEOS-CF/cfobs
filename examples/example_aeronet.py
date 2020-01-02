#!/usr/bin/env python
# ****************************************************************************
# Do a comparison between GEOS-CF and OpenAQ for June 1, 2019 - June 7, 2019. 
#
# USAGE:
# python example_aeronet.py 
#
# HISTORY:
# 20191230 - christoph.a.keller at nasa.gov - Initial version 
# ****************************************************************************

# requirements
import sys
import numpy as np
import datetime as dt
import os
import pandas as pd

# import CFtools
sys.path.insert(1,'../')
import cfobs.cfobs as cfobs 
import cfobs.systools as systools

# settings
ofile_csv     = 'aeronet_%Y%m.csv'
ofile_png     = 'aeronet_%Y%m_!k_!v.png'
configfile    = 'config/mapping_aeronet.yaml'
regionsfile   = 'config/regions.yaml'

# for map plot
modvars = ['aod550_bc','aod550_oc','aod550_dust','aod550_sulfate','aod550_sala','aod550_salc']
mapfiles = 'https://opendap.nccs.nasa.gov/dods/gmao/geos-cf/assim/xgc_tavg_1hr_g1440x721_x1'

def main(args):
    '''
    Read daily averaged AERONET data for one month and corresponding
    GEOS-CF output. 
    Save that data frame as csv table and also make some plots of the
    model-observation comparisons.
    '''
    # Read AERONET data 
    startday = dt.datetime(2019,6,1)
    endday = dt.datetime(2019,6,7)
    cfob = cfobs.CFObs( verbose=1 )
    cfob.read_obs( obskey='aeronet', startday=startday, read_freq=None, end=endday )

    # Merge with CF data and add region information
    cfob.add_cf( configfile=configfile )
    cfob.update_regions( regionsfile=regionsfile )

    # Save to file
    #cfob.save( ofile=ofile_csv, append=0, nfloats=4 )

    # Plot
    cfob.plot(plotkey='map',obstype='aod550',ofile=ofile_png,mapfiles=mapfiles,modvar=modvars,title='Aeronet vs. GEOS-CF (June 1-7, 2019)',maplabel='AOD 550nm [unitless]',maxval=1.0,maxbias=0.5)
    cfob.plot(plotkey='boxplot',obstype='aod550',ofile=ofile_png,regionsfile=regionsfile,title='GEOS-CF - Aeronet (June 1-7, 2019)',ylabel='AOD at 550nm, Model - Aeronet',minval=0.0,maxval=1.0)

    return
    

if __name__ == "__main__":
    main()
