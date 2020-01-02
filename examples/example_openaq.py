#!/usr/bin/env python
# ****************************************************************************
# Do a comparison between GEOS-CF and OpenAQ for Dec 1, 2019. 
#
# USAGE: 
# python example_openaq.py 
#
# HISTORY:
# 20191220 - christoph.a.keller at nasa.gov - Initial version 
# ****************************************************************************

# requirements
import argparse
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
openaq_json   = 'data/openaq.%Y-%m-%d.ndjson'
mapfiles      = 'https://opendap.nccs.nasa.gov/dods/gmao/geos-cf/assim/chm_tavg_1hr_g1440x721_v1' 
ofile_csv     = 'out/data/openaq_%Y-%m_%d.csv'
ofile_png     = 'out/figures/openaq_%Y%m%d_!k_!v.png'
configfile    = 'config/mapping_openaq.yaml'
regionsfile   = 'config/regions.yaml'

def get_mapplotspecs(args):
    '''Specify map plot specifications for all species'''
    mapplotspecs = {}
    mapplotspecs['o3'] = set_mapplotspecs('o3','o3',1.0e9,50.0,0.0,80.0,'Surface O$_{3}$ [ppbv]','IOA','Ozone (%Y-%m-%d)')
    mapplotspecs['no2'] = set_mapplotspecs('no2','no2',1.0e9,25.0,0.0,75.0,'Surface NO$_{2}$ [ppbv]','IOA','Nitrogen dioxide (%Y-%m-%d)')
    mapplotspecs['pm25_gcc'] = set_mapplotspecs('pm25','pm25_rh35_gcc',1.0,50.0,0.0,80.0,'Surface PM2.5 [$\mu$gm$^{-3}$]','IOA','PM2.5 from GEOS-Chem (%Y-%m-%d)')
    mapplotspecs['pm25_gocart'] = set_mapplotspecs('pm25','pm25_rh35_gocar',1.0e9,50.0,0.0,80.0,'Surface PM2.5 [$\mu$gm$^{-3}$]','IOA','PM2.5 from GOCART (%Y-%m-%d)')
    return mapplotspecs

def set_mapplotspecs(obstype,modvar,modvarscal,maxbias,minval,maxval,maplabel,stat,title,modcol='conc_mod',ofile=ofile_png,mapfiles=mapfiles,dotedgecolor='grey',dotsize=10):
    '''Define map plot settings for a species.'''
    idict = {'obstype':obstype,'modvar':modvar,'modvarscal':modvarscal,'maxbias':maxbias,'minval':minval,'maxval':maxval,'maplabel':maplabel,'statistic':stat,'title':title,'modcol':modcol,'ofile':ofile,'mapfiles':mapfiles}
    return idict 

def get_boxplotspecs(args):
    '''Specify boxplot specifications'''
    boxplotspecs = {}
    boxplotspecs['o3'] = set_boxplotspecs('o3',-50.0,50.0,'Ozone (%Y-%m-%d)','Surface O$_{3}$ bias [ppbv]')
    boxplotspecs['no2'] = set_boxplotspecs('no2',-50.0,50.0,'Nitrogen dioxide (%Y-%m-%d)','Surface NO$_{2}$ bias [ppbv]')
    boxplotspecs['pm25_gcc'] = set_boxplotspecs('pm25',-100.0,100.0,'PM2.5 from GEOS-Chem (%Y-%m-%d)','Surface PM2.5 bias [$\mu$gm$^{-3}$]',modvar='pm25_rh35_gcc')
    boxplotspecs['pm25_gocart'] = set_boxplotspecs('pm25',-100.0,100.0,'PM2.5 from GOCART (%Y-%m-%d)','Surface PM2.5 bias [$\mu$gm$^{-3}$]',modcol='conc_mod_gocart',modvar='pm25_rh35_gocar')
    return boxplotspecs

def set_boxplotspecs(obstype,minval,maxval,title,ylabel,plot_by_season=0,plot_by_region=1,regionsfile=regionsfile,stat='bias',modcol='conc_mod',aggregate_by_location=0,ofile=ofile_png,modvar=None):
    '''Define boxplot settings for a species'''
    idict = {'obstype':obstype,'minval':minval,'maxval':maxval,'title':title,'ylabel':ylabel,'plot_by_season':plot_by_season,'plot_by_region':plot_by_region,'regionsfile':regionsfile,'statistic':stat,'modcol':modcol,'aggregate_by_location':aggregate_by_location,'ofile':ofile,'modvar':modvar}
    return idict


def main(args):
    '''
    Read daily OpenAQ data and add corresponding CF output to it. Then
    save that data frame as csv table and also mamke some plots of the
    model-observation comparisons.
    '''
    iday = dt.datetime(2019,12,1)
    cfob = cfobs.CFObs( verbose=1 )
    # Read OpenAQ data and get data frame 
    cfob.read_obs( obskey='openaq', startday=iday, json_tmpl=openaq_json )

    # Merge OpenAQ data with CF data
    cfob.add_cf( configfile=configfile )

    # Add regions information
    cfob.update_regions( regionsfile=regionsfile )

    # Save to file
    if args.save_csv==1:
        systools.check_dir(ofile_csv,iday)
        cfob.save( ofile=ofile_csv, append=False, nfloats=4 )

    # Make plots
    if args.plot == 1:
        systools.check_dir(ofile_png,iday)
        # Map plot
        if args.mapplot==1:
            mapplotspecs = get_mapplotspecs(args)
            for ispec in mapplotspecs:
                cfob.plot( plotkey='map',
                           **mapplotspecs[ispec])
        # Boxplot 
        if args.boxplot==1:
             boxplotspecs = get_boxplotspecs(args)
             for ispec in boxplotspecs:
                 cfob.plot( plotkey='boxplot',
                            **boxplotspecs[ispec])
    return
    

def parse_args():
    p = argparse.ArgumentParser(description='Undef certain variables')
    p.add_argument('-s','--save-csv',type=int,help='save data to file for faster reloading lateron',default=0)
    p.add_argument('-p','--plot',type=int,help='make plot',default=1)
    p.add_argument('-mp','--mapplot',type=int,help='make map plot?',default=1)
    p.add_argument('-bp','--boxplot',type=int,help='make box plot?',default=1)
    return p.parse_args()


if __name__ == "__main__":
    main(parse_args())
