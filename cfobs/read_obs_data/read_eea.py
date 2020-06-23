#!/usr/bin/env python
# ****************************************************************************
# read_eea.py 
#
# DESCRIPTION: 
# Reads AQ observation data from the European Environmental Agency (EEA) as
# obtained from https://discomap.eea.europa.eu/map/fme/AirQualityExport.htm 
#
# DATA SOURCE: EEA, https://discomap.eea.europa.eu/map/fme/AirQualityExport.htm
#
# HISTORY:
# 20200524 - christoph.a.keller at nasa.gov - Initial version 
# ****************************************************************************


# requirements
import logging
import os 
import argparse
import numpy as np
import datetime as dt
import pandas as pd
import pytz
import yaml
import glob
import requests
from dateutil import parser

from ..cfobs_save import save as cfobs_save


def read_eea(iday=None,idir=None,url_country=None,url_start_year=None,url_end_year=None,metafile=None,specs=['o3','no2','pm25'],stationsfile_local=None,ofile_local=None,ofile_local_append=False,ofile_local_merge=True,**kwargs):
    '''
    Read AQ observations from the EEA.
    '''
    log = logging.getLogger(__name__)
    if metafile is None:
        log.warning('Must specify metafile - return')
        return None
    # read metadata file
    meta = _read_meta(metafile)
    # dictionary of stations if specified so
    stations = None
    if stationsfile_local is not None:
        if os.path.isfile(stationsfile_local):
            with open(stationsfile_local,'r') as f:
                stations = yaml.load(f, Loader=yaml.FullLoader)
        else:
            stations = {}
    # read data for each species
    dfs = []
    for s in specs:
        if idir is not None:
            ifiles = glob.glob(idir.replace('%s',s))
        else:
            ifiles = _get_urls( spec=s, country=url_country, start_year=url_start_year, end_year=url_end_year )
        for ifile in ifiles:
            idf,locname,stations = _read_file(ifile,meta,stations,**kwargs)
            if idf is None:
                continue
            # add to list of dataframes
            dfs.append(idf)
            # write to individual files if specied so
            if ofile_local is not None:
                ofile = ofile_local.replace('%l',locname)
                if ofile_local_merge and os.path.isfile(ofile):
                    _merge_files(idf,ofile)
                else:
                    _ = cfobs_save(idf,ofile,iday,append=ofile_local_append)
    # write out stations if specified so
    if stationsfile_local is not None:
        with open(stationsfile_local,'w') as file:
            yaml.dump(stations, file)
        log.info('Written YAML file: {}'.format(stationsfile_local))
    # merge all data into one
    df = pd.concat(dfs,ignore_index=True) if len(dfs)>0 else None 
    return df


def _merge_files(idf,ofile):
    '''
    Read existing file and merge new data into it.
    '''
    log = logging.getLogger(__name__)
    odat = pd.read_csv(ofile,parse_dates=['ISO8601','localtime'],date_parser=lambda x: pd.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ'))
    idf = idf[list(odat.keys())]
    odat = odat.append(idf)
    refdate = dt.datetime(2018,1,1,0,0,0)
    odat['localtime'] = [(i-refdate).total_seconds()/60.0 for i in odat['localtime']]
    odat = odat.groupby(['ISO8601','obstype']).mean().reset_index()
    odat['localtime'] = [(refdate+dt.timedelta(minutes=i)) for i in odat['localtime']]
    for v in ['original_station_name','country','station_type','unit']:
        odat[v] = [idf[v].values[0] for i in range(odat.shape[0])]
    odat = odat[['ISO8601','localtime','original_station_name','lat','lon','country','station_type','obstype','unit','value']]
    odat = odat.sort_values(by="ISO8601")
    odat.to_csv(ofile,mode='w+',date_format='%Y-%m-%dT%H:%M:%SZ',index=False,header=True,float_format='%.4f')
    log.info('Merged new data into file: {}'.format(ofile))
    return


def _get_urls(spec='o3',country=None,start_year=None,end_year=None):
    '''
    Get links to csv files for a given country, species, and the specified time period.
    '''
    log = logging.getLogger(__name__)
    if country is None:
        log.error('Must provide country ID')
        return []
    specid = None
    if spec=='no2': 
        specid = '8'
    if spec=='o3': 
        specid = '7'
    if spec=='pm25': 
        specid = '6001'
    if specid is None:
        log.error('Invalid specid: {}'.format(spec))
        return []
    year_start = str(dt.datetime.today().year) if start_year is None else str(start_year)
    year_end = str(dt.datetime.today().year) if end_year is None else str(end_year)
    url='https://fme.discomap.eea.europa.eu/fmedatastreaming/AirQualityDownload/AQData_Extract.fmw?CountryCode='+country+'&CityName=&Pollutant='+specid+'&Year_from='+year_start+'&Year_to='+year_end+'&Station=&Samplingpoint=&Source=All&Output=TEXT&UpdateDate=&TimeCoverage=Year'
    log.info('Parsing: {}'.format(url))
    r = requests.get(url)
    candidate_urls = r.text.split('.csv')
    r.close()
    urls = []
    for u in candidate_urls:
        if 'https://' not in u:
            continue
        core = u.split('https://')[1]
        if 'ereporting.blob.core' in core:
            istr = 'https://'+core+'.csv'
            urls.append(istr)
    return urls


def _read_file(ifile,meta,stations,time_offset=0,firstday=None,lastday=None):
    '''Read a single file.'''
    log = logging.getLogger(__name__)
    locname = 'unknown'
    if not os.path.exists(ifile) and 'https' not in ifile:
        log.warning('file not found: {}'.format(ifile))
        return None,locname,stations
    log.info('Reading {}'.format(ifile))
    origtb = pd.read_csv(ifile,sep=",") ##,encoding="ISO-8859-1")
    # data should be for one station only:
    all_stations = list(origtb.SamplingPoint.unique())
    if len(all_stations)>1:
        a = origtb.groupby('SamplingPoint').count().reset_index()
        istation = a.loc[a['Concentration']==a['Concentration'].values.max(),'SamplingPoint'].values[0]
        tbnew = origtb.loc[origtb['SamplingPoint']==istation].copy()
        origtb = tbnew.copy()
        log.warning('More than one station found in file {} - will only read data for location with most values ({})'.format(ifile,istation))
    # only read valid entries
    tb = origtb.loc[~np.isnan(origtb['Concentration'].values)].copy()
    del(origtb)
    if tb.shape[0]==0:
        log.warning('No valid concentration entries found in {}'.format(ifile))
        return None,locname,stations
    # get meta data for that station
    thisstation = tb.SamplingPoint.unique()[0]
    istat = meta.loc[meta['SamplingPoint']==thisstation]
    if istat.shape[0]==0:
        log.warning('No meta data found for sampling point: {} - cannot read data')
        return None,locname,stations
    locname = str(istat['AirQualityStation'].values[0].replace('STA.','').replace('STA-','').replace('STA_',''))
    loclat  = np.round(np.float(istat['Latitude'].values[0]),4)
    loclon  = np.round(np.float(istat['Longitude'].values[0]),4)
    loctype = istat['AirQualityStationType'].values[0]
    loccountry = istat['Countrycode'].values[0]
    locname = '_'.join(('EEA',loccountry,locname))
    # read dates
    offset = dt.timedelta(minutes=time_offset)
    utc = pytz.utc
    df = pd.DataFrame()
    df['localtime'] = [parser.parse(i) for i in tb['DatetimeBegin']]
    df['ISO8601'] = [i.astimezone(utc)+offset for i in df['localtime']]
    ns = 1.0e-9
    df['ISO8601'] = [dt.datetime.utcfromtimestamp(i.astype(int)*ns) for i in df['ISO8601'].values]
    df['localtime'] = [parser.parse(i,ignoretz=True) for i in tb['DatetimeBegin']]
    # add station information
    df['original_station_name'] = [locname for i in range(df.shape[0])]
    df['lat'] = [loclat for i in range(df.shape[0])]
    df['lon']= [loclon for i in range(df.shape[0])]
    df['country']= [loccountry for i in range(df.shape[0])]
    df['station_type']= [loctype for i in range(df.shape[0])]
    # add observations
    df['obstype'] = tb['AirPollutant'].values
    df['unit'] = tb['UnitOfMeasurement'].values
    df['value'] = tb['Concentration'].values
    # replace with 'standard' values
    df.loc[df['unit']==u'\u00b5g/m3','unit'] = 'ugm-3'
    df.loc[df['obstype']=='NO2','obstype'] = 'no2'
    df.loc[df['obstype']=='O3','obstype'] = 'o3'
    df.loc[df['obstype']=='PM2.5','obstype'] = 'pm25'
    # filter by days
    if firstday is not None:
        #firstday_tzaware = dt.datetime(firstday.year,firstday.month,firstday.day,tzinfo=pytz.utc)
        log.info('Only use data after {}'.format(firstday))
        df = df.loc[df['ISO8601'] >= firstday]
    if lastday is not None:
        #lastday_tzaware = dt.datetime(lastday.year,lastday.month,lastday.day,tzinfo=pytz.utc)
        log.info('Only use data before {}'.format(lastday))
        df = df.loc[df['ISO8601'] < lastday]
    # sort data
    df = df.sort_values(by="ISO8601")
    # eventually update stations entry
    if stations is not None:
        if locname not in stations:
            stations[locname] = {'country':loccountry,'lat':'{:.4f}'.format(loclat),'lon':'{:.4f}'.format(loclon)}
    return df,locname,stations


def _read_meta(metafile):
    '''Read meta data'''
    log = logging.getLogger(__name__)
    log.info('Reading metadata from {}'.format(metafile))
    meta = pd.read_csv(metafile,sep='\t')
    return meta
