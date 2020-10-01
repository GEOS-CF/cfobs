#!/usr/bin/env python
# ****************************************************************************
# plot_timeseries.py 
#
# DESCRIPTION: 
# Make a timeseries of CF vs. observations. 
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


# PARAMETER
MONTHSLABEL = ["J","F","M","A","M","J","J","A","S","O","N","D"]

def plot(orig_df,iday,obstype='o3',modvar=None,title=None,by='location',ofile='ts_!t_%Y%m%d.png',subtitle='!n, !latN !lonE',ylabel='!t',leglabel=None,nrow=4,ncol=4,hscale=5,vscale=5,sort_by_lat=True,**kwargs):
    '''
    Make timeseries of CF vs observation. 
    '''

    log = logging.getLogger(__name__)
    modvar = obstype if modvar is None else modvar
    df = orig_df.loc[orig_df['obstype']==obstype].copy()
    if df.shape[0] == 0:
        log.warning('No data of obstype {} found!'.format(obstype))
        return
    df['Month'] = [i.month for i in df['ISO8601']]
    panel_names = df[by].unique() 
    npanels = len(panel_names)
    # Eventually reorder panel list so that data is sorted by latitude
    if sort_by_lat:
        lats = []
        for p in panel_names:
            iidf = df.loc[df[by]==p]
            lats.append(iidf.lat.values.mean())
        panel_names = [x for _,x in sorted(zip(lats,panel_names),reverse=True)] 
    # Get number of figures 
    nfigures = np.int(np.ceil(np.float(npanels)/(ncol*nrow)))
    cnt = 0
    ylab = parse_vars(ylabel,obstype,modvar)
    ofile_template = parse_date(parse_vars(ofile,obstype,modvar),iday)
    for ifig in range(nfigures): 
        fig = plt.figure(figsize=(hscale*ncol,vscale*nrow))
        for i in range(ncol*nrow):
            idf = df.loc[df[by]==panel_names[cnt]]
            ilon  = idf.lon.values.mean()
            ilat  = idf.lat.values.mean()
            iname = str(panel_names[cnt])
            if type(subtitle)==type([]):
                isbtitle = subtitle[i]
            else:
                isbtitle = subtitle
            ititle = isbtitle.replace('!n',iname).replace('!lon','{0:.2f}'.format(ilon)).replace('!lat','{0:.2f}'.format(ilat))
            ax = fig.add_subplot(nrow,ncol,i+1)
            ax,l1,l2 = make_timeseries(ax,i,idf,ititle,ylab,**kwargs) 
            cnt+=1
            if cnt==npanels:
                break
        if title is not None:
            title = parse_vars(title,obstype,modvar) 
            title = parse_date(title,iday)
            fig.suptitle(title)
        leglabel = leglabel if leglabel is not None else ['CF','obs']
        fig.legend( [l1,l2], leglabel, 'lower center', ncol=2)
        #fig.legend( [l1,l2], ['CF','obs'], ncol=2)
        fig.tight_layout(rect=[0, 0.03, 1, 0.97])
        # add figure number
        if nfigures > 1:
            tmp = ofile_template.split('.')
            iofile = '.'.join(tmp[:-1])+'_fig{:02d}'.format(ifig+1)+'.'+tmp[-1]
        else:
            iofile = ofile_template
        plt.savefig(iofile,bbox_inches='tight')
        plt.close()
        log.info('Figure written to '+iofile)
    return


def make_timeseries(ax,i,idf,ititle,ylabel,xlabel=MONTHSLABEL,xticks_loc=None,modcol='conc_mod',obscol='conc_obs',minval=None,maxval=None,modcolor='black',obscolor='red',xoffset=0.1,filter=None,groupby_value='Month',ninset=None):
    '''Make the timeseries at the given axis.'''

    log = logging.getLogger(__name__)
    # remove 'outliers' if specified so
    if filter is not None:
        q_low = idf[obscol].quantile(1.0-filter)
        q_hi  = idf[obscol].quantile(filter)
        idf = idf.loc[(idf[obscol] < q_hi) & (idf[obscol] > q_low)] 
    # Get grouped values 
    grp = idf.groupby(groupby_value)
    mn = grp.mean().reset_index()
    sd = grp.std().reset_index()
    x1 = [i-xoffset for i in mn[groupby_value].values]
    x2 = [i+xoffset for i in mn[groupby_value].values]
    # select colors
    if type(modcolor)==type([]):
        modcolor = modcolor[np.mod(i,len(modcolor))]
    if type(obscolor)==type([]):
        obscolor = obscolor[np.mod(i,len(obscolor))]
    # make plot
    l1 = ax.errorbar(x=x1,y=mn[modcol],yerr=sd[modcol],fmt='-o',color=modcolor)
    l2 = ax.errorbar(x=x2,y=mn[obscol],yerr=sd[obscol],fmt='-o',color=obscolor)
    if xticks_loc is None:
        xticks_loc = np.arange(len(xlabel))+1
    _ = plt.xticks(xticks_loc,xlabel)
    if minval is not None:
        _ = ax.set_ylim(bottom=minval)
    if maxval is not None:
        _ = ax.set_ylim(top=maxval)
    _ = ax.set_ylabel(ylabel)
    _ = ax.set_title(ititle)
    if ninset is not None:
        if ninset in idf:
            ilabel = 'N={}'.format(len(idf[ninset].unique()))
            ax.text(0.02,0.98,ilabel,horizontalalignment='left',verticalalignment='top',transform=ax.transAxes)
        else:
            log.warning('Cannot show number in inset - column {} not found'.format(ninset))
    return ax,l1,l2
