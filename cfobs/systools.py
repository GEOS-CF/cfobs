#!/usr/bin/env python
# ****************************************************************************
# systools.py 
#
# DESCRIPTION: 
# Collection of system tools. 
# 
# HISTORY:
# 20191211 - christoph.a.keller at nasa.gov - Initial version
# ****************************************************************************
import os

from .parse_string import parse_date


def check_dir(full_file_name,idate):
    '''Check if directory exists, creates it if needed'''
    odir = '/'.join(full_file_name.split('/')[:-1])
    odir = parse_date(odir,idate)
    if not os.path.isdir(odir):
        os.makedirs(odir)
    return
