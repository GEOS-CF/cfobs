#!/usr/bin/env python
# ****************************************************************************
# parse_string.py 
# 
# DESCRIPTION:
# Routines to parse strings 

# HISTORY:
# 20191220 - christoph.a.keller at nasa.gov - Initial version 
# ****************************************************************************
import datetime as dt

def parse_date(string,this_datetime):
    '''Parse dates for the given string'''
    return this_datetime.strftime(string) 

def parse_vars(string,type,var):
    '''Parse variable pattern for the given string'''
    return string.replace('!t',type).replace('!v',var)

def parse_key(string,key):
    '''Parse key pattern for the given string'''
    return string.replace('!k',key)
