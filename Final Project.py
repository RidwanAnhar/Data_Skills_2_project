# Data Skills 2
# Autumn 2020
#
#           RIDWAN ANHAR - 12244795 - RANHAR@UCHICAGO.EDU
# Homework 4
#
#
#
import pandas as pd
import pandas_datareader.data as web
import os
import datetime
import requests
import seaborn as sns
import us
import numpy as np

#Importing Dataset from FRED
#Automating import dataset from FRED by using us library to create
#FRED specified series id.
gdp = []
rhousehold_inc = []
home_rate = []
unemp_rate = []
state = us.states.mapping('fips', 'abbr')

def get_series():
    '''create a function to create FRED series_id using us library fips and abbreviation
    and remove unnecessary data
    '''
    fips_remove = ['60','66','69','72','78',None]
    for fips in fips_remove:
        state.pop(fips)
    for fips,abbr in state.items():
        gdp.append(abbr + 'NGSP')
        rhousehold_inc.append('MEHOINUS' + abbr + 'A672N')
        home_rate.append(abbr + 'HOWN')
    for fips,abbr in state.items():
        unemp_rate.append('LAUST' + fips + '0000000000003A')

def get_fred(series_id):
    '''automation import economic indicators from fred using
    economic percentage growth (YoY from previous 1 year) in 2008 and 2012
    '''
    start = datetime.date(year=2007, month=1,  day=1)
    end   = datetime.date(year=2012, month=12, day=31)
    series = series_id
    source = 'fred'
    df = web.DataReader(series, source, start, end)
    df.reset_index(inplace = True)
    df = df.drop(df.index[[2,3]])
    new_df = df.set_index('DATE')
    if series_id == unemp_rate:
        df_pct = new_df.diff() #unemp_rate is already in percentage
    else:
        df_pct = new_df.pct_change(1)*100
    # cite:https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.pct_change.html

    df_fix = df_pct.drop(df_pct.index[[0,2]])
    df_fix.reset_index(inplace = True)
    df_fin = df_fix.rename(columns={'DATE': 'year'})
    df_fin = df_fin.melt(id_vars=['year'], var_name='state')
    df_fin['year'] = df_fin['year'].apply(lambda x: x.strftime('%Y'))
    #convert datetime into a string so it easier to merge with presidential election data
    #cite: https://stackoverflow.com/questions/19738169/convert-column-of-date-objects-in-pandas-dataframe-to-strings/33967346

    return df_fin

def merge_df():
    '''clean and merge economic indicators data
    '''
    gdp_df = get_fred(gdp)
    gdp_df = gdp_df.replace('NGSP', '', regex=True)
    gdp_df = gdp_df.rename(columns={'value': 'gdp_yoy%'})

    rhi_df = get_fred(rhousehold_inc)
    rhi_df = rhi_df.replace('MEHOINUS', '', regex=True)
    rhi_df = rhi_df.replace('A672N', '', regex=True)
    rhi_df = rhi_df.rename(columns={'value': 'rhi_yoy%'})

    hr_df = get_fred(home_rate)
    hr_df = hr_df.replace('HOWN', '', regex=True)
    hr_df = hr_df.rename(columns={'value': 'hr_yoy%'})

    ur_df = get_fred(unemp_rate)
    ur_df = ur_df.replace('LAUST', '', regex=True)
    ur_df = ur_df.replace('0000000000003A', '', regex=True)
    ur_df = ur_df.replace({"state": state})
    ur_df = ur_df.rename(columns={'value': 'ur_yoy%'})

    df1 = pd.merge(gdp_df, rhi_df, on = ['year','state'], how = 'outer')
    df2 = pd.merge(df1, hr_df, on = ['year','state'], how = 'outer')
    merge_df = pd.merge(df2, ur_df, on = ['year','state'], how = 'outer')
    merge_df = merge_df.sort_values(by=['year','state'])

    return merge_df

#Calling importing and merging function
get_series()
econ_df = merge_df()
econ_df
