#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 18 13:48:18 2022

@author: naman
"""

import os
import re
import pandas as pd
import sqlalchemy as db

import warnings
warnings.filterwarnings("ignore")
#!pip install PyMySQL
#Restart Kernal

os.chdir('/Users/naman/Desktop/Autumn 2022 Quarter 1/Data Engineering Platforms/Final Project/Airplane Delay/raw_data')

engine = db.create_engine('mysql+pymysql://root:rootroot@34.121.37.33/flights',echo = True)
conn = engine.connect()

r_ontime = re.compile('ONTIME_REPORTING_.*')
files_ontime = list(filter(r_ontime.match,os.listdir()))

for file in files_ontime:
    df_ontime = pd.read_csv(file)
    df_ontime = df_ontime.loc[:, ~df_ontime.columns.str.contains('^Unnamed')]
    
    if file == files_ontime[0]:
        df_ontime['YEAR'] = 2019
        df_ontime.to_sql(name = 'ontime_reporting',con = conn, schema = 'flights', index = False, if_exists = 'replace')
        
    else:
 
        if '2020' in file:
            df_ontime['YEAR'] = 2020
        else:
            df_ontime['YEAR'] = 2019
            
        df_ontime.to_sql(name = 'ontime_reporting',con = conn, schema = 'flights', index = False, if_exists = 'append')


#df_ontime.to_sql(name = 'ontime_reporting',con = conn, schema = 'flights', index = False, if_exists = 'replace')

cols = ['STATION', 'NAME', 'DATE', 'AWND', 'PGTM', 'PRCP', 'PSUN', 'SNOW','SNWD','TAVG','TMAX','TMIN','TSUN']

df_weather_2020 = pd.read_csv('airport_weather_2020.csv')
df_weather_2020['DATE'] = pd.to_datetime(df_weather_2020['DATE']).dt.date
df_weather_2020 = df_weather_2020[cols]

df_weather_2019 = pd.read_csv('airport_weather_2019.csv')
df_weather_2019['DATE'] = pd.to_datetime(df_weather_2019['DATE']).dt.date
df_weather_2019 = df_weather_2019[cols]

df_weather = df_weather_2019.append(df_weather_2020)
df_weather.to_sql(name = 'weather',con = conn, schema = 'flights', index = False, if_exists = 'replace')


carrier_decode = pd.read_csv('CARRIER_DECODE.csv')
carrier_decode.to_sql(name = 'carrier_decode',con = conn, schema = 'flights', index = False, if_exists = 'replace')

airport_coordinates = pd.read_csv('AIRPORT_COORDINATES.csv')
carrier_decode.to_sql(name = 'airport_coordinates',con = conn, schema = 'flights', index = False, if_exists = 'replace')

airport_list = pd.read_csv('airports_list.csv')
airport_list.to_sql(name = 'aiport_list',con = conn, schema = 'flights', index = False, if_exists = 'replace')

employees = pd.read_csv('P10_EMPLOYEES.csv')
employees.to_sql(name = 'p10_employees',con = conn, schema = 'flights', index = False, if_exists = 'replace')



