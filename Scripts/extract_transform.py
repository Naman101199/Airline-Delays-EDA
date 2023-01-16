#aircraft -> info about each carriers flight
#carrier -> info about each carrier
#on_time_reporting -> info about all flight timings and delays
#weather -> weather details for each airport
#airport -> info about all airports
#location -> info about city and state

import os
import re
import pandas as pd
import zipfile
import numpy as np

import warnings
warnings.filterwarnings("ignore")

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/Users/naman/Downloads/dogwood-abbey-364703-1289c40bd8d0.json'

os.chdir('/Users/'+ os.getlogin() + '/Desktop/DataEngineeringPlatforms')

# =============================================================================
# Extract
# =============================================================================
zf = zipfile.ZipFile('raw_data.zip') 

airport_coordinates = pd.read_csv(zf.open('AIRPORT_COORDINATES.csv'), usecols = ['ORIGIN_AIRPORT_ID', 'DISPLAY_AIRPORT_NAME', 'LATITUDE','LONGITUDE'])
airport_list = pd.read_csv(zf.open('airports_list.csv'), usecols = ['ORIGIN_AIRPORT_ID', 'DISPLAY_AIRPORT_NAME', 'ORIGIN_CITY_NAME','NAME'])
airport_inventory = pd.read_csv(zf.open('B43_AIRCRAFT_INVENTORY.csv'), encoding = 'windows-1252')
airline = pd.read_csv(zf.open('CARRIER_DECODE.csv'))

# =============================================================================
# Preprocess (Transform and Normalize)
# =============================================================================


# =============================================================================
# Merging all Ontime Reporting csvs
# =============================================================================
r_ontime = re.compile('ONTIME_REPORTING_.*')
files_ontime = list(filter(r_ontime.match,zf.namelist()))

ontime_repoting = pd.DataFrame()
for file in files_ontime:
    print(file)
    df_ontime = pd.read_csv(zf.open(file), usecols = ['MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK', 'ORIGIN_AIRPORT_ID', 'DEST_AIRPORT_ID','OP_UNIQUE_CARRIER','TAIL_NUM','OP_CARRIER_FL_NUM',
                                            'CRS_DEP_TIME', 'DEP_TIME', 'CRS_ARR_TIME', 'ARR_TIME', 'CANCELLED', 'DISTANCE',
                                            'CARRIER_DELAY', 'WEATHER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY', 'SECURITY_DELAY'])
   
    if file == files_ontime[0]:
        df_ontime['YEAR'] = 2019
        ontime_repoting = ontime_repoting.append(df_ontime)
    else:
 
        if '2020' in file:
            df_ontime['YEAR'] = 2020
        else:
            df_ontime['YEAR'] = 2019
        ontime_repoting = ontime_repoting.append(df_ontime)


# =============================================================================
# Merging Weather Data
# =============================================================================
cols = ['STATION', 'NAME', 'DATE', 'AWND', 'PGTM', 'PRCP', 'TSUN', 'SNOW','SNWD','TAVG','TMAX','TMIN']
weather_type = ['WT' + str('%02d' % i) for i in range(1, 11)]
cols = cols + weather_type

df_weather_2020 = pd.read_csv(zf.open('airport_weather_2020.csv'), usecols = cols)
df_weather_2020['DATE'] = pd.to_datetime(df_weather_2020['DATE']).dt.date
df_weather_2020 = df_weather_2020[cols]

df_weather_2019 = pd.read_csv(zf.open('airport_weather_2019.csv'), usecols = cols)
df_weather_2019['DATE'] = pd.to_datetime(df_weather_2019['DATE']).dt.date
df_weather_2019 = df_weather_2019[cols]

df_weather = df_weather_2019.append(df_weather_2020)

sep = ' AIRPORT'
df_weather['NAME'] = df_weather['NAME'].str.split(sep,1).apply(lambda x: x[0].title())

# =============================================================================
# #Location -> location_id,city,state
# =============================================================================
location = airport_list['ORIGIN_CITY_NAME'].str.split(',',expand = True).drop_duplicates().reset_index().drop_duplicates()
location.columns = ['LOCATION_ID','CITY','STATE']
location['LOCATION_ID'] = location['LOCATION_ID'] + 1

# =============================================================================
# #airport -> aiport_id, airport_name, location_id, lat, lon
# =============================================================================
airport_coordinates = airport_coordinates.groupby('ORIGIN_AIRPORT_ID').first().drop_duplicates().reset_index()

airport = airport_coordinates.merge(airport_list, 
                                    how = 'inner', 
                                    on = ['ORIGIN_AIRPORT_ID','DISPLAY_AIRPORT_NAME']).drop_duplicates()

airport[['CITY','STATE']] = airport['ORIGIN_CITY_NAME'].str.split(',',expand = True)
airport = airport.merge(location, on = ['CITY','STATE'])[['ORIGIN_AIRPORT_ID','DISPLAY_AIRPORT_NAME','LOCATION_ID','LATITUDE','LONGITUDE']]
airport.columns = ['AIRPORT_ID','AIRPORT_NAME','LOCATION_ID','LATITUDE','LONGITUDE']

# =============================================================================
# #airline -> op_unique_carrier, carrier_name
# =============================================================================
airline = airline[['AIRLINE_ID','UNIQUE_CARRIER','UNIQUE_CARRIER_NAME']].drop_duplicates()

# =============================================================================
# #aircraft
# =============================================================================
aircraft = ontime_repoting[['OP_UNIQUE_CARRIER','OP_CARRIER_FL_NUM']].drop_duplicates().reset_index(drop = True).reset_index()
aircraft = aircraft.merge(airline, left_on = 'OP_UNIQUE_CARRIER',right_on = 'UNIQUE_CARRIER', how = 'left').drop_duplicates()
aircraft = aircraft[['index','AIRLINE_ID','OP_CARRIER_FL_NUM','UNIQUE_CARRIER']]
aircraft.columns = ['AIRCRAFT_ID','AIRLINE_ID','OP_CARRIER_FL_NUM','UNIQUE_CARRIER']
aircraft['AIRCRAFT_ID'] = aircraft['AIRCRAFT_ID'] + 1

# =============================================================================
# Creating flight merged data
# =============================================================================
ontime_repoting = ontime_repoting.rename(columns = {'DAY_OF_MONTH':'DAY'})
ontime_repoting['DATE'] = pd.to_datetime(ontime_repoting[['YEAR','MONTH','DAY']])

ontime_repoting = ontime_repoting.drop(['MONTH','DAY','DAY_OF_WEEK','YEAR'],axis = 1)

ontime_repoting_merged = ontime_repoting.merge(aircraft, 
                                               left_on = ['OP_UNIQUE_CARRIER', 'OP_CARRIER_FL_NUM'],
                                               right_on = ['UNIQUE_CARRIER', 'OP_CARRIER_FL_NUM'])

ontime_repoting_merged = ontime_repoting_merged.drop(['OP_UNIQUE_CARRIER', 'TAIL_NUM', 'OP_CARRIER_FL_NUM','AIRLINE_ID','UNIQUE_CARRIER'], axis = 1)

ontime_repoting_merged = ontime_repoting_merged.sort_values(['DATE','AIRCRAFT_ID','DEP_TIME']).reset_index(drop = True).reset_index().rename(columns = {'index':'ID'})


def getTime(t):
    if np.isnan(t):
        return np.nan
    else:
        t = str(int(t))
        if t == '2400':
            return '00:00'
        elif len(t) == 4:
            return t[:2] + ':' + t[2:]
        elif len(t) == 3:
            return '0' + t[:1] + ':' + t[1:]

ontime_repoting_merged['CRS_ARR_TIME'] = pd.to_datetime(ontime_repoting_merged['CRS_ARR_TIME'].apply(lambda x: getTime(x)),format = '%H:%M').dt.time
ontime_repoting_merged['ARR_TIME'] = pd.to_datetime(ontime_repoting_merged['ARR_TIME'].apply(lambda x: getTime(x)),format = '%H:%M').dt.time
ontime_repoting_merged['CRS_DEP_TIME'] = pd.to_datetime(ontime_repoting_merged['CRS_DEP_TIME'].apply(lambda x: getTime(x)),format = '%H:%M').dt.time
ontime_repoting_merged['DEP_TIME'] = pd.to_datetime(ontime_repoting_merged['DEP_TIME'].apply(lambda x: getTime(x)),format = '%H:%M').dt.time

# =============================================================================
# Data Cleaning
# =============================================================================

#Filter data to include only delta and american airline
airlines_to_consider = [19790,19805]

#AIRLINE
airline = airline[airline['AIRLINE_ID'].isin(airlines_to_consider)]

#AIRCRAFT
aircraft = aircraft[aircraft['AIRLINE_ID'].isin(airlines_to_consider)]

#FLIGHTS
ontime_repoting_merged = ontime_repoting_merged[ontime_repoting_merged['AIRCRAFT_ID'].isin(aircraft['AIRCRAFT_ID'].unique())]
ontime_repoting_merged = ontime_repoting_merged[ontime_repoting_merged['DATE'].dt.year == 2019]

#AIRPORT
airport_list = np.concatenate((ontime_repoting_merged['ORIGIN_AIRPORT_ID'],ontime_repoting_merged['DEST_AIRPORT_ID']),axis = 0)
airport = airport[airport['AIRPORT_ID'].isin(airport_list)]

#LOCATION
location = location[location['LOCATION_ID'].isin(airport['LOCATION_ID'])]

#WEATHER
df_weather = df_weather.merge(airport[['AIRPORT_NAME','AIRPORT_ID']], left_on = 'NAME', right_on = 'AIRPORT_NAME', how = 'inner')
df_weather = df_weather.drop(['NAME','AIRPORT_NAME'],axis = 1)
df_weather = df_weather.reset_index()
df_weather = df_weather.rename(columns = {'index':'WEATHER_ID'})


#Writing data to csv to use for inline function to insert the data into the db
df_weather.to_csv('final_data/weather.csv',index = False,header=None)
location.to_csv('final_data/location.csv',index = False,header=None)
airport.to_csv('final_data/airport.csv',index = False,header=None)
airline.to_csv('final_data/airline.csv',index = False,header=None)
aircraft.to_csv('final_data/aircraft.csv',index = False,header=None)
ontime_repoting_merged.to_csv('final_data/flight.csv',index = False,header=None)