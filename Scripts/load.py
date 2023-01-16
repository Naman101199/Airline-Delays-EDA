import os
import pandas as pd
import sqlalchemy as db
import warnings

warnings.filterwarnings("ignore")
os.chdir('/Users/'+ os.getlogin() + '/Desktop/DataEngineeringPlatforms')

#GCP Bucket connector
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/Users/naman/Downloads/dogwood-abbey-364703-1289c40bd8d0.json'


# =============================================================================
# Method 1 - Inserting data into MySQL
# =============================================================================
engine = db.create_engine('mysql+pymysql://root:rootroot@34.121.37.33/flights_db',
                          echo = True)
conn = engine.connect()

weather = pd.read_csv('final_data/weather.csv')
flight = pd.read_csv('final_data/flight.csv')

#After creating EER in MySQL, we have the table structure. So we set if_exists = 'append'
weather.to_sql(name = 'weather',
               con = conn,
               schema='flights_db',
               if_exists = 'append',
               index = False)

flight.to_sql(name = 'flight',
              con = conn,
              schema='flights_db',
              if_exists = 'append',
              index = False)


# =============================================================================
# Method 2 - Inserting data into Bucket and then importing data into MySQL
# =============================================================================
from google.cloud import storage

def upload_to_bucket(bucket_name, blob_path, local_path):
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_path)

# method call
bucket_name = 'depa_flights_datasets' # do not give gs:// ,just bucket name

#csvs = ['weather.csv','location.csv','airport.csv','airline.csv','aircraft.csv','flight.csv']
csvs = ['location.csv','airport.csv','airline.csv','aircraft.csv','flight.csv','weather.csv']

for csv in csvs:
    blob_path = 'final_data/' + csv
    local_path = 'final_data/' + csv #local file path
    upload_to_bucket(bucket_name, blob_path, local_path)
