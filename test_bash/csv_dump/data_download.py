import os
import wget
import zipfile
import shutil
import glob
import re
import csv
import pandas as pd



files = os.listdir('./')

for file in files:
    if file == 'f1db_csv.zip':
        os.remove(file)
    if file == 'f1db_csv':
        shutil.rmtree(file)
        
csvs = glob.glob('./*.csv')
for csv in csvs:
    os.remove(csv)

url = 'http://ergast.com/downloads/f1db_csv.zip'

wget.download(url)

with zipfile.ZipFile("f1db_csv.zip") as f_in:
    f_in.extractall()
    
os.remove('f1db_csv.zip')

csvs = glob.glob("*.csv")

circuit_headers = ['circuit_id', 'circuit_ref', 'name', 'location', 'country', 'lat', 'lng', 'alt', 'url']
status_headers = ['status_id', 'status']
lap_time_headers = ['race_id', 'driver_id', 'lap', 'position', 'time', 'milliseconds']
races_headers = ['race_id', 'year', 'round', 'circuit_id', 'name', 'date', 'time', 'url']
constructors_headers = ['constructor_id', 'constructor_ref', 'name', 'nationality', 'url']
constructor_standings_headers = ['constructor_standings_id', 'race_id', 'constructor_id', 'points', 'position', 'position_text', 'wins']
driver_headers = ['driver_id', 'driver_ref', 'number', 'code', 'forename', 'surname', 'dob', 'nationality', 'url']
qualifying_headers = ['qualify_id', 'race_id', 'driver_id', 'constructor_id', 'number', 'position', 'q1', 'q2', 'q3']
driver_standings_headers = ['driver_standings)_id', 'race_id', 'driver_id', 'points', 'position', 'position_text', 'wins']
constructor_results_headers = ['constructor_results_id', 'race_id', 'constructor_id', 'points', 'status']
pit_stops_headers = ['race_id', 'driver_id', 'stop', 'lap', 'time', 'duration', 'milliseconds']
seasons_headers = ['year', 'url']
results_headers = ['result_id', 'race_id', 'driver_id', 'constructor_id', 'number', 'grid', 'position', 'position_text',
                   'position_order', 'points', 'laps', 'time', 'milliseconds', 'fastest_lap', 'rank', 'fastest_lap_time',
                   'fastest_lap_speed', 'status_id']

headers_list = [circuit_headers, status_headers, lap_time_headers, races_headers, constructors_headers,
               constructor_standings_headers, driver_headers, qualifying_headers, driver_standings_headers,
               constructor_results_headers, pit_stops_headers, seasons_headers, results_headers]

def header_boi(in_csv, table_headers):
    df = pd.read_csv(in_csv)
    df.columns = table_headers
    df.to_csv(in_csv)
       

for i in range(0,12):
    header_boi(csvs[i], headers_list[i])