import os
import wget # Not Base
import zipfile
import shutil
import glob
import re
import csv
import pandas as pd # Not Base
import pymysql # Not Base
import sqlalchemy # Not Base
from sklearn.preprocessing import scale # Not Base

# Making a list of the files in the current directory
files = os.listdir('./')

# Remove old CSV zip file and unzipped folder from extraction
for file in files:
    if file == 'f1db_csv.zip':
        os.remove(file)
    if file == 'f1db_csv':
        shutil.rmtree(file)

# Make a list of all csv files in current directory        
csvs = glob.glob('./*.csv')

# Remove old CSVs
for csv in csvs:
    os.remove(csv)

# Download the new CSV zip file
url = 'http://ergast.com/downloads/f1db_csv.zip'
wget.download(url)

# Unzip the file
with zipfile.ZipFile("f1db_csv.zip") as f_in:
    f_in.extractall()

# Remove zip file
os.remove('f1db_csv.zip')

# Create list with all csv files in the directory
csvs = glob.glob("*.csv")

# Prep to give each CSV proper headers
circuit_headers = ['circuit_id', 'circuit_ref', 'name', 'location', 'country', 'lat', 'lng', 'alt', 'url']
status_headers = ['status_id', 'status']
lap_time_headers = ['race_id', 'driver_id', 'lap', 'position', 'time', 'milliseconds']
races_headers = ['race_id', 'year', 'round', 'circuit_id', 'name', 'date', 'time', 'url']
constructors_headers = ['constructor_id', 'constructor_ref', 'name', 'nationality', 'url']
constructor_standings_headers = ['constructor_standings_id', 'race_id', 'constructor_id', 'points', 'position', 'position_text', 'wins']
driver_headers = ['driver_id', 'driver_ref', 'number', 'code', 'forename', 'surname', 'dob', 'nationality', 'url']
qualifying_headers = ['qualify_id', 'race_id', 'driver_id', 'constructor_id', 'number', 'position', 'q1', 'q2', 'q3']
driver_standings_headers = ['driver_standings_id', 'race_id', 'driver_id', 'points', 'position', 'position_text', 'wins']
constructor_results_headers = ['constructor_results_id', 'race_id', 'constructor_id', 'points', 'status']
pit_stops_headers = ['race_id', 'driver_id', 'stop', 'lap', 'time', 'duration', 'milliseconds']
seasons_headers = ['year', 'url']
results_headers = ['result_id', 'race_id', 'driver_id', 'constructor_id', 'number', 'grid', 'position', 'position_text',
                   'position_order', 'points', 'laps', 'time', 'milliseconds', 'fastest_lap', 'rank', 'fastest_lap_time',
                   'fastest_lap_speed', 'status_id']

# Consolidate
headers_list = [circuit_headers, status_headers, lap_time_headers, races_headers, constructors_headers,
               constructor_standings_headers, driver_headers, qualifying_headers, driver_standings_headers,
               constructor_results_headers, pit_stops_headers, seasons_headers, results_headers]

def header_boi(in_csv, table_headers):
    ### This function will read a CSV file in the current directory and give the CSV its user specified headers
    # in_csv is the csv you wish to pull in
    # table_headers is a list of the headers you want the CSV to take on
    df = pd.read_csv(in_csv, header = None, index_col = False)
    df.columns = table_headers
    df.to_csv(in_csv)
       
# Loop through to give all CSVs in directory the proper headers
for i in range(0, len(csvs)):
    header_boi(csvs[i], headers_list[i])

# Connect to MySQL DB
### Assumes you already have a connection on local host, using user root, 
### with password root, an a db schema named f1test
conn = pymysql.connect(
    host = '127.0.0.1',
    port = 3306,
    user = 'root',
    passwd = 'root',
    db = 'f1test'
)

# Create Cursor
cur = conn.cursor()

# Create Engine cus I got confused and dont know if you need the connection above or not
e = sqlalchemy.create_engine("mysql+pymysql://root:root@localhost/f1test")

# Again create list of all CSVs, to then regex to get a list of all table names
csvs = glob.glob("*.csv")
tables = glob.glob("*.csv")

# Regex to remove .csv
for i in range(0, len(tables)):
    tables[i] = re.sub('.csv', '', tables[i])
    
# Create Drop Statements for old tables within the DB
del_statements = []
for table in tables:
    del_statements.append('drop table if exists f1test.{}'.format(table))

# Drop the old tables
for statement in del_statements:
    cur.execute(statement)

# Upload new tables
for table in tables:
    df = pd.read_csv('{}'.format(table) + '.csv')
    df.to_sql('{}'.format(table), con = e)
    







































