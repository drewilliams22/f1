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
    
# Create Query for base table
query = '''with 
base as (
select re.race_id + 1 as upcoming_race, ra.year as prior_race_year, re.race_id as prior_race
    , concat(re.race_id, '-', re.driver_id) as comp_key
    , re.driver_id, re.constructor_id
    , re.points as prior_points_driver
    , ds.points as prior_total_points_driver
        , ds.wins as prior_total_wins_driver
        , ds.position as prior_position_driver
        , cs.points as prior_total_points_constructor
        , cs.wins as prior_total_wins_constructor
from results re
  join races ra
    on re.race_id = ra.race_id
  join constructor_standings cs
    on re.race_id = cs.race_id and re.constructor_id = cs.constructor_id
  join driver_standings ds
    on re.race_id = ds.race_id and re.driver_id = ds.driver_id
  where ra.year = 2019
),
step1_zscore as (
  select lt.race_id, lt.driver_id, lt.milliseconds
      , avg(lt.milliseconds) OVER (partition by lt.race_id) as ms_avg_race
      , stddev(lt.milliseconds) OVER (partition by lt.race_id) as sd_ms
      , concat(lt.race_id, '-', lt.driver_id) as fk
  from lap_times lt
    join races ra
      on lt.race_id = ra.race_id
    where ra.year = 2019
),
z_score as (
    select  avg((milliseconds - ms_avg_race) / sd_ms) as scaled_performance, fk
    from step1_zscore
    group by fk
),
quali as (
	select concat(race_id, '-', driver_id) as fk
    , q1, q2, q3
    from qualifying
)
select ra.year as upcoming_race_year -- Just to double check for backtesting (cant predict the first race of the next year with the last race of the prior year)
    , b.*
    , z.scaled_performance as prior_race_scaled_performance
    , q.q1, q.q2, q.q3
    , re.position as pred_position
from base b
  left join races ra
    on b.upcoming_race = ra.race_id
  left join z_score z
    on b.comp_key = z.fk
  left join quali q
	on b.comp_key = q.fk
  left join results re
	on b.upcoming_race = re.race_id and b.driver_id = re.driver_id
order by prior_race asc, driver_id asc;'''

# Save query to Pandas DF
base_query = pd.read_sql_query(query, e)