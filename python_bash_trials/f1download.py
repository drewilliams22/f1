import os
import wget
import gzip
import shutil


files = os.listdir('./')

for file in files:
    if file == 'f1db.sql':
        os.remove(file)
    if file == 'f1db.sql.gz':
        os.remove(file)
        
url = 'http://ergast.com/downloads/f1db.sql.gz'

wget.download(url)

def gunzip_shutil(source_filepath, dest_filepath, block_size=65536):
    with gzip.open(source_filepath, 'rb') as s_file, \
            open(dest_filepath, 'wb') as d_file:
        shutil.copyfileobj(s_file, d_file, block_size)

gunzip_shutil('f1db.sql.gz', 'f1db.sql')

os.remove('f1db.sql.gz')
