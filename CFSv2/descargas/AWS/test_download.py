#! /usr/bin/env python3

# this assumes the final prefix files fall under
# to explore other prefixes, same general approach may be used, but response (r) differs in structure

import pandas as pd
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import sys

from urls import gen_urls
from download import check_md5, set_file_abs_path
from hashlib import md5

years = [str(yr) for yr in range(2009,2010)]
fechas = pd.read_csv('../fechas_descarga_CFSR.csv')
endpoint="https://www.ncei.noaa.gov/oa"
bucket = 'prod-cfs-reforecast'
datab = 'cfs_reforecast_6-hourly_9mon_flxf'

c = boto3.client("s3", endpoint_url=endpoint, config=Config(signature_version=UNSIGNED))

for year in years[0:1]:
    print('Working in year ' + year)
    for index, row in fechas.iloc[0::,:].iterrows():
        c = boto3.client("s3", endpoint_url=endpoint, config=Config(signature_version=UNSIGNED))
        mes_g = str(row['mes_guia']).zfill(2)
        mes_d = str(row['mes_descarga']).zfill(2)
        dia_d = str(row['dia_descarga']).zfill(2)
        ymd = year + mes_d + dia_d
        ym_g = year + mes_g
        ym_d = year + mes_d
        print('-- ', index, ' --')
        print(ymd, ym_g, ym_d)
        prefix = datab + '/' + year + '/' + ym_g + '/' + ymd + '/' 
        r = c.list_objects(Bucket=bucket,Delimiter='/',Prefix=prefix)
        urls = list(gen_urls(int(year), row['mes_descarga'], row['dia_descarga'], row['mes_guia'], datab))
        for url in urls:
            print(url)
            filename = url.split('/')[-1]
            print(filename)
            file_path = set_file_abs_path(url, year, mes_g)
            print(file_path)
            try:
                c.download_file(Bucket=bucket, Key=url, Filename=file_path)
            except:
                print('No pude descargar:', file_path)
                continue
