# 1. Import the requests library
import pandas as pd
import time
import os
from tqdm import tqdm
#from bs4 import BeautifulSoup

from urls import gen_urls
from download import check_md5, download_file_from_url, set_file_abs_path
from download import set_file_data

start = time.time()

#carpeta = '../../../DATOS/CFSR/'
#carpeta = '/Volumes/Almacenamiento/python_proyects/DATOS/CFSv2/'
carpeta = '/shera/datos/CFSv2/'
os.makedirs(carpeta, exist_ok=True)

url_base = "https://www.ncei.noaa.gov/data/climate-forecast-system/access/reforecast/6-hourly-flux-9-month-runs/"
#url_base = "https://www.ncei.noaa.gov/data/climate-forecast-system/access/reforecast/high-priority-subset/time-series-9-month/"
#url_base = "https://www.ncei.noaa.gov/oa/prod-cfs-reforecast/index.html#high-priority-subset/time-series-9-month/"
#url_base = "https://www.ncei.noaa.gov/oa/prod-cfs-reforecast/index.html#cfs_reforecast_6-hourly_9mon_flxf/"

years = [str(yr) for yr in range(2000,2012)]
months = [str(mo).zfill(2) for mo in range(1,13)]
fechas = pd.read_csv('fechas_descarga_CFSR.csv')
variables = ['t2']


for year in years[0:1]:
    print('Working in year ' + year)
    for index, row in fechas.iloc[24:,:].iterrows():
        year_g = year
        mes_g = str(row['mes_guia']).zfill(2)
        mes_d = str(row['mes_descarga']).zfill(2)
        dia_d = str(row['dia_descarga']).zfill(2)
        ymd = year + mes_d + dia_d
        ym_g = year + mes_g
        ym_d = year + mes_d
        # Nombre carpeta donde se guarda
        carpeta_s = carpeta + year + '/' + mes_g + '/'
        os.makedirs(carpeta_s, exist_ok=True)
        # URL de localilizacion de archivo
        urls = list(gen_urls(int(year), row['mes_descarga'], row['dia_descarga'], row['mes_guia'], url_base))
        print(urls[0])
        
        for n, url in enumerate(urls):
            print(f"Procesando archivo {n + 1} de {len(urls)}.")
            print(url)
            file_path = set_file_abs_path(url, year_g, mes_g)
            if not file_path.exists() or not check_md5(file_path, url):
                download_file_from_url(file_path, url)
