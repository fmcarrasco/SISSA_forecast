# 1. Import the requests library
import numpy as np
import pandas as pd
import time
import os
from tqdm import tqdm
#from bs4 import BeautifulSoup

from urls_variable import gen_urls
from download import check_md5, download_file_from_url, set_file_abs_path
from download import set_file_data

start = time.time()

#carpeta = '../../../DATOS/CFSR/'
#carpeta = '/Volumes/Almacenamiento/python_proyects/DATOS/CFSv2/'
carpeta = '/shera/datos/CFSv2/'
os.makedirs(carpeta, exist_ok=True)

url_base = "https://www.ncei.noaa.gov/data/climate-forecast-system/access/operational-9-month-forecast/time-series/"

years = [str(yr) for yr in range(2011,2020)]
months = [str(mo).zfill(2) for mo in range(1,13)]
fechas = pd.read_csv('fechas_descarga_CFSR.csv')


for year in years[8:9]:
    print('Working in year ' + year)
    for index, row in fechas.iloc[69::,:].iterrows():
        year_g = year
        mes_g = str(row['mes_guia']).zfill(2)
        mes_d = str(row['mes_descarga']).zfill(2)
        dia_d = str(row['dia_descarga']).zfill(2)
        ymd = year + mes_d + dia_d
        ym_g = year + mes_g
        ym_d = year + mes_d
        print('-- ', index, ' --')
        print(ymd, ym_g, ym_d)
        # Nombre carpeta donde se guarda
        carpeta_s = carpeta + year + '/' + mes_g + '/'
        os.makedirs(carpeta_s, exist_ok=True)
        # URL de localilizacion de archivo
        for hour in [0,6,12,18]:
            urls = list(gen_urls(int(year), row['mes_descarga'], row['dia_descarga'], hour, row['mes_guia'], url_base))
            for n, url in enumerate(urls):
                print(f"Procesando archivo {n + 1} de {len(urls)}.")
                print(url)
                #exit()
                file_path = set_file_abs_path(url, year_g, mes_g)
                print(file_path)
                if not file_path.exists() or not check_md5(file_path, url):
                    download_file_from_url(file_path, url)
                    print('Guardando archivo en:', file_path)
                print('\n')
                time.sleep(3)
end = time.time()
secs = np.round(end - start,2)
mins = np.round(secs/60., 2)
hour = np.round(mins/60.,2)
print('######### TERMINO LA DESCARGA #########')
print('### Segundos:', secs)
print('### Minutos:', mins)
print('### Horas:', hour)
