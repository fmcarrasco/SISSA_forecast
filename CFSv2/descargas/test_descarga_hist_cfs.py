# 1. Import the requests library
import requests
import pandas as pd
import time
import os
from tqdm import tqdm
from bs4 import BeautifulSoup

start = time.time()

carpeta = './descargas_cfs/'
os.makedirs(carpeta, exist_ok=True)

url_base0 = "https://www.ncei.noaa.gov/data/climate-forecast-system/access/reforecast/6-hourly-flux-9-month-runs/"
url_base1 = "https://www.ncei.noaa.gov/oa/prod-cfs-reforecast/index.html#cfs_reforecast_6-hourly_9mon_flxf/"

year = '2000'
month= '01'
mes_g = '01'  # mes guia
mes_d = '12'  # mes descarga
dia_d = '12' # dia descarga
variables = ['t2']

# comenzamos a escribir los links
ymd = year + mes_d + dia_d
ym_g = year + mes_g
ym_d = year + mes_d
# Nombre carpeta donde se guarda
carpeta_s = carpeta + year + '/' + mes_g + '/'
os.makedirs(carpeta_s, exist_ok=True)
# URL de localilizacion de archivo
url_test = url_base0 + year + '/' + ym_d + '/' + ymd + '/'
print(url_test)
# Archivo de texto con todos los nombres de archivo para url_base0
soup = BeautifulSoup(requests.get(url_test).text, features="lxml")
with open('url0_lista.txt','w') as f:
    for a in soup.find_all('a'):
        f.write(a['href']+'\n')
###############
url_test = url_base1 + year + '/' + ym_d + '/' + ymd + '/'
print(url_test)

# Archivo de texto con todos los nombres de archivo para url_base1
soup = BeautifulSoup(requests.get(url_test).text, features="lxml")
with open('url1_lista.txt','w') as f:
    for a in soup.find_all('a'):
        f.write(a['href']+'\n')




