# 1. Import the requests library
import requests
import time
import os
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from urls import gen_urls

headers = {
    'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/58.0.3029.110 Safari/537.3'
}

start = time.time()

carpeta = './descargas_cfs/'
os.makedirs(carpeta, exist_ok=True)

url_base0 = "https://www.ncei.noaa.gov/data/climate-forecast-system/access/reforecast/6-hourly-flux-9-month-runs/"
url_base1 = "https://www.ncei.noaa.gov/oa/prod-cfs-reforecast/index.html#cfs_reforecast_6-hourly_9mon_flxf/"

year = '2001'
month = '01'
mes_g = '01'  # mes guia
mes_d = '12'  # mes descarga
dia_d = '12'  # día descarga
variables = ['t2']

# comenzamos a escribir los links
ymd = year + mes_d + dia_d
ym_g = year + mes_g
ym_d = year + mes_d
# Nombre carpeta donde se guarda
carpeta_s = carpeta + year + '/' + mes_g + '/'
os.makedirs(carpeta_s, exist_ok=True)

###############
# URL de localización de archivo
url_test = url_base0 + year + '/' + ym_d + '/' + ymd + '/'
print(url_test)

# Archivo de texto con todos los nombres de archivo para url_base0
resp = requests.get(url_test, headers=headers, timeout=None)
if resp.status_code == 502:
    print(f'Status Code: {resp.status_code} (1er intento)')
    time.sleep(10)
    resp = requests.get(url_test, headers=headers, timeout=None)
    print(f'Status Code: {resp.status_code} (2do intento)')
if resp.status_code == 200:
    soup = BeautifulSoup(resp.text, features="lxml")
    with open(f'urls_descargadas_{year}-{mes_d}-{dia_d}.txt', 'w') as f:
        for a in soup.find_all('a'):
            if a['href'].endswith('.grb2'):
                f.write(url_test + a['href']+'\n')
else:
    print(f'Status Code: {resp.status_code}')

# Generar lista de URLs automáticamente (sin descargar nada)
with open(f'urls_generadas_{year}-{mes_d}-{dia_d}.txt', 'w') as f:
    for url in gen_urls(int(year), int(mes_d), int(dia_d), url_base0):
        f.write(url + '\n')


###############
# URL de localización de archivo
url_test = url_base1 + year + '/' + ym_d + '/' + ymd + '/'
print(url_test)

# Archivo de texto con todos los nombres de archivo para url_base1
options = FirefoxOptions()
options.add_argument("--headless")
driver = webdriver.Firefox(options=options)
driver.get(url_test)
soup = BeautifulSoup(driver.page_source, features="html.parser")
with open(f'url_descargadas_{year}-{mes_d}-{dia_d}_firefox.txt', 'w') as f:
    for a in soup.find_all('a'):
        if a['href'].endswith('.grb2'):
            f.write(a['href']+'\n')

# OBS:
# Solución problema selenium firefox desde pycharm:
# https://github.com/mozilla/geckodriver/issues/2062#issuecomment-1406785007
