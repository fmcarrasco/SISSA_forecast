# 1. Import the requests library
import requests
import pandas as pd
import time
import os
from tqdm import tqdm
from bs4 import BeautifulSoup

start = time.time()

#carpeta = '../../../DATOS/CFSR/'
carpeta = '/Volumes/Almacenamiento/python_proyects/DATOS/CFSR/'
os.makedirs(carpeta, exist_ok=True)

url_base = "https://www.ncei.noaa.gov/data/climate-forecast-system/access/reforecast/6-hourly-flux-9-month-runs/"
#url_base = "https://www.ncei.noaa.gov/data/climate-forecast-system/access/reforecast/high-priority-subset/time-series-9-month/"

years = [str(yr) for yr in range(2000,2012)]
months = [str(mo).zfill(2) for mo in range(1,13)]
fechas = pd.read_csv('fechas_descarga_CFSR.csv')
variables = ['t2']


for year in years[7:8]:
    print('Working in year ' + year)
    for month in months[8:9]:
        print('Working in month: ', month)
        df = fechas.loc[fechas.mes_guia == int(month)]
        for index, row in df.iterrows():
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
            url_test = url_base + year + '/' + ym_d + '/' + ymd + '/'
            # Archivo de texto con todos los nombres de archivo
            farch = carpeta_s + 'links_' + ymd + '.txt'
            soup = BeautifulSoup(requests.get(url_test).text, features="lxml")
            with open(farch,'w') as f:
                for a in soup.find_all('a'):
                    elec = ('.grb2.md5' in a['href']) or ('?C' in a['href']) or ('/data/' in a['href'])
                    if elec:
                        continue
                    else:
                        print(a['href'])
                        f.write(a['href']+'\n')

            #df = pd.read_csv('./lista_links.txt')
            #lista_arch = df.iloc[:,0].to_list()
            #sufix = '.01.' + ymd + '18' + '.grb2'
            #sublista = [urld for urld in lista_arch if (sufix in urld)]
            #print(sublista[0])
            #print(sublista[-1])
            #print(len(sublista))
            exit()
            print('Descargando archivo: ', archivo)
            # Generamos una link con el servidor
            response = requests.get(urld)
            total_b = int(response.headers.get('content-length', 0))
            if total_b < 500:
                continue
            fname = carpeta_s + archivo
            progress_bar = tqdm(total=total_b, unit='iB', unit_scale=True, unit_divisor=1024)
            with open(fname, 'wb') as farchivo:
                for data in response.iter_content(chunk_size=1024):
                    size = farchivo.write(data)
                    progress_bar.update(size)
            progress_bar.close()
            response.close()

end = time.time()
print('Tiempo de descarga: ', (end - start)/60., ' minutos')
exit()
'''
#2006/200612/20061212/flxf2007020212.01.2006121200.grb2"
# 2. download the data behind the URL
response = requests.get(URL)
total = int(response.headers.get('content-length', 0))
# 3. Open the response into a new file called instagram.ico
# open("acpcp_sfc_2000010500_c00.grib2", "wb").write(response.content)
#fname = "prate.01.2011121200.daily.grb2"
fname = "flxff2007020212.01.2006121200.grb2"
with open(fname, 'wb') as file, tqdm(desc=fname, total=total, unit='iB', unit_scale=True, unit_divisor=1024) as bar:
        for data in response.iter_content(chunk_size=1024):
            size = file.write(data)
            bar.update(size)

for hora in ['00', '06', '12', '18']:
                # Nombre archivo
                #archivo = 'flxf' + ymd + hora + '.01.' + ymd + '00' + '.grb2'
                # Nombre carpeta donde se guarda
                carpeta_s = carpeta + year + '/' + mes_g + '/'
                # URL de localilizacion de archivo
                #urld = url_base + year + '/' + ym_d + '/' + ymd + '/' + archivo
                url_test = url_base + year + '/' + ym_d + '/' + ymd + '/'
                # Archivo de texto con todos los nombres de archivo
                soup = BeautifulSoup(requests.get(url_test).text, features="lxml")
                with open('lista_links.txt','w') as f:
                    for a in soup.find_all('a'):
                        elec = ('.grb2.md5' in a['href']) or ('?C' in a['href']) or ('/data/' in a['href'])
                        if elec:
                            continue
                        else:
                            f.write(a['href']+'\n')
                df = pd.read_csv('./lista_links.txt')
                lista_arch = df.iloc[:,0].to_list()
                sufix = '.01.' + ymd + '18' + '.grb2'
                sublista = [urld for urld in lista_arch if (sufix in urld)]
                print(sublista[0])
                print(sublista[-1])
                print(len(sublista))

'''