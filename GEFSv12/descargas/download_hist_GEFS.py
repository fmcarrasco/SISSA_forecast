import os
import datetime as dt
import pandas as pd
import requests
import time
from tqdm import tqdm


'''
Script escrito por: Felix Carrasco
Marco: Consultoria SISSA pronosticos

Este script esta hecho para la descarga de datos historicos de GEFSv12
De acuerdo a la documentacion en:
https://noaa-gefs-retrospective.s3.amazonaws.com/Description_of_reforecast_data.pdf

Solo los dias miercoles hay 10 miembros del ensamble (c: control + p{xx}: miembros)
y con pronósticos hasta 35 dias.

Las variables disponibles de interes para el proyecto son:

1) apcp_sfc: Total precipitation (kg m-2, i.e., mm) sum over the last 6-h period
2) tmp_2m: 2-meter temperature (K)
3) pres_sfc: Surface pressure (Pa)
4) dswrf_sfc: Downward short-wave radiation flux at the surface (W m-2) average in last 6-h period
5) dlwrf_sfc: Downward long-wave radiation flux at the surface (W m-2) average in last 6-h period
6) ugrd_hgt: U component at 10m/100m (m/s)
7) vgrd_hgt: V componente at 10m/100m (m/s)

Falta por ver: Presion de vapor, otros?

Estructura de carpetas de descarga

base: /shera/datos/GEFSv12/
resto:
{variable}/{year}/{yearmonthday}00/[d1-10, d10-35]/{Archivos}.grib2
'''
start = time.time()

carpeta = '/shera/datos/GEFSv12/'
os.makedirs(carpeta, exist_ok=True)

url_base = 'https://noaa-gefs-retrospective.s3.amazonaws.com/GEFSv12/reforecast/'
fechas = pd.bdate_range(start='2004-12-08', end='2019-12-25', freq='W-WED')
variable = 'apcp_sfc'
ensembles = ['c00', 'p01', 'p02', 'p03', 'p04', 'p05', 'p06', 'p07', 'p08', 'p09', 'p10']
dias = ['Days%3A1-10', 'Days%3A10-35']
dias_f = ['d1-10', 'd10-35']


for fecha in fechas[0:1]:
    year = fecha.strftime('%Y')
    ymd = fecha.strftime('%Y%m%d')
    for ensemble in ensembles[1:2]:
        for dia, dia_f in zip(dias, dias_f):
            time.sleep(6)  # 6 segundos de sleep para no colapsar el servidor.
            # Colocamos nombre de archivo
            archivo = variable + '_' + ymd + '00_' + ensemble + '.grib2'
            carpeta_s = carpeta + variable + '/' + year + '/' + ymd + '00/' + dia_f + '/'
            # Colocamos nombre de URL
            urld = url_base + year + '/' + ymd + '00/' + ensemble + '/' + dia + '/' + archivo
            # Creamos carpeta
            os.makedirs(carpeta_s, exist_ok=True)
            # Generamos una link con el servidor
            response = requests.get(urld)
            total = int(response.headers.get('content-length', 0))
            fname = carpeta_s + archivo
            # Comenzamos la descarga
            print('Descargando ', archivo, ' en ', carpeta_s)
            with open(fname, 'wb') as file:#, tqdm(desc=fname, total=total, unit='iB', unit_scale=True, unit_divisor=1024) as bar:
                    for data in response.iter_content(chunk_size=1024):
                        size = file.write(data)
                        #bar.update(size)
            response.close()
            ##########
            
end = time.time()
print('Tiempo de descarga: ', (end - start)/60., ' minutos')
