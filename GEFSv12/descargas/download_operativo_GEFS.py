import os
import datetime as dt
import numpy as np
import pandas as pd
import requests
import time
from tqdm import tqdm


'''
Script escrito por: Felix Carrasco
Marco: Consultoria SISSA pronosticos

Este script esta hecho para la descarga de datos operativos de GEFSv12
De acuerdo a la documentacion en:
https://www.emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/gefs.php
https://www.nco.ncep.noaa.gov/pmb/products/gens/

Se pretende la descarga operativa todos los dias miercoles
donde hay 31 miembros del ensamble (c: control + p{xx}: miembros)
y con pronósticos hasta 10 días en 0.25º resolucion
y 35 dias en 0.5º resolucion.

Al contrario de los datos historicos, en este caso se descarga un archivo por
hora de pronóstico y en el sitio HTTPS donde para cada miercoles
hay que descargar:

1) control + 30 miembros con datos entre 0-10 días de pronóstico en 0.25
cada 3 horas [Carpeta pgrb2sp25] [80 + 2400 archivos]
2) control + 30 miembros con datos entre 11-35 días de pronóstico en 0.5
cada 6 horas [Carpeta pgrb2ap5] [96 + 2880 archivos]

-------- Variables ----------
Las variables disponibles en cada archivo se pueden encontrar:

datos en 0.25:
https://www.nco.ncep.noaa.gov/pmb/products/gens/gec00.t00z.pgrb2s.0p25.f003.shtml
https://www.nco.ncep.noaa.gov/pmb/products/gens/gep01.t00z.pgrb2s.0p25.f003.shtml

formato archivo:
control: gec00.t00z.pgrb2s.0p25.fXXX [XXX entre 000 y 240 cada 3]
ensemble: gepNN.t00z.pgrb2s.0p25.fXXX [NN entre 01 y 30 - XXX entre 000 y 240 cada 3]


datos en 0.5:
https://www.nco.ncep.noaa.gov/pmb/products/gens/gec00.t00z.pgrb2a.0p50.f003.shtml
https://www.nco.ncep.noaa.gov/pmb/products/gens/gep01.t00z.pgrb2a.0p50.f003.shtml

formato archivo:
control: gec00.t00z.pgrb2a.0p50.fXXX [XXX entre 246 y 840 cada 6]
ensemble: gepNN.t00z.pgrb2a.0p50.fXXX [NN entre 01 y 30 - XXX entre 246 y 840 cada 6]

------ Estructura de carpetas de descarga ------

base: /shera/datos/GEFSv12/operativos/
resto:
{year}/{yearmonthday}00/[d1-10, d10-35]/{Archivos}.grib2
'''
start = time.time()

carpeta = './shera/datos/GEFSv12/operativos/'
os.makedirs(carpeta, exist_ok=True)

url_base = 'https://ftp.ncep.noaa.gov/data/nccf/com/gens/prod/'
fecha = dt.datetime.strptime('2022-10-12', '%Y-%m-%d')

url025 = url_base + 'gefs.' + fecha.strftime('%Y%m%d/')+'00/atmos/pgrb2sp25/'
url050 = url_base + 'gefs.' + fecha.strftime('%Y%m%d/')+'00/atmos/pgrb2ap5/'

ensn = [str(a).zfill(2) for a in np.arange(0,31)]
h025 = [str(a).zfill(3) for a in np.arange(0,243,3)]
h050 = [str(a).zfill(3) for a in np.arange(246,846,6)]

year = fecha.strftime('%Y')
ymd = fecha.strftime('%Y%m%d')
ycarpeta025 = carpeta + year + '/' + ymd + '/' + 'd1-10/'
ycarpeta050 = carpeta + year + '/' + ymd + '/' + 'd10-35/'

##### Descargas para datos 0.25 #####
os.makedirs(ycarpeta025, exist_ok=True)

for i, n_ens in enumerate(ensn):
    for hora in h025:
        time.sleep(1)
        if i == 0:
            archivo = 'gec' + n_ens + '.t00z.pgrb2s.0p25.f' + hora
        else:
            archivo = 'gep' + n_ens + '.t00z.pgrb2s.0p25.f' + hora
        ######
        urld = url025 + archivo
        fname = ycarpeta025 + archivo

        # Generamos una link con el servidor
        response = requests.get(urld)
        print('Descargando ', archivo, ' en ', ycarpeta025)
        with open(fname, 'wb') as file:
                for data in response.iter_content(chunk_size=1024):
                    size = file.write(data)
        response.close()


##### Descargas para datos 0.5 #####
os.makedirs(ycarpeta050, exist_ok=True)

for i, n_ens in enumerate(ensn):
    for hora in h050:
        time.sleep(1)
        if i == 0:
            archivo = 'gec' + n_ens + '.t00z.pgrb2a.0p50.f' + hora
        else:
            archivo = 'gep' + n_ens + '.t00z.pgrb2a.0p50.f' + hora
        ######
        urld = url025 + archivo
        fname = ycarpeta025 + archivo

        # Generamos una link con el servidor
        response = requests.get(urld)
        print('Descargando ', archivo, ' en ', ycarpeta025)
        with open(fname, 'wb') as file:
                for data in response.iter_content(chunk_size=1024):
                    size = file.write(data)
        response.close()



end = time.time()
print('Tiempo de descarga: ', (end - start)/60., ' minutos')
