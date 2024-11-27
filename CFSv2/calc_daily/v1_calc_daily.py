import time
import iris
import datetime as dt
import xarray as xr
import numpy as np
import pandas as pd
import glob
import os
from funciones import calculo_diario, variable_diaria

import sys

# Variables dadas en el llamado
year = sys.argv[1]
mes = sys.argv[2].zfill(2)
variable = sys.argv[3]

var_d = variable_diaria(variable)

# Variables iniciales

if int(year) > 2011:
    print('Este script trabaja con datos desde 2000-2011')
    exit()

######################################
print('Working in year ' + year)
print('at month:', mes)
c0 = '/shera/datos/SISSA/CFSv2/'
c1 = '/shera/datos/SISSA/Diarios/CFSv2/'
carpeta_i = c0 + variable + '/' + year + '/' + mes + '/'
carpeta_o = c1 + var_d + '/' + year + '/' + mes + '/'
os.makedirs(carpeta_o, exist_ok=True)

print(carpeta_i)
print(carpeta_o)

fechas = sorted([ f.path for f in os.scandir(carpeta_i) if f.is_dir() ])
horas = ['00', '06', '12', '18']
carpetas = []
for fecha in fechas:
    for hora in horas:
        cp0 = fecha + '/' + hora + '/'
        if os.path.isdir(cp0):
            carpetas.append(cp0)

print(len(carpetas))
starttime_carpeta = time.time()
for carpeta in carpetas:
    archivos = sorted(glob.glob(carpeta + '*.nc'))
    n_archivo = var_d + '.' + '.'.join(os.path.basename(archivos[0]).split('.')[1:])
    try:
        ds = xr.open_mfdataset(archivos)
        print('Procesando carpeta:', carpeta)
        print('Archivo inicial:', archivos[0])
        # Calculos Diarios
        daily = calculo_diario(ds, variable, v025, esquema)
        outfile = carpeta_o + n_archivo
        print('Guardando archivo:', outfile)
        iris.save(daily, outfile)
    except:
        print('Error con el archivo:', n_archivo)
        continue
# END FOR
endtime_carpeta = time.time()
carpeta_sec = endtime_carpeta - starttime_carpeta
carpeta_min = np.round(carpeta_sec/60.,2)
carpeta_hou = np.round(carpeta_min/60.,2)
print('Minutos por carpeta', carpeta_min)
print('Horas por carpeta', carpeta_hou)

