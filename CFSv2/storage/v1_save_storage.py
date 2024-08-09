import time
import xarray as xr
import numpy as np
import pandas as pd
import glob
import os
from aux_functions import get_cut_grib, save_netcdf

import sys

start = time.time()

year = sys.argv[1]
mes = sys.argv[2].zfill(2)
cpta = int(sys.argv[3])

c0 = '/shera/datos/CFSv2/'
c1 = '/shera/datos/SISSA/CFSv2/'

f_guia = '../descargas/fechas_descarga_CFSR.csv'
fechas = pd.read_csv(f_guia)

variables = pd.read_csv('variables_aceptadas.txt',index_col=['var'])

print('Working in year ' + year)
print('at month:', mes)
year_g = year
mes_g = mes
carpeta_i = c0 + year + '/' + mes_g + '/'
dirlist = sorted([d for d in next(os.walk(carpeta_i))[1]])
starttime_carpeta = time.time()
for carpeta in dirlist[cpta:cpta+1]:
    print('Working in carpeta ' + carpeta_i + carpeta)
    for hour in ['00','06','12','18']:
        print('Working in hour ' + hour)
        archivos=sorted(glob.glob(carpeta_i + carpeta + '/*.*' + hour + '.grb2'))
        #ds0 = xr.open_dataset(archivos[0], engine='pynio')
        #ds1 = xr.open_dataset(archivos[1], engine='pynio')
        #ds_concat = xr.concat([ds0, ds1], dim='time')
        ds_concat = xr.open_mfdataset(archivos, concat_dim='time', combine='nested', compat='override', engine='pynio')
        print(ds_concat)
        exit()
        '''
        for archivo in archivos:
            try:
                tiempos, lat, latb, lon, lonb, datos, unidades = get_cut_grib(archivo)
                print('Procesando archivo:', archivo)
            except:
                print('Error con el archivo:', archivo)
                continue
            for key in datos.keys():
                carpeta_o = c1 + '/' + key + '/' + year + '/' + mes_g + '/' + carpeta + '/' + hour + '/'
                os.makedirs(carpeta_o, exist_ok=True)
                narchivo = '.'.join(os.path.basename(archivo).split('.')[0:3]) + '.nc'
                ncfile = carpeta_o + narchivo
                new_datos = {'nvar': key, 'units': unidades[key],
                        'standard_name': variables.loc[key,'standard_name'],
                        'long_name': variables.loc[key,'long_name'],
                        'valores': datos[key]}
                save_netcdf(ncfile, tiempos, lat, latb, lon, lonb, new_datos)
# END FOR
endtime_carpeta = time.time()
carpeta_sec = endtime_carpeta - starttime_carpeta
carpeta_min = np.round(carpeta_sec/60.,2)
carpeta_hou = np.round(carpeta_min/60.,2)
print('Minutos por carpeta', carpeta_min)
print('Horas por carpeta', carpeta_hou)
        '''

