import time
import xarray as xr
import numpy as np
import pandas as pd
import glob
import os
from aux_functions import get_cut_grib, save_netcdf

start = time.time()

c0 = '/shera/datos/CFSv2/'
c1 = '/shera/datos/SISSA/CFSv2/'

f_guia = '../descargas/fechas_descarga_CFSR.csv'
years = [str(yr) for yr in range(2000,2012)]
months = [str(mo).zfill(2) for mo in range(1,13)]
fechas = pd.read_csv(f_guia)

variables = pd.read_csv('variables_aceptadas.txt',index_col=['var'])

for year in years[0:1]:
    print('Working in year ' + year)
    for mes in months[0:1]:
        year_g = year
        mes_g = str(mes).zfill(2)
        print(mes_g)
        carpeta_i = c0 + year + '/' + mes_g + '/'
        dirlist = sorted([d for d in next(os.walk(carpeta_i))[1]])
        starttime_carpeta = time.time()
        for carpeta in dirlist[0:1]:
            print('Working in carpeta ' + carpeta_i + carpeta)
            for hour in ['00','06','12','18']:
                print('Working in hour ' + hour)
                archivos=sorted(glob.glob(carpeta_i + carpeta + '/*.*' + hour + '.grb2'))
                for archivo in archivos[0:1]:
                    try:
                        tiempos, lat, latb, lon, lonb, datos, unidades = get_cut_grib(archivo)
                        print('Procesando archivo:', archivo)
                    except:
                        print('Error con el archivo:', archivo)
                        continue
                    for key in datos.keys():
                        #carpeta_o = c1 + '/' + key + '/' + year + '/' + mes_g + '/' + carpeta + '/' + hour + '/'
                        carpeta_o = './'
                        os.makedirs(carpeta_o, exist_ok=True)
                        narchivo = '.'.join(os.path.basename(archivo).split('.')[0:3]) + '.nc'
                        ncfile = carpeta_o + narchivo
                        new_datos = {'nvar': key, 'units': unidades[key],
                                'standard_name': variables.loc[key,'standard_name'],
                                'long_name': variables.loc[key,'long_name'],
                                'valores': datos[key]}
                        save_netcdf(ncfile, tiempos, lat, latb, lon, lonb, new_datos)
                    exit()
        endtime_carpeta = time.time()
        carpeta_sec = endtime_carpeta - starttime_carpeta
        carpeta_min = np.round(carpeta_sec/60.,2)
        carpeta_hou = np.round(carpeta_min/60.,2)
        print('Minutos por carpeta', carpeta_min)
        print('Horas por carpeta', carpeta_hou)
endtime = time.time()
total_seg = endtime - start
total_min = np.round(total_seg/60., 2)
total_hou = np.round(total_min/60., 2)
print('Minutos totales:', total_min)
print('Horas totales:', total_hou)

