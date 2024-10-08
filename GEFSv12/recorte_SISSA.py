import numpy as np
import pandas as pd
import time
import os

import sys
sys.path.append('./lib/')

from aux_functions import get_cut_grib, save_netcdf

start = time.time()

variables = ['apcp_sfc', 'dlwrf_sfc', 'dswrf_sfc', 'pres_sfc', 'tmax_2m', 'tmin_2m',
             'tmp_2m', 'ulwrf_sfc', 'ugrd_hgt', 'vgrd_hgt', 'spfh_2m']

var = variables[10]

datavar = {'nvar': 'spfh_2m', 'standard_name': 'specific_humidity',
        'long_name': '2-Meter Specific Humidity'}
#2000-01-05
fechas = pd.date_range('2017-04-05', '2019-12-25', freq='W-WED')
ensembles = ['c00', 'p01', 'p02', 'p03', 'p04', 'p05', 'p06', 'p07', 'p08', 'p09', 'p10']

carpeta = '/shera/datos/SISSA/GEFSv12/'

print('---------------')
for fecha in fechas:
    print('Processing: ' + fecha.strftime('%Y-%m-%d'))
    for ensemble in ensembles:
        print('Ensemble member: ' + ensemble)
        year = str(fecha.year)
        fecha_c = fecha.strftime('%Y%m%d00')
        f2 = '/shera/datos/GEFSv12/' + var + '/' + year + '/' + fecha_c + '/d10-35/' + var +\
                '_' + fecha_c + '_' + ensemble + '.grib2'
        f1 = '/shera/datos/GEFSv12/' + var + '/' + year + '/' + fecha_c + '/d1-10/' + var +\
                '_' + fecha_c + '_' + ensemble + '.grib2'
        #------ Recorte para archivo 1-10 dias
        if (not os.path.exists(f1)) or (not os.path.exists(f2)):
            continue
        pesoMB = np.round(os.stat(f1).st_size/1024.0/1024.0,2)
        if pesoMB < 1.:
            print('Error en el archivo: ')
            print(f1)
            print('Pesa menos de lo esperado: ', pesoMB, ' MB')
        else:
            tiempos, lat, latb, lon, lonb, datos, units = get_cut_grib(f1)
            carpeta_nc = carpeta + var + '/' + year + '/' + fecha_c + '/d1-10/'
            file_nc = var + '_' + fecha_c + '_' + ensemble + '.nc'
            os.makedirs(carpeta_nc, exist_ok=True)
            print('Creating file: ', carpeta_nc + file_nc)
            datavar['units'] =  units,
            datavar['valores'] = datos
            save_netcdf(carpeta_nc + file_nc, tiempos, lat, latb, lon, lonb, datavar)
            del tiempos, lat, latb, lon, lonb, datos, units
        #------ Recorte para archivo 10-35 dias
        pesoMB = np.round(os.stat(f2).st_size/1024.0/1024.0,2)
        if pesoMB < 1.:
            print('Error en archivo GRIB')
            print(f2)
            print('Pesa menos de lo esperado: ', pesoMB, ' MB')
        else:
            tiempos, lat, latb, lon, lonb, datos, units = get_cut_grib(f2)
            carpeta_nc = carpeta + var + '/' + year + '/' + fecha_c + '/d10-35/'
            file_nc = var + '_' + fecha_c + '_' + ensemble + '.nc'
            os.makedirs(carpeta_nc, exist_ok=True)
            datavar['units'] =  units,
            datavar['valores'] = datos
            save_netcdf(carpeta_nc + file_nc, tiempos, lat, latb, lon, lonb, datavar)
            print('Creating file: ', carpeta_nc + file_nc)
            del tiempos, lat, latb, lon, lonb, datos, units
        print('------------------------------------------')

end = time.time()
print( np.round((end - start)/60.,3), ' minutes')
