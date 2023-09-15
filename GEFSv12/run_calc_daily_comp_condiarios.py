import sys
import pandas as pd
import numpy as np
import xarray as xr
from netCDF4 import Dataset
sys.path.append('./lib/')
from aux_functions import save_netcdf

import time
import os
start = time.time()


def calc_tdmean(rh, tmean):
    num = 243.04*(np.log(rh/100.)+((17.625*tmean)/(243.04+tmean)))
    den = (17.625-np.log(rh/100.)-((17.625*tmean)/(243.04+tmean)))
    return num/den # en Celsius

def calc_pvmean(tdmean):
    num = 17.67*tdmean
    den = tdmean - 243.5
    frac = num/den
    return 6.112*np.exp(frac) # en Pa

v0 = 'tdmean' # Variable a calcular
v1 = 'rh' # Datos de Entrada
v2 = 'tmean' # Datos de Entrada

fechas = pd.bdate_range(start='2000-01-05', end='2019-12-25', freq='W-WED')
#fechas = pd.bdate_range(start='2004-12-08', end='2004-12-08', freq='W-WED')
ensembles = ['c00', 'p01', 'p02', 'p03', 'p04', 'p05', 'p06', 'p07', 'p08', 'p09', 'p10']
carpeta = '/shera/datos/SISSA/Diarios/GEFSv12/' + v0 + '/'
os.makedirs(carpeta, exist_ok=True)

c_daily = '/shera/datos/SISSA/Diarios/GEFSv12/'

for ffecha in fechas:
    yr = ffecha.strftime('%Y/')
    print(ffecha)
    fecha = ffecha.strftime('%Y%m%d')
    carpeta_f = carpeta + yr + fecha + '/'
    os.makedirs(carpeta_f, exist_ok=True)
    for nens in ensembles:
        f1 = c_daily + v1 + '/' + yr + fecha + '/' + v1 + '_' + fecha + '_' + nens + '.nc'
        f2 = c_daily + v2 + '/' + yr + fecha + '/' + v2 + '_' + fecha + '_' + nens + '.nc'
        exist_f1 = os.path.isfile(f1)
        exist_f2 = os.path.isfile(f2)
        if exist_f1 & exist_f2:
            a1 = xr.open_dataset(f1)
            a2 = xr.open_dataset(f2)
            n1 = Dataset(f1, 'r')
            lat = n1['lat'][:]
            latb = n1['lat_bnds'][:]
            lon = n1['lon'][:]
            lonb = n1['lon_bnds'][:]
            tiempos = n1['time'][:]
            n1.close()
        else:
            print('No existen los dos archivos para calculo')
            print(f1)
            print(exist_f1)
            print(f2)
            print(exist_f2)
            continue
        tdmean = calc_tdmean(a1[v1], a2[v2])
        datos = {'nvar':v0, 'standard_name': 'mean_dewpoint_temperature', 'units':'Celsius', 
                'long_name': '2m mean Dewpoint Temperature 0-23 UTC', 'valores':tdmean.values } 
        #
        ncfile =  carpeta_f + v0 + '_' + fecha + '_' + nens + '.nc'
        print('### Guardando archivo diario', ncfile, ' ###')
        save_netcdf(ncfile, tiempos, lat, latb, lon, lonb, datos)
end = time.time()
print( np.round(end - start,3)/60., ' minutes')

