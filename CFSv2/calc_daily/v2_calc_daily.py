import time
import iris
import datetime as dt
import xarray as xr
import numpy as np
import pandas as pd
import glob
import os
from funciones import variable_diaria, get_lat_lon_bnds

import sys

# Variables dadas en el llamado
year = sys.argv[1]
mes = sys.argv[2].zfill(2)
variable = sys.argv[3]

var_d = variable_diaria(variable)

var_atrs = {'standard_name':'air_temperature', 'units':'degC', 'long_name':'minimum temperature at 2m'}
lat_atrs = {'axis':'Y','bounds':'lat_bnds','units':'degrees_north','standard_name':'latitude',
            'long_name':'latitude'}
lon_atrs = {'axis':'X','bounds':'lon_bnds','units':'degrees_east','standard_name':'longitude',
            'long_name':'longitude'}

######################################
print('Working in year ' + year)
print('at month:', mes)
c0 = '/shera/datos/SISSA/CFSv2/'
c1 = '/shera/datos/SISSA/Diarios/CFSv2/'
carpeta_i = c0 + variable + '/' + year + '/' + mes + '/'
carpeta_o = c1 + var_d + '/' + year + '/' + mes + '/'
os.makedirs(carpeta_o, exist_ok=True)

archivos = sorted(glob.glob(carpeta_i + '/*.nc'))
print(carpeta_i)
print(carpeta_o)
print(var_d)
starttime_carpeta = time.time()
for archivo in archivos:
    print(archivo)
    n_archivo = var_d + '.' + '.'.join(os.path.basename(archivo).split('.')[1:])
    ds = xr.open_dataset(archivo)
    print('Procesando archivo:', archivo)
    if var_d == 'tmean':
        var = ds['t2m']
    else:
        var = ds[var_d]
    var = var.rename(var_d)
    # Calculos Diarios
    if var_d == 'tmin':
        daily = var.resample(valid_time='1D').min() - 273.15
    elif var_d == 'tmax':
        daily = var.resample(valid_time='1D').max() - 273.15
    elif var_d == 'tmean':
        daily = var.resample(valid_time='1D').mean() - 273.15

    daily.valid_time.encoding["units"] = 'days since 1900-01-01 00:00:00'
    daily = daily.rename({'valid_time':'time', 'time':'initial_time'})
    daily = daily.drop_vars('heightAboveGround')
    # Recolectamos datos para dataset final
    datos = daily.to_numpy()
    lat = daily.lat.to_numpy()
    lon = daily.lon.to_numpy()
    # Calculamos los bordes de lat/lon para aplicaciones de interpolacion
    latb, lonb = get_lat_lon_bnds(lat, lon)
    # Generamos dataset final
    ds = xr.Dataset(data_vars=dict(tmin=(['time', 'lat', 'lon'], datos, var_atrs),
                                   lat_bnds=(['lat', 'bnds'], latb),
                                   lon_bnds=(['lon', 'bnds'], lonb)),
                                   coords=dict(lon=('lon', lon,lon_atrs),
                                               lat=('lat', lat, lat_atrs),
                                               time=daily.time,
                                               bnds=[0,1],),
                                   attrs=dict(informacion='Datos CFSv2 diarios procesados para SISSA'))
    outfile = carpeta_o + n_archivo
    print('Guardando archivo:', outfile)
    ds.to_netcdf(outfile)
# END FOR
endtime_carpeta = time.time()
carpeta_sec = endtime_carpeta - starttime_carpeta
carpeta_min = np.round(carpeta_sec/60.,2)
carpeta_hou = np.round(carpeta_min/60.,2)
print('Minutos por carpeta', carpeta_min)
print('Horas por carpeta', carpeta_hou)

