import glob
import xarray as xr
import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta


def open_archivo(archivo):
    ds = xr.open_dataset(archivo, engine='cfgrib',
            backend_kwargs={"filter_by_keys": {"typeOfLevel": "heightAboveGround", 'level':2},
                "indexpath": "",})
    variables = ['time', 'step', 'latitude', 'longitude', 'prate', 'u10', 'v10', 'tmax', 'tmin',
                 't2m', 'sh2m', 'dlwrf', 'dswrf', 'ulwrf', 'pres']
    ds = ds.drop([i for i in ds.variables if i not in variables])
    #ds = ds.sel(latitude=slice(-8, -57), longitude=slice(360 - 82, 360 - 33))
    
    return ds


coordenadas = ['step', 'time', 'latitude', 'longitude', 'valid_time']
base = '/shera/datos/CFSv2/'

for anio in np.arange(2000, 2001):
    print(anio)
    for mes in np.arange(1, 2):
        directorios = sorted(os.listdir('/shera/datos/CFSv2/' + str(anio) + '/' + str(mes).zfill(2) + '/'))
        for j in directorios[0:1]:
            for i in ['00', '06', '12', '18']:
                archivos = base + str(anio) + '/' + str(mes).zfill(2) + '/' + j +\
                           '/flxf*.01.' + j + i + '.grb2'
                new_carpeta = base + str(anio) + '/' + str(mes).zfill(2) + '/' + j + i + '/'
                print(new_carpeta)
                os.makedirs(new_carpeta, exist_ok=True)
                a1 = sorted(glob.glob(archivos))
                print(len(a1))
                lista = [open_archivo(i) for i in a1]
                ds_t = xr.concat(lista, dim='step')
                for v in ds_t.variables:
                    if v not in coordenadas:
                        ds_t[v].to_netcdf(new_carpeta + v + '.01.' + j + i + '.daily.nc')
