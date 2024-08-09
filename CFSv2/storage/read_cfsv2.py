import glob
import xarray as xr
import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta


def select_vars(ds):
    variables = ['time', 'step', 'latitude', 'longitude', 'prate', 'u10', 'v10', 'tmax', 'tmin',
                 't2m', 'sh2m', 'dlwrf', 'dswrf', 'ulwrf', 'pres']
    ds = ds.drop([i for i in ds.variables if i not in variables])
    return ds
#dlwrf_sfc    spfh_2m  tmin_2m  ugrd_hgt   vgrd_hgt
#dswrf_sfc  pres_sfc  tmax_2m  tmp_2m   ulwrf_sfc
coordenadas = ['step', 'time', 'latitude', 'longitude', 'valid_time']

for anio in np.arange(2001, 2002):
    for mes in np.arange(2, 3):
        directorios = sorted(os.listdir('/shera/datos/CFSv2/' + str(anio) + '/' + str(mes).zfill(2) + '/'))
        for j in directorios[0:1]:
            for i in ['00', '06', '12', '18']:
                #---- Viento
                archivos = '/shera/datos/CFSv2/' + str(anio) + '/' + str(mes).zfill(2) + '/' + j +\
                           '/flxf200102*.01.' + j + i + '.grb2'
                print(archivos)
                ds = xr.open_mfdataset(archivos, engine='cfgrib', combine='nested',
                        concat_dim='step', parallel=True,
                        backend_kwargs={ "filter_by_keys": {"typeOfLevel": "heightAboveGround", 'level':10 },
                            "indexpath": "",'errors': 'ignore'},
                        preprocess=select_vars)
                ds = ds.sel(latitude=slice(-8.5, -57), longitude=slice(360 - 82, 360 - 33))
                for v in ['u10', 'v10']:
                    if v not in coordenadas:
                        ds[v].to_netcdf(v + '.01.' + j + i + '_SISSA.nc')
                #---- Precipitacion
                archivos = '/shera/datos/CFSv2/' + str(anio) + '/' + str(mes).zfill(2) + '/' + j +\
                        '/flxf*.01.' + j + i + '.grb2'
                print(archivos)
                #a1 = glob.glob(archivos)
                #ds_t = xr.open_dataset(a1[35], engine='cfgrib',
                #        backend_kwargs={"filter_by_keys": {"typeOfLevel": "heightAboveGround", 'level':10 },
                #                        "indexpath": "",})
                #print(ds_t)
                #exit()
                ds = xr.open_mfdataset(archivos, engine='cfgrib', combine='nested', concat_dim='step',
                        parallel=True,
                        backend_kwargs={"filter_by_keys": {"typeOfLevel": "surface"},
                                        "indexpath": "",},
                        preprocess=select_vars)
                ds = ds.sel(latitude=slice(-8.5, -57), longitude=slice(360 - 82, 360 - 33))
                for v in ds.variables:
                    if v not in coordenadas:
                        ds[v].to_netcdf(v + '.01.' + j + i + '_SISSA.nc')
                #---- Temperaturas
                archivos = '/shera/datos/CFSv2/' + str(anio) + '/' + str(mes).zfill(2) + '/' + j +\
                        '/flxf*.01.' + j + i + '.grb2'
                print(archivos)
                ds = xr.open_mfdataset(archivos, engine='cfgrib', combine='nested', concat_dim='step',
                        parallel=True,
                        backend_kwargs={"filter_by_keys": {"typeOfLevel": "heightAboveGround", 'level':2 },
                                        "indexpath": "", 'errors': 'ignore'},
                        preprocess=select_vars)
                ds = ds.sel(latitude=slice(-8.5, -57), longitude=slice(360 - 82, 360 - 33))
                for v in ds.variables:
                    if v not in coordenadas:
                        ds[v].to_netcdf(v + '.01.' + j + i + '_SISSA.nc')

