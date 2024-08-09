import glob
import xarray as xr
import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta


def open_archivo(archivo):
    ds = xr.open_dataset(archivo, engine='cfgrib',
            backend_kwargs={"filter_by_keys": {"typeOfLevel": "heightAboveGround", 'level':10 },
                "indexpath": "",})
    #ds = ds.sel(latitude=slice(-8, -57), longitude=slice(360 - 82, 360 - 33))
    
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
                '''
                ds = xr.open_mfdataset(archivos, engine='cfgrib', combine='nested',
                        concat_dim='step', parallel=True,
                        backend_kwargs={ "filter_by_keys": {"typeOfLevel": "heightAboveGround", 'level':10 },
                            "indexpath": "",'errors': 'ignore'},
                        preprocess=select_vars)
                ds = ds.sel(latitude=slice(-8.5, -57), longitude=slice(360 - 82, 360 - 33))
                for v in ['u10', 'v10']:
                    if v not in coordenadas:
                        ds[v].to_netcdf(v + '.01.' + j + i + '_SISSA.nc')
                
                '''
                a1 = sorted(glob.glob(archivos))
                lista = [open_archivo(i) for i in a1]
                ds_t = xr.concat(lista, dim='step')
                for v in ['u10', 'v10']:
                    if v not in coordenadas:
                        ds_t[v].to_netcdf(v + '.01.' + j + i + '_felix.nc')
                '''
                for archivo in a1:
                    print(archivo)
                    ds_t = xr.open_dataset(archivo, engine='cfgrib',
                        backend_kwargs={"filter_by_keys": {"typeOfLevel": "heightAboveGround", 'level':10 },
                                        "indexpath": "",})
                    print(list(ds_t.variables))
                '''
