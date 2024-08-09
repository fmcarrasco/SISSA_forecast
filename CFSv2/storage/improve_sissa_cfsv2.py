import xarray as xr
import os
import glob
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
folder = '/shera/datos/SISSA/CFSv2/'
var_dir = sorted(os.listdir(folder))

for var in var_dir[0:1]:
    for year in np.arange(2000, 2001):
        for month in np.arange(1,2):
            subf = folder + var + '/' + str(year) + '/' + str(month).zfill(2) + '/'
            ddir = sorted(os.listdir(subf))
            for directorio in ddir[0:1]:
                for hora in ['00', '06', '12', '18']:
                    archivos = sorted(glob.glob(subf + directorio + '/' + hora + '/*.nc'))
                    print(archivos[10])
                    ds = xr.open_dataset(archivos[10])
                    print(ds)
                    exit()
                    


for anio in np.arange(2000, 2001):
    for mes in np.arange(1, 2):
        var_dir = sorted(os.listdir(folder))
        for var in var_dir[0:1]:
            for i in ['00', '06', '12', '18']:
                #---- Viento
                archivos = '/shera/datos/CFSv2/' + str(anio) + '/' + str(mes).zfill(2) + '/' + j +\
                           '/flxf*.01.' + j + i + '.grb2'
                print(archivos)
                ds = xr.open_mfdataset(archivos, engine='cfgrib', combine='nested',
                        concat_dim='step', parallel=True,
                        backend_kwargs={"filter_by_keys": {"typeOfLevel": "heightAboveGround", 'level':10 },
                            "indexpath": "",})
                        #preprocess=select_vars)
                print(ds)
                exit()
                ds = ds.sel(latitude=slice(-8, -57), longitude=slice(360 - 81, 360 - 33))
                for v in ['u10', 'v10']:
                    if v not in coordenadas:
                        ds[v].to_netcdf(v + '.01.' + j + i + '_SISSA.nc')
                #---- Precipitacion
                archivos = '/shera/datos/CFSv2/' + str(anio) + '/' + str(mes).zfill(2) + '/' + j +\
                        '/flxf*.01.' + j + i + '.grb2'
                print(archivos)
                ds = xr.open_mfdataset(archivos, engine='cfgrib', combine='nested', concat_dim='step',
                        parallel=True,
                        backend_kwargs={"filter_by_keys": {"typeOfLevel": "surface"},
                                        "indexpath": "",},
                        preprocess=select_vars)
                ds['prate'].to_netcdf('prate.01.' + j  + i + '.nc')
                ds = ds.sel(latitude=slice(-8, -57), longitude=slice(360 - 81, 360 - 33))
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
                                        "indexpath": "",},
                        preprocess=select_vars)
                for v in ['tmax', 'tmin', 't2m']:
                    if v not in coordenadas:
                        ds[v].to_netcdf(v + '.01.' + j + i + '.nc')
                ds = ds.sel(latitude=slice(-8, -57), longitude=slice(360 - 81, 360 - 33))
                for v in ds.variables:
                    if v not in coordenadas:
                        ds[v].to_netcdf(v + '.01.' + j + i + '_SISSA.nc')

