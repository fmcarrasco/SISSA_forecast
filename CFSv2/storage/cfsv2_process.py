import time
import glob
import xarray as xr
import pandas as pd
import os
import iris
import numpy as np
from datetime import datetime, timedelta
#from aux_functions import open_archivo_viento
#from aux_functions import open_archivo_surface
#from aux_functions import open_archivo_temp

#############################
# Informacion para interpolar a reticula 0.25 simil GEFSv12
nctestigo = '../calc_daily/archivo_guia/tmax_20000105_c00.nc'
c1 = iris.load(nctestigo)
#if variable == 'apcp_sfc':
#    esquema = iris.analysis.AreaWeighted(mdtol=0.5)
#else:
#    esquema = iris.analysis.Linear()
#Obtenemos el cubo
v025 = c1[0]

#base = '/shera/datos/CFSv2/'
base = '/vegeta/datos/CFSv2/'

cfnames = {'dlwrf':'downwelling_longwave_flux_in_air', 'uswrf':'upwelling_shortwave_flux_in_air',
        'dswrf':'downwelling_shortwave_flux_in_air', 'prate':'precipitation_flux',
        'sp':'surface_air_pressure', 't2m':'air_temperature','sh2':'specific_humidity',
        'tmax':'air_temperature','tmin':'air_temperature'}
coordenadas = ['step', 'time', 'latitude', 'longitude', 'valid_time', 'surface','height', 'heightAboveGround']
years = np.arange(2000,2001)
months = [str(a).zfill(2) for a in np.arange(1,13)]
lonwest = -82
loneast = -33
latsouth = -57
latnorth = -8.5

start = time.time()
for anio in years:
    for mes in np.arange(2, 3):
        directorios = sorted(os.listdir(base + str(anio) + '/' + str(mes).zfill(2) + '/'))
        for j in directorios[0:1]:
            for i in ['00']:#['00', '06', '12', '18']:
                #---- Viento
                archivos = base + str(anio) + '/' + str(mes).zfill(2) + '/' + j +\
                           '/flxf*.01.' + j + i + '.grb2'
                print(archivos)
                ds = xr.open_mfdataset(archivos, engine='cfgrib', combine='nested',
                        concat_dim='step', parallel=True,
                        backend_kwargs={ "filter_by_keys": {"typeOfLevel": "heightAboveGround", 'level':10 },
                            "indexpath": "",'errors': 'ignore'})
                for v in ds.variables:
                    if v not in coordenadas:
                        ds[v].to_netcdf(v + '.01.' + j + i + '_original.nc')
                ds = ds.sel(latitude=slice(latnorth, latsouth), longitude=slice(360 + lonwest, 360 + loneast))
                for v in ['u10', 'v10']:
                    if v not in coordenadas:
                        esquema = iris.analysis.Linear()
                        cubo = ds[v].to_iris()
                        cubo_reg = cubo.regrid(v025, esquema)
                        iris.save(cubo_reg, v + '.01.' + j + i + '_SISSA.nc')
                        #ds[v].to_netcdf(v + '.01.' + j + i + '_SISSA.nc')
                #---- Precipitacion
                archivos = base + str(anio) + '/' + str(mes).zfill(2) + '/' + j +\
                        '/flxf*.01.' + j + i + '.grb2'
                print(archivos)
                ds = xr.open_mfdataset(archivos, engine='cfgrib', combine='nested', concat_dim='step',
                        parallel=True,
                        backend_kwargs={"filter_by_keys": {"typeOfLevel": "surface"},
                                        "indexpath": "",})
                for v in ds.variables:
                    if v not in coordenadas:
                        ds[v].to_netcdf(v + '.01.' + j + i + '_original.nc')
                ds = ds.sel(latitude=slice(latnorth, latsouth), longitude=slice(360 + lonwest, 360 + loneast))
                for v in ds.variables:
                    if v not in coordenadas:
                        print(v)
                        ds[v].attrs['standard_name'] = cfnames[v]
                        cubo = ds[v].to_iris()
                        print(cubo)
                        if v == 'prate':
                            cubo.coord('longitude').guess_bounds()
                            cubo.coord('latitude').guess_bounds()
                            esquema = iris.analysis.AreaWeighted(mdtol=0.5)
                        else:
                            esquema = iris.analysis.Linear()
                        cubo_reg = cubo.regrid(v025, esquema)
                        iris.save(cubo_reg, v + '.01.' + j + i + '_SISSA.nc')
                        #ds[v].to_netcdf(v + '.01.' + j + i + '_SISSA.nc')
                #---- Temperaturas
                archivos = base + str(anio) + '/' + str(mes).zfill(2) + '/' + j +\
                        '/flxf*.01.' + j + i + '.grb2'
                print(archivos)
                ds = xr.open_mfdataset(archivos, engine='cfgrib', combine='nested', concat_dim='step',
                        parallel=True,
                        backend_kwargs={"filter_by_keys": {"typeOfLevel": "heightAboveGround", 'level':2 },
                                        "indexpath": "", 'errors': 'ignore'})
                for v in ds.variables:
                    if v not in coordenadas:
                        ds[v].to_netcdf(v + '.01.' + j + i + '_original.nc')
                ds = ds.sel(latitude=slice(latnorth, latsouth), longitude=slice(360 + lonwest, 360 + loneast))
                for v in ds.variables:
                    if v not in coordenadas:
                        print(v)
                        ds[v].attrs['standard_name'] = cfnames[v]
                        esquema = iris.analysis.Linear()
                        cubo = ds[v].to_iris()
                        print(cubo)
                        cubo_reg = cubo.regrid(v025, esquema)
                        iris.save(cubo_reg, v + '.01.' + j + i + '_SISSA.nc')
                        #ds[v].to_netcdf(v + '.01.' + j + i + '_SISSA.nc')
end = time.time()
print(end-start, 'segundos')
