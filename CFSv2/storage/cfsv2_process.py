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

base = '/shera/datos/CFSv2/'
#base = '/vegeta/datos/CFSv2/'

base_out = '/shera/datos/SISSA/CFSv2/'
# /shera/datos/SISSA/CFSv2/dlwrf_sfc/2011/06/


cfnames = {'dlwrf':'downwelling_longwave_flux_in_air', 'uswrf':'upwelling_shortwave_flux_in_air',
        'dswrf':'downwelling_shortwave_flux_in_air', 'ulwrf':'upwelling_longwave_flux_in_air',
        'prate':'precipitation_flux', 'sp':'surface_air_pressure',
        't2m':'air_temperature','sh2':'specific_humidity',
        'tmax':'air_temperature','tmin':'air_temperature'}
cn_var = {'dlwrf':'dlwrf_sfc', 'uswrf':'uswrf_sfc', 'dswrf':'dswrf_sfc', 'ulwrf':'ulwrf_sfc', 'u10':'ugrd_hgt',
          'v10':'vgrd_hgt','prate':'prate', 'sp':'pres_sfc', 't2m':'tmp_2m', 'sh2':'spfh_2m', 'tmax':'tmax_2m',
          'tmin':'tmin_2m'}
coordenadas = ['step', 'time', 'latitude', 'longitude', 'valid_time', 'surface','height', 'heightAboveGround']
years = np.arange(2002,2010)
months = [str(a).zfill(2) for a in np.arange(7,13)]
lonwest = -82
loneast = -33
latsouth = -57
latnorth = -8.5

start = time.time()
for anio in years:
    print('#### Trabajando en el year:', anio, ' ####')
    for mes in np.arange(7, 13):
        print('### Trabajando en el mes:', mes, ' ###')
        directorios = sorted(os.listdir(base + str(anio) + '/' + str(mes).zfill(2) + '/'))
        for j in directorios:
            print('## Trabajando en la carpeta:', j, ' ##')
            for i in ['00', '06', '12', '18']:
                print('# Trabajando en la hora:', i, ' #')
                print( 'Trabajando archivos viento')
                #---- Viento
                archivos = base + str(anio) + '/' + str(mes).zfill(2) + '/' + j +\
                           '/flxf*.01.' + j + i + '.grb2'
                print(archivos)
                if not list(glob.glob(archivos)):
                    print('NO HAY ARCHIVOS')
                    continue
                ds = xr.open_mfdataset(archivos, engine='cfgrib', combine='nested',
                        concat_dim='step', parallel=True,
                        backend_kwargs={ "filter_by_keys": {"typeOfLevel": "heightAboveGround", 'level':10 },
                            "indexpath": "",'errors': 'ignore'})
                '''
                # Test para ver como queda el dato sin interpolar
                for v in ds.variables:
                    if v not in coordenadas:
                        ds[v].to_netcdf(v + '.01.' + j + i + '_original.nc')
                '''
                ds = ds.sel(latitude=slice(latnorth, latsouth), longitude=slice(360 + lonwest, 360 + loneast))
                for v in ['u10', 'v10']:
                    if v not in coordenadas:
                        c_out = base_out + cn_var[v] + '/' + str(anio) + '/' + str(mes).zfill(2) + '/'
                        os.makedirs(c_out, exist_ok=True)
                        f_out = c_out + cn_var[v] + '.01.' + j + i + '.nc'
                        if os.path.isfile(f_out):
                            print('&&& Ya existe el archivo:', f_out)
                            continue
                        print('$ Guardando archivo:', f_out, '$')
                        esquema = iris.analysis.Linear()
                        cubo = ds[v].to_iris()
                        cubo_reg = cubo.regrid(v025, esquema)
                        iris.save(cubo_reg, f_out)
                        #ds[v].to_netcdf(v + '.01.' + j + i + '_SISSA.nc')
                #---- Precipitacion
                print( 'Trabajando archivos de lluvia')
                archivos = base + str(anio) + '/' + str(mes).zfill(2) + '/' + j +\
                        '/flxf*.01.' + j + i + '.grb2'
                print(archivos)
                ds = xr.open_mfdataset(archivos, engine='cfgrib', combine='nested', concat_dim='step',
                        parallel=True,
                        backend_kwargs={"filter_by_keys": {"typeOfLevel": "surface"},
                                        "indexpath": "",})
                '''
                # Test para ver como queda el dato sin interpolar
                for v in ds.variables:
                    if v not in coordenadas:
                        ds[v].to_netcdf(v + '.01.' + j + i + '_original.nc')
                '''
                ds = ds.sel(latitude=slice(latnorth, latsouth), longitude=slice(360 + lonwest, 360 + loneast))
                for v in ds.variables:
                    if v not in coordenadas:
                        print(v)
                        #
                        c_out = base_out + cn_var[v] + '/' + str(anio) + '/' + str(mes).zfill(2) + '/'
                        os.makedirs(c_out, exist_ok=True)
                        f_out = c_out + cn_var[v] + '.01.' + j + i + '.nc'
                        if os.path.isfile(f_out):
                            print('&&& Ya existe el archivo:', f_out)
                            continue
                        print('$ Guardando archivo:', f_out, '$')
                        #
                        ds[v].attrs['standard_name'] = cfnames[v]
                        cubo = ds[v].to_iris()
                        if v == 'prate':
                            cubo.coord('longitude').guess_bounds()
                            cubo.coord('latitude').guess_bounds()
                            esquema = iris.analysis.AreaWeighted(mdtol=0.5)
                        else:
                            esquema = iris.analysis.Linear()
                        cubo_reg = cubo.regrid(v025, esquema)
                        iris.save(cubo_reg, f_out)
                        #ds[v].to_netcdf(v + '.01.' + j + i + '_SISSA.nc')
                #---- Temperaturas
                print( 'Trabajando archivos de temperatura')
                archivos = base + str(anio) + '/' + str(mes).zfill(2) + '/' + j +\
                        '/flxf*.01.' + j + i + '.grb2'
                print(archivos)
                ds = xr.open_mfdataset(archivos, engine='cfgrib', combine='nested', concat_dim='step',
                        parallel=True,
                        backend_kwargs={"filter_by_keys": {"typeOfLevel": "heightAboveGround", 'level':2 },
                                        "indexpath": "", 'errors': 'ignore'})
                '''
                # test para ver como queda el dato sin interpolar
                for v in ds.variables:
                    if v not in coordenadas:
                        ds[v].to_netcdf(v + '.01.' + j + i + '_original.nc')
                '''
                ds = ds.sel(latitude=slice(latnorth, latsouth), longitude=slice(360 + lonwest, 360 + loneast))
                for v in ds.variables:
                    if v not in coordenadas:
                        print(v)
                        #
                        c_out = base_out + cn_var[v] + '/' + str(anio) + '/' + str(mes).zfill(2) + '/'
                        os.makedirs(c_out, exist_ok=True)
                        f_out = c_out + cn_var[v] + '.01.' + j + i + '.nc'
                        if os.path.isfile(f_out):
                            print('&&& Ya existe el archivo:', f_out)
                            continue
                        print('$ Guardando archivo:', f_out, '$')
                        #
                        ds[v].attrs['standard_name'] = cfnames[v]
                        esquema = iris.analysis.Linear()
                        cubo = ds[v].to_iris()
                        cubo_reg = cubo.regrid(v025, esquema)
                        iris.save(cubo_reg, f_out)
                        #ds[v].to_netcdf(v + '.01.' + j + i + '_SISSA.nc')
end = time.time()
print(end-start, 'segundos')
