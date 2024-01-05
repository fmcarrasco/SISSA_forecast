'''
Funciones auxiliares para manipular datos de GEFSv12


'''

import numpy as np
import xarray as xr
import datetime as dt
from netCDF4 import date2num, Dataset


def convert_bytes(num):
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return np.round(num,2)
        num /= 1024


def get_cut_grib(nfile):
    import pandas as pd
    # -- Variables que nos quedamos
    variables = pd.read_csv('variables_aceptadas.txt')
    #
    ds = xr.open_dataset(nfile, engine='pynio')
    #----- Extraccion de tiempos -----
    fstring = nfile.split('/')[7].split('.')[0][4:]
    # Hora inicial
    tiempos = [dt.datetime.strptime(fstring, '%Y%m%d%H')]
    # Array de tiempos
    time_unit = 'hours since 1990-01-01 00:00'
    time_calendar = 'proleptic_gregorian'
    time_vals = date2num(tiempos, units=time_unit, calendar=time_calendar)
    #----- Recorte area SISSA ----
    min_lat = -8.5; max_lat = -57.
    min_lon = -82.; max_lon = -33.
    # Cambio 0-360 a -180-180 en Longitudes
    #ds.coords['lon_0'] = (ds.coords['lon_0'] + 180) % 360 - 180
    min_lon_360 = min_lon + 360.
    max_lon_360 = max_lon + 360.
    # Recortamos los datos al area SISSA
    cropped_ds = ds.sel(lat_0=slice(min_lat, max_lat), lon_0=slice(min_lon_360, max_lon_360))
    # Extraemos datos de Lon/Lat recortados
    lon = (cropped_ds.coords['lon_0'].to_numpy() +180) % 360 - 180
    lat = cropped_ds.coords['lat_0'].to_numpy()
    # Lon/Lat BOUNDS
    resol_lon = np.fabs(lon[1] - lon[0])
    resol_lat = np.fabs(lat[1] - lat[0])
    #
    lonb = np.empty((len(lon),2))
    lonb[:,0] = lon - resol_lon/2.
    lonb[:,1] = lon + resol_lon/2.
    latb = np.empty((len(lat),2))
    latb[:,0] = lat + resol_lat/2.
    latb[:,1] = lat - resol_lat/2.
    # ------
    # Extraemos el dato recortado como diccionario
    datos = {}
    unidades = {}
    for index, row in variables.iterrows():
        datos[row['var']] = cropped_ds[row['vargrb2']].to_numpy()
        unidades[row['var']] = cropped_ds[row['vargrb2']].units 
    # Cerramos los datos abiertos
    ds.close()
    cropped_ds.close()
    
    return time_vals, lat, latb, lon, lonb, datos, unidades


def save_netcdf(ncfile, tiempos, lat, latb, lon, lonb, datos):
    
    ds = Dataset(ncfile, 'w', format='NETCDF4')
    #---- Atributos globales
    ds.Conventions = 'CF-1.7'
    ds.title = 'ensemble member forecast run'
    ds.institution = 'NCEP'
    ds.source = 'CFS2 reforecast dataset'
    ds.history = 'Process for SISSA at ' + dt.datetime.utcnow().strftime('%d/%m/%Y %H:%M')
    ds.references = ''
    ds.comment = ''

    #---- Dimension Tiempo
    ds.createDimension('time', None)
    #---- Variable Tiempo
    t0 = ds.createVariable('time', 'u8', ('time',))
    t0.long_name = 'time'
    t0.standard_name = 'time'
    t0.units = 'hours since 1990-01-01 00:00'
    t0.calendar = 'proleptic_gregorian'
    t0.axis = 'T'
    t0[:] = tiempos

    #---- Dimension bounds
    ds.createDimension('bounds2', 2)

    #---- Dimension Lat
    ds.createDimension('lat', len(lat))
    #---- Variable lat BOUNDS
    y0b = ds.createVariable('lat_bnds', 'f8', ('lat', 'bounds2',))
    y0b[:] = latb
    #---- Variable lat
    y0 = ds.createVariable('lat', 'f8', ('lat',))
    y0.units = 'degrees_north'
    y0.long_name = 'latitude'
    y0.standard_name = 'latitude'
    y0.axis = 'Y'
    y0.bounds = 'lat_bnds'
    y0[:] = lat

    #---- Dimension Lon
    ds.createDimension('lon', len(lon))
    #---- Variable lon BOUNDS
    x0b = ds.createVariable('lon_bnds', 'f8', ('lon', 'bounds2',))
    x0b[:] = lonb
    #---- Varible lon
    x0 = ds.createVariable('lon', 'f8', ('lon',))
    x0.units = 'degrees_east'
    x0.long_name = 'longitude'
    x0.standard_name = 'longitude'
    x0.axis = 'X'
    x0.bounds = 'lon_bnds'
    x0[:] = lon

    #---- Agregamos el dato
    fvar = ds.createVariable(datos['nvar'], datatype='f8', dimensions=('time', 'lat', 'lon',))
    fvar.standard_name = datos['standard_name']
    fvar.units = datos['units']
    fvar.long_name = datos['long_name']
    fvar[:] = datos['valores']

    #---- Cerramos el archivo NetCDF
    ds.close()


