import os
import numpy as np
from netCDF4 import Dataset, num2date
import pandas as pd
import datetime as dt

def crea_netcdf_distrib(ncfile, plazo, fechas, lat, lon):
    ds = Dataset(ncfile, 'w', format='NETCDF4')
    #---- Atributos globales
    ds.title = 'Archivo distribucion para plazo pronostico: ' + str(plazo).zfill(2)
    ds.source = 'Datos diarios GEFSv12 corregidos para SISSA'
    ds.references = ''
    ds.comment = ''

    #---- Dimension Tiempo
    ds.createDimension('time', None)
    t0 = ds.createVariable('time', 'u8', ('time',))
    t0.long_name = 'time'
    t0.standard_name = 'time'
    t0.units = 'days since 1900-01-01 00:00:00'
    t0.calendar = 'proleptic_gregorian'
    t0.axis = 'T'
    t0[:] = fechas

    #---- Dimension Lat
    ds.createDimension('lat', len(lat))
    #---- Variable lat
    y0 = ds.createVariable('lat', 'f8', ('lat',))
    y0.units = 'degrees_north'
    y0.long_name = 'latitude'
    y0.standard_name = 'latitude'
    y0.axis = 'Y'
    y0[:] = lat

    #---- Dimension Lon
    ds.createDimension('lon', len(lon))
    #---- Variable lon
    x0 = ds.createVariable('lon', 'f8', ('lon',))
    x0.units = 'degrees_east'
    x0.long_name = 'longitude'
    x0.standard_name = 'longitude'
    x0.axis = 'X'
    x0[:] = lon

    return ds
#
variable = 'tmax'
carpeta = '/shera/datos/SISSA/Diarios/GEFSv12_corr/'
carpeta_var = carpeta + variable + '/'
carpeta_dist = '/shera/datos/SISSA/Distrib/GEFSv12_corr/' + variable + '/'
fechas = pd.bdate_range(start='2010-01-06', end='2019-12-25', freq='W-WED')
plazos = np.arange(0,34)
ens_member = ['c00', 'p01', 'p02', 'p03', 'p04', 'p05', 'p06', 'p07', 'p08', 'p09', 'p10']
#
os.makedirs(carpeta_dist, exist_ok=True)
#
nt = len(fechas)
nens = len(ens_member)
ny = 187
nx = 189
#
for plazo in plazos:
    print('Trabajando en el plazo:', plazo)
    dist_file = carpeta_dist + variable + '_' + str(plazo).zfill(2) + '.nc'
    print(dist_file)
    fechas_p = []
    for ens in ens_member:
        print(ens)
        datos = np.empty((nt,ny,nx))
        datos[:] = np.nan
        for it, fecha in enumerate(fechas):
            fechaf = fecha.strftime('%Y%m%d')
            year = fecha.strftime('%Y')
            ncfile = carpeta_var + year + '/' + fechaf + '/' + variable + '_' + fechaf + '_' + ens + '.nc'
            if not os.path.exists(ncfile):
                print('No existe el archivo:', ncfile)
                continue
            nc = Dataset(ncfile, 'r')
            datos[it,:,:] = nc.variables[variable][plazo,:,:]
            standard_name = nc.variables[variable].standard_name
            units = nc.variables[variable].units
            long_name = ens + ' ' + nc.variables[variable].long_name
            if ens == 'c00':
                lat = nc.variables['lat'][:]
                lon = nc.variables['lon'][:]
                aux_t = nc.variables['time'][:]
                fechas_p.append(aux_t[plazo])
            nc.close()
        #---- Guardamos el dato para cada miembro de ensamble
        '''
        if ens == 'c00':  # Solo para el primer miembro abrimos el archivo y luego guardamos en el
            if len(fechas_p) < nt:
                print('Corrigiendo fechas_p')
                aux_dist_file =  '/shera/datos/SISSA/Distrib/GEFSv12/tmean/tmean_' + str(plazo).zfill(2) + '.nc'
                print(aux_dist_file)
                ncaux = Dataset(aux_dist_file, 'r')
                fechas_p = ncaux['time'][:]
                ncaux.close()
        '''
        if ens == 'c00':
            ds = crea_netcdf_distrib(dist_file, plazo, fechas_p, lat, lon)
    fvar = ds.createVariable(ens, datatype='f8', dimensions=('time', 'lat', 'lon'))
    fvar.standard_name = standard_name
    fvar.units = units
    fvar.long_name = long_name
    fvar[:] = datos
        
    ds.close()

print('--------- Termino de procesar datos -----------')

