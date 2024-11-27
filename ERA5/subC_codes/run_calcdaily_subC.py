import os
from netCDF4 import Dataset, num2date, date2num
import numpy as np
import numpy.ma as ma
import xarray as xr


import sys
sys.path.append('../lib/')
from erah_class import erah_class


def create_fichero_netcdf(v_folder, year, latis, lonis, times):
    namefile = v_folder + str(year) + '.nc'
    print('Generando Archivo: ' + namefile)

    f = Dataset(namefile, 'w', format='NETCDF4')
    time_unit = 'days since 1900-01-01 00:00:00'
    calendar_u = 'proleptic_gregorian'
    #-- DIMENSIONES
    f.createDimension('time', None)
    f.createDimension('latitude', len(latis))
    f.createDimension('longitude', len(lonis))
    #-- Variables Dimensiones
    tiempos = f.createVariable('time', 'u8', ('time',))
    lats = f.createVariable('latitude', 'f4', ('latitude',))
    lons = f.createVariable('longitude', 'f4', ('longitude',))
    #-- Valores Dimensiones lat/lon
    lats[:] = latis
    lons[:] = lonis
    tiempos[:] = date2num(times, units=time_unit, calendar=calendar_u)
    #-- Caracteristicas Dimensiones
    # Tiempo
    tiempos.units = time_unit
    tiempos.calendar = calendar_u
    # Lat
    lats.units = 'degrees_north'
    lats.long_name = 'latitude'
    # Lon
    lons.units = 'degrees_east'
    lons.long_name = 'longitude'


    return f

def crea_variable_estacion(nc_arch, nvar, varn, fval, unit, long_name):
    '''
    ncfile.createVariable('temp',np.float64,('time','lat','lon'))
    '''
    pp_o = nc_arch.createVariable(nvar, 'f8', ('time','latitude','longitude'), fill_value=fval)
    pp_o.units = unit
    pp_o.long_name = long_name
    nc_arch.variables[nvar][:] = varn

###########################################################################
###########################################################################
###########################################################################

daily_vars = ['tmax', 'tmin', 'tmean', 'rain']
hourly_vars = ['t2m', 't2m', 't2m','tp']

for var, hvar in zip(daily_vars,hourly_vars):
    print(var, hvar)
    v_folder = '/datos2/SISSA/ERA5/' + var + '/'
    os.makedirs(v_folder, exist_ok=True)
    for year in np.arange(1998,1999):
        print(year)
        a = erah_class(year)
        latis = a.lat
        lonis = a.lon
        times = a.get_daily_datetimes()
        d1, m1, u1, fval = a.get_variables(hvar)
        datos = {}; mask = {}
        datos[hvar] = d1; mask[hvar] = m1
        dato_diario = a.calc_daily(var, datos, mask)

        ncf = create_fichero_netcdf(v_folder, year, latis, lonis, times)
        unit = a.units_daily[var]
        long_name = a.longname_daily[var]
        dato = ma.getdata(dato_diario[var])
        crea_variable_estacion(ncf, var, dato, fval, unit, long_name)

        ncf.close()
