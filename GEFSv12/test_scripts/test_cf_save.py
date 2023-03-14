import numpy as np
import pandas as pd
import xarray as xr
import time
import datetime as dt
import cartopy.crs as ccrs
import matplotlib.pyplot as plt

from netCDF4 import date2num, Dataset

start = time.time()

f1 = '/shera/datos/GEFSv12/apcp_sfc/2000/2000010500/d1-10/apcp_sfc_2000010500_c00.grib2'
print(f1)
ds1 = xr.open_dataset(f1, engine='pynio')
var = list(ds1.data_vars)[0]

# Get Tiempos para guardar en netcdf
seg_aux = ds1.forecast_time0.values
horas = [(x.astype('timedelta64[h]') / np.timedelta64(1, 'h')).astype(int)  for x in seg_aux]
ini_time = dt.datetime.strptime(ds1[var].initial_time, '%m/%d/%Y (%H:%M)')
tiempos = [ini_time + dt.timedelta(hours=int(hora)) for hora in horas]
time_units = 'hours since 2000-01-01 00:00'
time_calendar = 'proleptic_gregorian'
time_vals = date2num(tiempos, units=time_units, calendar=time_calendar)
print(time_vals)

# Get Lon y Lat
ds1.coords['lon_0'] = (ds1.coords['lon_0'] + 180) % 360 - 180

min_lat = -9.5
max_lat = -56.
min_lon = -81.
max_lon = -34.
cropped_ds = ds1.sel(lat_0=slice(min_lat,max_lat), lon_0=slice(min_lon,max_lon))
lon = cropped_ds.coords['lon_0'].to_numpy()
lat = cropped_ds.coords['lat_0'].to_numpy()

# Get Data Matrix
datos = cropped_ds[var].to_numpy()
print(cropped_ds[var].attrs.keys())

# WRITE NETCDF

test_fname = 'archivo_025.nc'
print(test_fname)
ds = Dataset(test_fname, 'w', format='NETCDF4')

# Atributos globales
ds.Conventions = 'CF-1.7'
ds.title = 'ensemble member forecast run'
ds.institution = 'NCEP'
ds.source = 'GEFSv12 reforecast dataset'
ds.history = 'Process at SISSA on ' + dt.datetime.utcnow().strftime('%d/%m/%Y %H:%M')
ds.references = ''
ds.comment = ''

# Variable Tiempo
ds.createDimension('time',None)
t0 = ds.createVariable('time', 'u8',('time',))
t0.units = time_units
t0[:] = time_vals

# Variable Lat
ds.createDimension('lat', len(lat))
y0 = ds.createVariable('lat', 'f8', ('lat',))
y0.standard_name = 'latitude'
y0.units = 'degrees_north'
y0.long_name = 'latitude'
y0[:] = lat

# Variable Lon
ds.createDimension('lon', len(lon))
x0 = ds.createVariable('lon', 'f8', ('lon',))
x0.standard_name = 'longitude'
x0.units = 'degrees_east'
x0.long_name = 'longitude'
x0[:] = lon
####################################

fvar = ds.createVariable('precipitation', datatype='f8', dimensions=('time','lat','lon',))
fvar.standard_name = 'precipitation'
fvar.units = cropped_ds[var].units
fvar.long_name = '6hour_accumulated_precipitation'
#fvar._FillValue = cropped_ds[var]._FillValue
fvar[:] = datos


# ----- cerramos el archivo ----
ds.close()




end = time.time()
print( np.round(end - start,3), ' seconds')
