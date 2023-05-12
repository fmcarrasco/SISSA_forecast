import numpy as np
import pandas as pd
import xarray as xr
import time
import datetime as dt
import cartopy.crs as ccrs
import matplotlib.pyplot as plt

from cftime import date2num

start = time.time()

variables = ['apcp_sfc', 'dlwrf_sfc', 'dswrf_sfc', 'pres_sfc', 'tmax_2m', 'tmin_2m',
             'tmp_2m', 'ulwrf_sfc', 'ugrd_hgt', 'vgrd_hgt']
'''
print('----------------------------')
for var in variables:
    print(var)
    f2 = '/shera/datos/GEFSv12/' + var + '/2010/2010010600/d10-35/' + var + '_2010010600_c00.grib2'
    f1 = '/shera/datos/GEFSv12/' + var + '/2010/2010010600/d1-10/' + var + '_2010010600_c00.grib2'
    ds1 = xr.open_dataset(f1, engine='pynio')
    ds2 = xr.open_dataset(f2, engine='pynio')
    print(f1)
    print(list(ds1.coords))
    print(list(ds1.dims))
    print(list(ds1.data_vars))
    print('-----------')
    print(f2)
    print(list(ds2.coords))
    print(list(ds2.dims))
    print(list(ds2.data_vars))
    print('-----------')
'''
#f1 = '/shera/datos/GEFSv12/apcp_sfc/2000/2000010500/d1-10/apcp_sfc_2000010500_c00.grib2'
f1 = '/shera/datos/GEFSv12/dswrf_sfc/2000/2000010500/d1-10/dswrf_sfc_2000010500_c00.grib2'
ds1 = xr.open_dataset(f1, engine='pynio')
ds1 = ds1.rename_dims({'forecast_time0':'time', 'lat_0':'lat', 'lon_0':'lon', 'forecast_time1':'time1'})
ds1 = ds1.rename({'forecast_time0':'time', 'lat_0':'lat', 'lon_0':'lon', 'forecast_time1':'time1'}) 

ds1.attrs['Conventions'] = 'CF-1.10'
ds1.attrs['title'] = '00 ensemble forecast'
ds1.attrs['source'] = 'GEFSv12'

ini_time = dt.datetime.strptime(ds1.DSWRF_P11_L1_GLL0_avg6h.initial_time, '%m/%d/%Y (%H:%M)')
horas = np.arange(6,246,6)
tiempos = [ini_time + dt.timedelta(hours=int(hora)) for hora in horas]

time_units = 'hours since 2000-01-01 00:00'
time_calendar = 'proleptic_gregorian'
time_vals = date2num(tiempos, time_units)

ds1['time'] = tiempos
ds1.time['axis'] = 'T'
ds1.time['standard_name'] = 'time'
ds1.time['long_name'] = 'time'
ds1.time.encoding['units'] = time_units
ds1.time['calendar'] = time_calendar

loni = -58.38 % 360

print(ds1.sel(lat=-34.61, lon=loni, method='nearest')['DSWRF_P11_L1_GLL0_avg6h'][:])
print(ds1.sel(lat=-34.61, lon=loni, method='nearest')['time'])


ds1.to_netcdf('./test_1.nc', format='NETCDF4')



#ppglob = ds1.APCP_P11_L1_GLL0_acc6h

'''
f2 = '/shera/datos/SISSA/GEFSv12/apcp_sfc/2000/2000010500/d1-10/apcp_sfc_2000010500_c00.nc'
ds2 = xr.open_dataset(f2)
pp2 = ds2.APCP_P11_L1_GLL0_acc6h

lon_name = 'lon_0'

ds1.coords['lon_0'] = (ds1.coords['lon_0'] + 180) % 360 - 180
ds1 = ds1.sortby(ds1.lon_0)

#print(ds['lat_0'].values)
#print(ds['lon_0'].values)

min_lat = -9.5
max_lat = -56.
min_lon = -81.
max_lon = -34.

cropped_ds = ds1.sel(lat_0=slice(min_lat,max_lat), lon_0=slice(min_lon,max_lon))
pp1 = cropped_ds.APCP_P11_L1_GLL0_acc6h

ppglob2d = ppglob.isel(forecast_time0=10)
pp12d = pp1.isel(forecast_time0=10)
pp22d = pp2.isel(forecast_time0=10)


p = ppglob2d.plot.pcolormesh(subplot_kws=dict(projection=ccrs.Robinson(), facecolor="gray"),
            transform=ccrs.PlateCarree(),cmap='gist_rainbow', vmin=0.1, vmax=50)
p.axes.set_global()
p.axes.coastlines()
ax = plt.gca()
ax.plot([min_lon, min_lon],[min_lat, max_lat], '--k', transform=ccrs.Geodetic())
ax.plot([min_lon, max_lon],[min_lat, min_lat], '--k', transform=ccrs.Geodetic())
ax.plot([max_lon, max_lon],[min_lat, max_lat], '--k', transform=ccrs.Geodetic())
ax.plot([min_lon, max_lon],[max_lat, max_lat], '--k', transform=ccrs.Geodetic())

plt.savefig('global.jpg')
'''
end = time.time()
print( np.round(end - start,3), ' seconds')
