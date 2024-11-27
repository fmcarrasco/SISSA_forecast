#
import os
import datetime as dt
import time
import glob
import calendar
import pandas as pd
import numpy as np
import xarray as xr

start = time.time()

#
folder = '/datos2/SISSA/ERA5/'
variable = 'rain'
week2 = True
folder_o = folder + 'clim/' + variable + '/'
os.makedirs(folder_o, exist_ok=True)

fechas_gen = pd.date_range('1960-01-01', '1960-12-31')
dayofyear = np.linspace(1, 366, num=366, dtype=np.int64)

dict_fecha = {}
for fecha in fechas_gen:
    list_d = []
    for year in np.arange(1999,2016):
        if ((fecha.day == 29) &  (fecha.month == 2) & (not calendar.isleap(year)) ):
            continue
        f0 = dt.datetime(year, fecha.month, fecha.day)
        f1 = f0 - dt.timedelta(days=3)
        f2 = f0 + dt.timedelta(days=3)
        list_d.extend(list(pd.date_range(f1,f2, freq='D')))
        
    dict_fecha[fecha.strftime('%d%m')] = list_d

#
archivos = sorted(glob.glob(folder + variable + '/*.nc'))
print(archivos)
#
ds = xr.open_mfdataset(archivos)
ds = ds.chunk({"time": -1, "latitude": "auto", "longitude": "auto"})
#################################################
# Se calcula media movil semanal
dsb = ds.sortby("time", ascending=False)
if week2:
    if variable == 'rain':
        print('Calculando acumulado de 2 semanas')
        ds_mweek = dsb.rolling(time=14).sum()
    elif variable == 'tmax':
        print('Calculando maxima de 2 semanas')
        ds_mweek = dsb.rolling(time=14).max()
    elif variable == 'tmin':
        print('Calculando minima de 2 semanas')
        ds_mweek = dsb.rolling(time=14).min()
    else:
        print('Calculando media de 2 semanas')
        ds_mweek = dsb.rolling(time=14).mean()
else:
    if variable == 'rain':
        print('Calculando acumulado de 1 semana')
        ds_mweek = dsb.rolling(time=7).sum()
    elif variable == 'tmax':
        print('Calculando maxima de 1 semana')
        ds_mweek = dsb.rolling(time=7).max()
    elif variable == 'tmin':
        print('Calculando minima de 1 semana')
        ds_mweek = dsb.rolling(time=7).min()
    else:
        print('Calculando media de 1 semanas')
        ds_mweek = dsb.rolling(time=7).mean()
####################################################

ds_mweek = ds_mweek.sortby('time')
# Calculamos ahora los percentiles para las semanas iniciadas
# en cada dia del agno
# se guardan como netcdf
for quantile in [20, 50, 80]:
    print('Calculando percentil:', quantile)
    if week2:
        filename0 = folder_o + variable + '_2weeklymean_pctile' + str(quantile) + '_v2.nc'
        filename1 = folder_o + variable + '_2weeklymean_pctile' + str(quantile) + '_smooth.nc'
    else:
        filename0 = folder_o + variable + '_weeklymean_pctile' + str(quantile) + '_v2.nc'
        filename1 = folder_o + variable + '_weeklymean_pctile' + str(quantile) + '_smooth.nc'
    qval = quantile/100.
    print(qval)
    print('& Trabajando en archivo:', filename0, '&')
    print('& Trabajando en archivo:', filename1, '&')
    list_xarray = []
    for fecha in fechas_gen:
        dm = fecha.strftime('%d%m')
        ds_mweek0 = ds_mweek.sel(time=dict_fecha[dm])
        ds_mweek0 = ds_mweek0.chunk({"time": -1, "latitude": "auto", "longitude": "auto"})
        pctile = ds_mweek0.quantile(q=qval, dim='time')
        list_xarray.append(pctile[variable])
    pctile_final = xr.concat(list_xarray, pd.Index(dayofyear, name="dayofyear"))
    # Comenzamos el suavizado segun codigos subX de kpegion
    pctile_final_smooth = pctile_final.copy()
    for i in range(2):
        # Extend the DataArray to allow rolling to do periodic
        pctile_final_smooth = xr.concat([pctile_final_smooth[-15:], pctile_final_smooth, pctile_final_smooth[:15]], 'dayofyear')
        # Rolling mean
        pctile_final_smooth = pctile_final_smooth.rolling(dayofyear=31, center=True, min_periods=1).mean()
        # Drop the periodic boundaries
        pctile_final_smooth = pctile_final_smooth.isel(dayofyear=slice(15, -15))
    # Extract the original days
    pctile_final_smooth = pctile_final_smooth.sel(dayofyear=pctile_final.dayofyear)
    # Cambiamos el dayofyear por S y fechas con el year 1960 (bisiesto)
    # Media diaria primero
    # Change coordinate to S
    pctile_final = pctile_final.assign_coords(S=('dayofyear', fechas_gen))
    pctile_final = pctile_final.swap_dims({'dayofyear': 'S'})
    pctile_final = pctile_final.reset_coords('dayofyear', drop=True)
    # Guardamos netcdf
    pctile_final.to_netcdf(filename0)
    pctile_final = None
    # Change coordinate to S for SMOOTH
    pctile_final_smooth = pctile_final_smooth.assign_coords(S=('dayofyear', fechas_gen))
    pctile_final_smooth = pctile_final_smooth.swap_dims({'dayofyear': 'S'})
    pctile_final_smooth = pctile_final_smooth.reset_coords('dayofyear', drop=True)
    # Guardamos netcdf
    pctile_final_smooth.to_netcdf(filename1)
    pctile_final_smooth = None



end = time.time()
tiempo = (end-start)/60.  # minutos

print('Se demoro:', np.round(tiempo,2), 'minutos')


