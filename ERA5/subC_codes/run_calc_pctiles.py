#
import os
import time
import glob
import pandas as pd
import numpy as np
import xarray as xr

start = time.time()

#
folder = '/datos2/SISSA/ERA5/'
variable = 'tmean'
folder_o = folder + 'clim/' + variable + '/'
os.makedirs(folder_o, exist_ok=True)

#
archivos = sorted(glob.glob(folder + variable + '/*.nc'))
print(archivos)
#
ds = xr.open_mfdataset(archivos)
ds = ds.chunk({"time": -1, "latitude": "auto", "longitude": "auto"})

# Se calcula media movil semanal
dsb = ds.sortby("time", ascending=False)
ds_mweek = dsb.rolling(time=14).mean()
ds_mweek = ds_mweek.sortby('time')
ds_mweek = ds_mweek.sel(time=slice('1999-01-01', '2015-12-31'))
# Calculamos ahora los percentiles para las semanas iniciadas
# en cada dia del agno
# se guardan como netcdf
for quantile in [20, 50, 80]:
    print('Calculando percentil:', quantile)
    filename = folder_o + variable + '_2weeklymean_pctile' + str(quantile) + '.nc'
    qval = quantile/100.
    print(qval)
    ds_mweek = ds_mweek.chunk({"time": -1, "latitude": "auto", "longitude": "auto"})
    b = ds_mweek.resample(time='6D')#.rolling('6D', center=True)
    print(b)
    exit()
    pctile = ds_mweek.groupby('time.dayofyear').quantile(q=qval, dim='time')
    pctile.to_netcdf(filename)


end = time.time()
tiempo = (end-start)/60.  # minutos

print('Se demoro:', np.round(tiempo,2), 'minutos')


