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
variable = 'rain'
folder_o = folder + 'clim/' + variable + '/'
os.makedirs(folder_o, exist_ok=True)

#
fechas_gen = pd.date_range('1960-01-01', '1960-12-31')
#
archivos = sorted(glob.glob(folder + variable + '/*.nc'))
print(archivos)
#
lat_e = -37.31958
lon_e = -59.12219
#
ds = xr.open_mfdataset(archivos)
ds = ds.sel(time=slice('1999-01-01', '2015-12-31'))
ds = ds.chunk({"time": -1, "latitude": "auto", "longitude": "auto"})
# Calculo de media diaria y suavizacion
media_diaria = ds[variable].groupby('time.dayofyear').mean('time')
#
# Calculo de media suavizada
media_diaria_smooth = media_diaria.copy()
for i in range(2):
    # Extand the DataArray to allow rolling to do periodic
    media_diaria_smooth = xr.concat([media_diaria_smooth[-15:],
                                     media_diaria_smooth,
                                     media_diaria_smooth[:15]],'dayofyear')
    # Rolling mean
    media_diaria_smooth = media_diaria_smooth.rolling(dayofyear=31,
                                                      center=True,
                                                      min_periods=1).mean()
    # Drop the periodic boundaries
    media_diaria_smooth = media_diaria_smooth.isel(dayofyear=slice(15, -15))
# Extract the original days
media_diaria_smooth = media_diaria_smooth.sel(dayofyear=media_diaria.dayofyear)
########################################
### SAVE NETCDF
########################################
# Media diaria primero
# Change coordinate to S
media_diaria = media_diaria.assign_coords(S=('dayofyear', fechas_gen))
media_diaria = media_diaria.swap_dims({'dayofyear': 'S'})
media_diaria = media_diaria.reset_coords('dayofyear', drop=True)
#
media_diaria.to_netcdf(folder_o + variable + 'Clim.nc')
#####
# Media diaria suavizada segundo
media_diaria_smooth = media_diaria_smooth.assign_coords(S=('dayofyear', fechas_gen))
media_diaria_smooth = media_diaria_smooth.swap_dims({'dayofyear': 'S'})
media_diaria_smooth = media_diaria_smooth.reset_coords('dayofyear', drop=True)
#
media_diaria_smooth.to_netcdf(folder_o + variable + 'ClimSmooth.nc')

#
end = time.time()
tiempo = (end-start)/60.  # minutos

print('Se demoro:', np.round(tiempo,2), 'minutos')


