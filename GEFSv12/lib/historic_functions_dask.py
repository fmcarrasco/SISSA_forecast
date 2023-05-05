import numpy as np
import pandas as pd
import numpy.ma as ma
import xarray as xr
import dask.array as da

from netCDF4 import Dataset, num2date


def get_era5hist_data_xarray(nomvar, era5_f, fechas):
    print('## Extrayendo datos ERA5 ##')
    meses = np.array([fecha.month for fecha in fechas])
    out_m = np.array([str(mes) for mes in meses])
    lista_xr = []
    for i, mes in enumerate(meses):
        if mes - 1 <= 0:
            cnd = [12, 1, 2]
        elif mes + 1 >= 13:
            cnd = [11, 12, 1]
        else:
            cnd = [mes - 1, mes, mes + 1]
        lista_datos = []
        archivos = [era5_f + nomvar + '/' + str(year) + '.nc' for year in np.arange(2000,2010)]
        ds = xr.open_mfdataset(archivos, parallel=True)
        ds_sel = ds.sel(time=np.isin(ds.time.dt.month, cnd))
        ds_sel = ds_sel.rename({'time':'hist_time0', 'latitude':'lat', 'longitude':'lon'})
        lista_xr.append(ds_sel.tmean)
    
    out_f = xr.concat(lista_xr, pd.DatetimeIndex(fechas, name='time'))
    
    return out_f, out_m

def get_era5_serie(d_era5,i,j):
    return d_era5[:,:,j,i]

def get_gefshist_data_xarray(nomvar, gefs_f, fechas):
    print('## Extrayendo datos GEFS ##')
    meses = np.array([fecha.month for fecha in fechas])
    #######################
    # Distribucion plazo
    plazos = np.arange(0, len(meses))
    lista_xr = []
    #out_d = da.from_array(np.empty((len(meses), 132, 187, 189)))
    #out_d[:] = np.nan
    out_m = np.array([str(mes) + '_' + str(int(plazo)).zfill(2) for mes, plazo in zip(meses, plazos)])
    for i, mes in enumerate(meses):
        plazo_str = str(int(i)).zfill(2)
        if mes - 1 <= 0:
            cnd = [12, 1, 2]
        elif mes + 1 >= 13:
            cnd = [11, 12, 1]
        else:
            cnd = [mes - 1, mes, mes + 1]
        lista_datos = []
        fmod = gefs_f + 'Distrib/' + nomvar + '/' + nomvar + '_' + plazo_str + '.nc'
        ds = xr.open_dataset(fmod).chunk(chunks={'time':10}) 
        promedio = ds.to_array(dim='new').mean('new')
        ds = ds.assign(promedio=promedio)
        ds_sel = ds.sel(time=np.isin(ds.time.dt.month, cnd))
        ds_sel = ds_sel.rename({'time':'hist_time1', 'promedio':nomvar})
        lista_xr.append(ds_sel[nomvar])
        # Calculamos media de ensamble
        #nt, ny, nx = ds_sel.promedio.data.shape
        #out_d[i,0:nt,:,:] = ds_sel.promedio.data
    out_f = xr.concat(lista_xr, pd.DatetimeIndex(fechas, name='time'))
    return out_f, out_m


def get_gefshist_serie(d_gefs,i,j):
    out_d = {}
    for key in d_gefs.keys():
        out_d[key] = d_gefs[key][:,j,i]
    return out_d