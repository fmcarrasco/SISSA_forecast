import numpy as np
import dask
import os
import pandas as pd
from netCDF4 import Dataset, num2date
import datetime as dt
import xarray as xr

import sys
sys.path.append('./lib/')
from historic_functions_dask import get_era5hist_data_xarray
from historic_functions_dask import get_gefshist_data_xarray
from correccion_functions import qq_correcion

import time

from dask.distributed import Client
from dask.distributed import performance_report

import ctypes

def trim_memory() -> int:
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)


if __name__ == "__main__":
    client = Client(memory_limit='3.5GB')

    #CORES = mp.cpu_count()

    fechas = pd.date_range('2000-01-05', '2019-12-25', freq='W-WED')

    ensambles = ['c00', 'p01', 'p02', 'p03', 'p04', 'p05', 'p06', 'p07', 'p08', 'p09', 'p10']

    #gefs_f = '/Volumes/Almacenamiento/python_proyects/DATOS/SISSA/GEFSv12/'
    gefs_f = '/shera/datos/SISSA/'
    #era5_f = '/Volumes/Almacenamiento/python_proyects/DATOS/SISSA/ERA5/'
    era5_f = '/shera/datos/SISSA/Diarios/ERA5/'
    nomvar = 'tmean'
    #(fecha.month==11) & (fecha.year==2011):
    for fecha in [dt.datetime(2011,2,16)]:
        print(fecha)
        yr = fecha.strftime('%Y')
        fp = fecha.strftime('%Y%m%d')
        fechas = [(fecha+dt.timedelta(days=i)) for i in range(0,34)]
        tiempos = pd.date_range(fecha, periods=34)
        tiempo_referencia = pd.Timestamp(fecha)
        # Lectura de datos historicos ERA5
        data_era5, i_era5= get_era5hist_data_xarray(nomvar, era5_f, fechas)
        # Lectura de datos historicos GEFSv12
        data_gefs, i_gefs= get_gefshist_data_xarray(nomvar, gefs_f, fechas)
        print('Extrayendo datos del pronostico')
        #archivos = [gefs_f + 'Diarios/'+ nomvar + '/' + fp +'/' + nomvar + '_' + fp + '_' + ens + '.nc' for ens in ensambles]
        archivos = [gefs_f + 'Diarios/GEFSv12/'+ nomvar + '/' + yr + '/' + fp +'/' + nomvar + '_' + fp + '_' + ens + '.nc' for ens in ensambles]
        list_trabajos = []
        start = time.time()
        for archivo in archivos:
            ds = xr.open_dataset(archivo, chunks={'time':-1,'lat':30, 'lon':30})
            out = qq_correcion(ds.tmean, data_era5, data_gefs)
            print('############### Archivo Correccion ###################')
            nfile = os.path.basename(archivo)
            out.to_netcdf('./'+nfile, compute=True)
            #list_trabajos.append(out.to_netcdf('./'+nfile, compute=False))
            #client.run(trim_memory)

        #####################################
        #datasets = dask.compute(list_trabajos)
    ############################
    end = time.time()
    minutos = np.round((end-start)/60., 3)
    horas = np.round(minutos/60., 3)
    print('Tiempo de demora: ', minutos, ' minutos.')
    print('Tiempo de demora: ', horas, ' horas.')
    client.shutdown()
