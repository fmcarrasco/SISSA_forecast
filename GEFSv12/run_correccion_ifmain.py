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
from correccion_functions import qq_correcion, qq_correcion_v2

import time

from dask.distributed import LocalCluster, Client
from dask.distributed import performance_report

import ctypes

def trim_memory() -> int:
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)


if __name__ == "__main__":
    cluster = LocalCluster(n_workers=1, threads_per_worker=4, memory_limit='1Gb')
    client = Client(cluster)
    print(client.scheduler_info())
    #CORES = mp.cpu_count()

    fechas = pd.date_range('2000-01-05', '2019-12-25', freq='W-WED')

    ensambles = ['c00', 'p01', 'p02', 'p03', 'p04', 'p05', 'p06', 'p07', 'p08', 'p09', 'p10']

    #gefs_f = '/Volumes/Almacenamiento/python_proyects/DATOS/SISSA/GEFSv12/'
    gefs_f = '/shera/datos/SISSA/'
    #era5_f = '/Volumes/Almacenamiento/python_proyects/DATOS/SISSA/ERA5/'
    era5_f = '/shera/datos/SISSA/Diarios/ERA5/'
    nomvar = 'tmean'
    #(fecha.month==11) & (fecha.year==2011):
    #client.run(trim_memory)
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
        x1 = client.scatter([data_gefs], broadcast=True)
        x2 = client.scatter([data_era5], broadcast=True)
        #print(['./' + os.path.basename(archivo) for archivo in archivos])
        open_kwargs = dict(chunks={'time':-1,'lat':30,'lon':30})
        open_tasks = [dask.delayed(xr.open_dataset)(f,**open_kwargs) for f in archivos]
        tasks = [dask.delayed(qq_correcion)(task.tmean, x1, x2) for task in open_tasks]
        save_tasks = [dask.delayed(task.to_netcdf('./v1_' + os.path.basename(archivos[ii]))) for ii, task in enumerate(tasks)]
        datasets = dask.compute(save_tasks)
        '''
        for archivo in archivos:
            ds = xr.open_dataset(archivo, chunks={'time':-1,'lat':30, 'lon':30})
            x0 = client.scatter(ds)
            print(x0)
            print(x1)
            print(x2)
            #out = qq_correcion(ds.tmean, data_era5, data_gefs)
            out = client.submit(qq_correcion, x0, x1, x2).result()
            print(out)
            print('############### Archivo Correccion ###################')
            nfile = os.path.basename(archivo)
            #out.to_netcdf('./'+nfile, compute=True)
            #list_trabajos.append(out.to_netcdf('./'+nfile, compute=False))
            #client.run(trim_memory)
        '''
        #####################################
        #datasets = dask.compute(list_trabajos)
        ############################
        
    end = time.time()
    minutos = np.round((end-start)/60., 3)
    horas = np.round(minutos/60., 3)
    print('Tiempo de demora: ', minutos, ' minutos.')
    print('Tiempo de demora: ', horas, ' horas.')
    client.shutdown()
