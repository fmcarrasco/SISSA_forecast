import os
import numpy as np
import pandas as pd
from netCDF4 import Dataset, num2date
import datetime as dt
import xarray as xr

import dask
from dask.distributed import Client, performance_report
from dask.distributed import LocalCluster
from dask.diagnostics import ProgressBar

import sys
sys.path.append('./lib/')
from historic_functions_dask import get_era5hist_data_xarray
from historic_functions_dask import get_gefshist_data_xarray

from correccion_functions import qq_correcion


#import multiprocessing as mp
#from pathos.multiprocessing import ProcessingPool as Pool

import time

#from memory_profiler import profile

#@profile(precision=4)
def run():
    cluster = LocalCluster(n_workers=1, threads_per_worker=5)
    client = Client(cluster)
    print(cluster)
    print(client)
    #CORES = mp.cpu_count()

    fechas = pd.date_range('2010-01-06', '2019-12-25', freq='W-WED')

    ensambles = ['c00', 'p01', 'p02', 'p03', 'p04', 'p05', 'p06', 'p07', 'p08', 'p09', 'p10']

    #gefs_f = '/Volumes/Almacenamiento/python_proyects/DATOS/SISSA/GEFSv12/'
    gefs_f = '/shera/datos/SISSA/'
    gefs_corr = '/shera/datos/SISSA/Diarios/GEFSv12_corr/'
    #era5_f = '/Volumes/Almacenamiento/python_proyects/DATOS/SISSA/ERA5/'
    era5_f = '/shera/datos/SISSA/Diarios/ERA5/'
    nomvar = 'tmean'
    carpeta_corr = gefs_corr + nomvar + '/'
    os.makedirs(carpeta_corr, exist_ok=True)
    #(fecha.month==11) & (fecha.year==2011):
    startf = time.time()
    for fecha in fechas[18:19]:
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
        for archivo in archivos[6:7]:
            print('Trabajando en: ', archivo)
            #with performance_report(filename="dask-report-1archivo.html"):
            ds = xr.open_dataset(archivo, chunks={'time':-1,'lat':50, 'lon':50})
            ds_copy = ds.copy(deep=True)
            #print(ds.tmean)
            #print(type(ds))
            #print(ds.tmean[:,:,:].values)
            start = time.time()
            out = qq_correcion(ds[nomvar], data_era5, data_gefs)
            out = out.transpose("time","lat","lon")
            ds_copy[nomvar] = out
            print('############### Archivo Correccion ###################')
            start_test = time.time()
            ######
            salida = dask.compute(ds_copy)
            ######
            end_test = time.time()
            minutos_test = np.round((end_test - start_test)/60., 4)
            print(minutos_test)
            exit()
            #print(out[10,10,:].data.compute())
            #with performance_report(filename='dask_report_v2.html'):
            carpeta_dato = carpeta_corr + yr + '/' + fp + '/'
            os.makedirs(carpeta_dato, exist_ok=True)
            #n_archivo = carpeta_dato + 'test_' + os.path.basename(archivo)
            n_archivo = carpeta_dato + 'test_' + os.path.basename(archivo).split('.')[0] + '.zarr'
            #write_job = out.to_netcdf(n_archivo, compute=False)
            write_job = ds_copy.to_zarr(n_archivo, compute=False)
            with ProgressBar():
                print(f"Escribiendo a {n_archivo}")
                write_job.compute()
            end = time.time()
            minutos = np.round((end-start)/60., 3)
            print('Se demoro en corregir', minutos, ' minutos')
            #print(out.tmean[10,10,:].values)
            #exit()
        #####################################
        #end = time.time()
        #minutos = np.round((end-start)/60., 3)
        #print('Tiempo de demora: ', minutos, ' minutos.')
############################
    endf = time.time()
    minutosf = np.round((endf - startf)/60., 3)
    horasf = np.round(minutosf/60., 3)
    print('Tiempo de demora: ', minutosf, ' minutos.')
    print('Tiempo de demora: ', horasf, ' horas.')
    client.shutdown()
if __name__ == "__main__":
    run()
