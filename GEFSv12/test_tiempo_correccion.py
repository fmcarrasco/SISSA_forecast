import os
import numpy as np
import pandas as pd
from netCDF4 import Dataset, num2date
import datetime as dt
import xarray as xr

from dask.distributed import Client, performance_report, LocalCluster
from dask.diagnostics import ProgressBar

import sys
sys.path.append('./lib/')
from historic_functions_dask import get_era5hist_data_xarray
from historic_functions_dask import get_gefshist_data_xarray

from correccion_functions import qq_correcion, series_qqcorr


#import multiprocessing as mp
#from pathos.multiprocessing import ProcessingPool as Pool

import time


def run():

    cluster = LocalCluster(n_workers=1, threads_per_worker=5)
    client = Client(cluster)
    #client = Client()
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
    for fecha in fechas[4:5]:
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
        for archivo in archivos[0:1]:
            print('Trabajando en: ', archivo)
            ds = xr.open_dataset(archivo, chunks={'time':-1,'lat':30, 'lon':30})
            x1 = ds.tmean[:,:,:].values
            np.save('prono_3d.npy', x1)
            del x1
            x2 = data_era5[:,:,:,:].values
            np.save('era5_hist_4d.npy', x2)
            del x2
            x3 = data_gefs[:,:,:,:].values
            np.save('gefs_hist_4d.npy', x3)
            del x3
            exit()
            start = time.time()
            print('Empezando series_qqcorr')
            with performance_report(filename="dask-report-test-qqcorr.html"):
                salida = series_qqcorr(x1,x2,x3) 
            end = time.time()
            segundos = np.round(end-start, 3)
            minutos = np.round((end-start)/60., 3)
            print('Se demoro series_qqcorr', segundos, ' segundos')
            print('Se demoro series_qqcorr', minutos, ' minutos')
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

if __name__ == "__main__":
    run()
