import os
import numpy as np
import pandas as pd
from netCDF4 import Dataset, num2date
import datetime as dt
import xarray as xr

import dask
from dask.distributed import Client, performance_report
from dask.distributed import LocalCluster
#from dask.diagnostics import ProgressBar

import sys
sys.path.append('./lib/')
from historic_functions_dask import get_gefshist_plazo_mes
from historic_functions_dask import get_era5hist_mes

import time

def run():
    #gefs_f = '/Volumes/Almacenamiento/python_proyects/DATOS/SISSA/GEFSv12/'
    gefs_f = '/shera/datos/SISSA/'
    #era5_f = '/Volumes/Almacenamiento/python_proyects/DATOS/SISSA/ERA5/'
    era5_f = '/shera/datos/SISSA/Diarios/ERA5/'
    nomvar = 'tmax'
    gefs_distrib = gefs_f + 'Distrib/GEFSv12_corr/' + nomvar + '/'
    era5_distrib = gefs_f + 'Distrib/ERA5/' + nomvar + '/'
    os.makedirs(gefs_distrib, exist_ok=True)
    #os.makedirs(era5_distrib, exist_ok=True)
    startf = time.time()
    for plazo in np.arange(0,34):
        print('Trabajando en plazo:', plazo)
        for mes in np.arange(1,13):
            str_mes = str(mes).zfill(2)
            str_plazo = str(plazo).zfill(2)
            print('Trabajando en mes:', mes)
            # Lectura de datos historicos GEFSv12
            data_gefs = get_gefshist_plazo_mes(nomvar, plazo, gefs_f, mes)
            ncfile = gefs_distrib + nomvar + '_p' + str_plazo + '_m' + str_mes + '.nc' 
            print('Guardando', ncfile)
            data_gefs.to_netcdf(ncfile)
            #h0 = data_gefs.values[:,:,:]
            '''
            if plazo == 0:
                data_era5 = get_era5hist_mes(nomvar, era5_f, mes)
                ncfile = era5_distrib + nomvar + '_m' + str_mes + '.nc' 
                print('Guardando', ncfile)
                data_era5.to_netcdf(ncfile)
            #h1 = data_era5.values[:,:,:]
            '''
############################
    endf = time.time()
    minutosf = np.round((endf - startf)/60., 3)
    horasf = np.round(minutosf/60., 3)
    print('Tiempo de demora: ', minutosf, ' minutos.')
    print('Tiempo de demora: ', horasf, ' horas.')
    #client.shutdown()
if __name__ == "__main__":
    run()
