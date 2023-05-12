import numpy as np
import pandas as pd
from netCDF4 import Dataset, num2date
import datetime as dt
import xarray as xr

import sys
sys.path.append('./lib/')
from historic_functions_dask import get_era5hist_data_xarray
from historic_functions_dask import get_gefshist_data_xarray

from correccion_functions import qq_correcion


#import multiprocessing as mp
#from pathos.multiprocessing import ProcessingPool as Pool

import time


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
    if fecha == dt.datetime(2011,2,16):
        print(fecha)
        yr = fecha.strftime('%Y')
        fp = fecha.strftime('%Y%m%d')
        fechas = [(fecha+dt.timedelta(days=i)) for i in range(0,34)]
        tiempos = pd.date_range(fecha, periods=34)
        tiempo_referencia = pd.Timestamp(fecha)
        data_era5, i_era5= get_era5hist_data_xarray(nomvar, era5_f, fechas)
        #print(data_era5)
        #print(type(data_era5))
        #
        data_gefs, i_gefs= get_gefshist_data_xarray(nomvar, gefs_f, fechas)
        #print(data_gefs)
        #print(type(data_gefs))
        #
        print('Extrayendo datos del pronostico')
        #archivos = [gefs_f + 'Diarios/'+ nomvar + '/' + fp +'/' + nomvar + '_' + fp + '_' + ens + '.nc' for ens in ensambles]
        archivos = [gefs_f + 'Diarios/GEFSv12/'+ nomvar + '/' + yr + '/' + fp +'/' + nomvar + '_' + fp + '_' + ens + '.nc' for ens in ensambles]

        for archivo in archivos[0:1]:
            ds = xr.open_dataset(archivo, chunks={'time':-1,'lat':20, 'lon':20})
            #print(ds.tmean)
            #print(type(ds))
            print(ds.tmean[:,10,10].values)
            start = time.time()
            out = qq_correcion(ds.tmean, data_era5, data_gefs)
            print('############### Archivo Correccion ###################')
            #print(out[10,10,:].data.compute())
            out.to_netcdf('./test_correccion.nc', compute=True)
            end = time.time()
            minutos = np.round((end-start)/60., 3)
            print('Se demoro en corregir', minutos, ' minutos')
            #print(out.tmean[10,10,:].values)
            exit()
            
            
            for i in np.arange(0,2):
                print(i)
                for j in np.arange(0,2):
                    print(j)
                    prono = datos[:,j,i]
                    print(type(prono))
                    exit()
                    era5_hist = data_era5[:,:,j,i]
                    gefs_hist = data_gefs[:,:,j,i]

                    datos_corr[:,j,i] = series_qq_corr(prono, era5_hist, gefs_hist)
                    print(datos[:,j,i])
                    print(datos_corr[:,j,i])
        #####################################
        end = time.time()
        minutos = np.round((end-start)/60., 3)
        print('Tiempo de demora: ', minutos, ' minutos.')
        # Recorremos cada uno de los valores de la matriz datos en lat/lon
        

                

############################
end = time.time()
minutos = np.round((end-start)/60., 3)
horas = np.round(minutos/60., 3)
print('Tiempo de demora: ', minutos, ' minutos.')
print('Tiempo de demora: ', horas, ' horas.')
