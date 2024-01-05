import os
import numpy as np
import pandas as pd
from netCDF4 import Dataset, num2date
import datetime as dt
import xarray as xr

import sys
sys.path.append('./lib/')
from correccion_functions import cycle_matrix_rain

import time


def run():
    #fechas = pd.date_range('2010-01-06', '2019-12-25', freq='W-WED')
    fechas = pd.date_range('2011-05-11', '2019-12-25', freq='W-WED')

    ensambles = ['c00', 'p01', 'p02', 'p03', 'p04', 'p05', 'p06', 'p07', 'p08', 'p09', 'p10']

    #gefs_f = '/Volumes/Almacenamiento/python_proyects/DATOS/SISSA/GEFSv12/'
    gefs_f = '/shera/datos/SISSA/'
    gefs_corr = '/shera/datos/SISSA/Diarios/GEFSv12_corr/'
    gefs_hist = '/shera/datos/SISSA/Distrib/GEFSv12/'
    #era5_f = '/Volumes/Almacenamiento/python_proyects/DATOS/SISSA/ERA5/'
    era5_hist = '/shera/datos/SISSA/Distrib/ERA5/'
    nomvar = 'rain'
    carpeta_corr = gefs_corr + nomvar + '/'
    os.makedirs(carpeta_corr, exist_ok=True)
    startf = time.time()
    for fecha in fechas:
        print(fecha)
        yr = fecha.strftime('%Y')
        if yr == '2014':
            print(u'Nos saltamos año:',yr)
            continue
        fp = fecha.strftime('%Y%m%d')
        archivos = [gefs_f + 'Diarios/GEFSv12/'+ nomvar + '/' + yr + '/' + fp +'/' + nomvar + '_' + fp + '_' + ens + '.nc' for ens in ensambles]
        print('Extrayendo datos del pronostico')
        for archivo in archivos:
            if not os.path.isfile(archivo):
                print('No existe el archivo:', archivo)
                continue
            print('Trabajando en: ', archivo)
            ds = xr.open_dataset(archivo)
            prono3d = ds[nomvar].values
            prono3d_corr = prono3d.copy()
            mask3d_corr = np.empty(prono3d.shape)
            meses = np.array(ds.time.dt.month)
            start = time.time()
            for t in np.arange(0,34):
                #print('------- Plazo: ', t, '-------')
                str_plazo = str(t).zfill(2)
                str_mes = str(meses[t]).zfill(2)
                fh_gefs = gefs_hist + nomvar + '/' + nomvar + '_p' + str_plazo + '_m' + str_mes + '.nc' 
                fm_gefs = gefs_hist + nomvar + '/' + nomvar + '_p' + str_plazo + '_m' + str_mes + '_minimo.npy' 
                fh_era5 = era5_hist + nomvar + '/' + nomvar + '_m' + str_mes + '.nc'
                #print(fm_gefs)
                e5hist = xr.open_dataset(fh_era5)
                gehist = xr.open_dataset(fh_gefs)
                minimo = np.load(fm_gefs)
                gh = gehist[nomvar].values
                er = e5hist[nomvar].values
                prono = prono3d[t,:,:]
                corr2d, mascara2d = cycle_matrix_rain(prono, gh, er, minimo)
                prono3d_corr[t,:,:] = corr2d
                mask3d_corr[t,:,:] = mascara2d
                # Close datasets 
                e5hist.close()
                gehist.close()
                #end = time.time()
                #minutos = np.round((end-start)/60., 3)
                #print('Se demoro en corregir', minutos, ' minutos')
            ds_copy = ds.copy(deep=True)
            ds_copy[nomvar].values = prono3d_corr
            print('############### Archivo Correccion ###################')
            carpeta_dato = carpeta_corr + yr + '/' + fp + '/'
            os.makedirs(carpeta_dato, exist_ok=True)
            n_archivo = carpeta_dato + os.path.basename(archivo)
            print('Guardando archivo:', n_archivo)
            ds_copy.to_netcdf(n_archivo)
            end = time.time()
            minutos = np.round((end-start)/60., 3)
            print('Se demoro en corregir', minutos, ' minutos')
            mask_file = os.path.basename(archivo).split('.')[0] + '.npy'
            np.save(carpeta_dato + mask_file, mask3d_corr)
############################
    endf = time.time()
    minutosf = np.round((endf - startf)/60., 3)
    horasf = np.round(minutosf/60., 3)
    print('Tiempo de demora: ', minutosf, ' minutos.')
    print('Tiempo de demora: ', horasf, ' horas.')

if __name__ == "__main__":
    run()