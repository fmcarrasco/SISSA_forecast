import os
import numpy as np
import pandas as pd
from netCDF4 import Dataset, num2date
import datetime as dt
import xarray as xr
import matplotlib.pyplot as plt
from numba import jit

import sys
sys.path.append('./lib/')

import time

@jit(nopython=True)
def calc_freq_pp(datos, ppmin):
    '''
    Calculo de frecuencia de dias sin precipitacion
    '''
    ind = np.array([e > ppmin if ~np.isnan(e) else False for e in datos], dtype=np.bool_)
    precdias = datos[ind]
    frec = 1. - 1.*precdias.shape[0]/datos.shape[0]

    return frec

@jit(nopython=True)
def calc_ppmin(do, dm):
    '''
    Limite minimo de PP para ERA5 consideramos 0.2 segun
    Rodwell et al 2010; Guia OMM
    https://rmets.onlinelibrary.wiley.com/doi/10.1002/qj.656
    '''
    frec_o = calc_freq_pp(do, 0.2)
    frec_m = calc_freq_pp(dm, 0.)
    if frec_m < frec_o:
        dm = dm[~np.isnan(dm)]
        dm_sorted = np.sort(dm[dm!=0])
        pos_min = np.int64(np.shape(dm_sorted)[0] - (1 - frec_o) * np.shape(dm)[0])
        if pos_min > 0:
            ppmin_int = dm_sorted[pos_min-1]
        else:
            ppmin_int = dm_sorted[pos_min]
        minimos_pp = np.round(ppmin_int, 2)
        frec_corr = calc_freq_pp(dm, minimos_pp)
    else:
        minimos_pp = np.nan
    return minimos_pp

@jit(nopython=True)
def ciclo_matriz(era5, gefs):
    a,b,c = era5.shape
    min_pp = np.zeros((b,c))
    min_pp[:] = np.nan
    for index in np.ndindex(b, c):
        ghist = gefs[:, index[0], index[1]]
        ehist = era5[:, index[0], index[1]]
        min_pp[index[0], index[1]] = calc_ppmin(ehist, ghist)
    return min_pp

def run():
    #gefs_f = '/Volumes/Almacenamiento/python_proyects/DATOS/SISSA/GEFSv12/'
    gefs_f = '/shera/datos/SISSA/Distrib/GEFSv12/rain/'
    #era5_f = '/Volumes/Almacenamiento/python_proyects/DATOS/SISSA/ERA5/'
    era5_f = '/shera/datos/SISSA/Distrib/ERA5/rain/'
    nomvar = 'rain'
    startf = time.time()
    for plazo in np.arange(0,34):
        print('Trabajando en plazo:', plazo)
        for mes in np.arange(1,13):
            str_mes = str(mes).zfill(2)
            str_plazo = str(plazo).zfill(2)
            print('Trabajando en mes:', mes)
            # Lectura de datos historicos ERA5
            ncfile0 = era5_f + nomvar + '_m' + str_mes + '.nc' 
            era5_d = Dataset(ncfile0, 'r')
            # Lectura de datos historicos GEFSv12
            ncfile1 = gefs_f + nomvar + '_p' + str_plazo + '_m' + str_mes + '.nc' 
            gefs_d = Dataset(ncfile1, 'r')
            # extraemos las matrices 3D y calculamos el minimo por plazo y para el mes
            dato_o = era5_d.variables[nomvar][:].data
            dato_m = gefs_d.variables[nomvar][:].data
            minimo_pp = ciclo_matriz(dato_o, dato_m)
            outfile = gefs_f + nomvar + '_p' + str_plazo + '_m' + str_mes + '_minimo.npy'
            # save the outputfile
            print('--- Guardando minimo de precipitacion: ', outfile, ' ---')
            np.save(outfile, minimo_pp)
            era5_d.close()
            gefs_d.close()
############################
    endf = time.time()
    minutosf = np.round((endf - startf)/60., 3)
    horasf = np.round(minutosf/60., 3)
    print('Tiempo de demora: ', minutosf, ' minutos.')
    print('Tiempo de demora: ', horasf, ' horas.')
    return minimo_pp

if __name__ == "__main__":
    minimo_pp = run()
