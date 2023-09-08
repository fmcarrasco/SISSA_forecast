import sys
import pandas as pd
import numpy as np
sys.path.append('./lib/')
from gefs_class import gefs_class
import time
import os
start = time.time()

v1 = 'dswrf_sfc';
v2 = 'ROCsfc'
fechas = pd.bdate_range(start='2000-01-05', end='2019-12-25', freq='W-WED')
ensembles = ['c00', 'p01', 'p02', 'p03', 'p04', 'p05', 'p06', 'p07', 'p08', 'p09', 'p10']

carpeta = '/shera/datos/SISSA/Diarios/GEFSv12/' + v2 + '/'
os.makedirs(carpeta, exist_ok=True)

for ffecha in fechas:
    yr = ffecha.strftime('%Y/')
    print(ffecha)
    fecha = ffecha.strftime('%Y%m%d')
    carpeta_f = carpeta + yr + fecha + '/'
    os.makedirs(carpeta_f, exist_ok=True)
    for nens in ensembles:
        a0 = gefs_class(v1, fecha, nens)
        if a0.continuar:
            continue
        else:
            a0.calc_daily()
            ncfile =  carpeta_f + v2 + '_' + fecha + '_' + nens + '.nc'
            print('### Guardando archivo diario', ncfile, ' ###')
            a0.save_netcdf(ncfile)


end = time.time()
print( np.round(end - start,3)/60., ' minutes')

