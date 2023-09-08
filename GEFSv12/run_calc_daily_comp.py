import sys
import pandas as pd
import numpy as np
sys.path.append('./lib/')
from gefs_class import gefs_class
from gefs_class import calc_ROLnet
from gefs_class import calc_u10

import time
import os
start = time.time()

v0 = 'tmp_2m' # None
v1 = 'rel_hum' # 'LWnet' # 'wspd'
v2 = 'pres_sfc' # 'ulwrf_sfc' #'vgrd_hgt'
v3 = 'rh' # 'ROLnet' # 'u10'
fechas = pd.bdate_range(start='2000-01-05', end='2019-12-25', freq='W-WED')
#fechas = pd.bdate_range(start='2004-12-08', end='2004-12-08', freq='W-WED')
ensembles = ['c00', 'p01', 'p02', 'p03', 'p04', 'p05', 'p06', 'p07', 'p08', 'p09', 'p10']
print(fechas)
carpeta = '/shera/datos/SISSA/Diarios/GEFSv12/' + v3 + '/'
os.makedirs(carpeta, exist_ok=True)

for ffecha in fechas:
    yr = ffecha.strftime('%Y/')
    print(ffecha)
    fecha = ffecha.strftime('%Y%m%d')
    carpeta_f = carpeta + yr + fecha + '/'
    os.makedirs(carpeta_f, exist_ok=True)
    for nens in ensembles:
        a0 = gefs_class(v1, fecha, nens)
        a1 = gefs_class(v2, fecha, nens)
        if v0 is not None:
            a3 = gefs_class(v0, fecha, nens)
            if a3.continuar:
                print('Estoy aca')
                print(a3.continuar)
                continue
        if a0.continuar | a1.continuar:
            print('Aca estoy')
            print(a0.continuar)
            print(a1.continuar)
            continue

        else:
            if v0 is not None:
                d_dato = {v0: a3.data, v2: a1.data}
            else:
                d_dato = {v2: a1.data}
            a0.calc_daily_compound(d_dato, 'calc_rh')
            #print(a0.daily_data[:,10,10])
            ncfile =  carpeta_f + v3 + '_' + fecha + '_' + nens + '.nc'
            print('### Guardando archivo diario', ncfile, ' ###')
            a0.save_netcdf(ncfile)
            ''' 
            for i in range(10,11):
                igroup = (i == a0.grupos)
                dl = a0.data[igroup,10,10]
                ul = a1.data[igroup,10,10]
                ROLnet = calc_ROLnet(dl - ul)
                #u10 = calc_u10(dl, ul)
                #net = a0.data[igroup,10,10] - a1.data[igroup,10,10]
                print(ROLnet)
                #print(u10)
                #print(np.array(a0.fechas_d)[i])
                #exit()
                #a0.calc_daily()
                #ncfile =  carpeta_f + v2 + '_' + fecha + '_' + nens + '.nc'
                #print('### Guardando archivo diario', ncfile, ' ###')
                #a0.save_netcdf(ncfile)
            '''

end = time.time()
print( np.round(end - start,3)/60., ' minutes')

