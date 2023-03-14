import sys
import pandas as pd
import numpy as np
sys.path.append('./lib/')
from gefs_class import gefs_class
import time

start = time.time()
#archivo0 = '../../DATOS/GEFSv12/apcp_sfc/2000/2000010500/d1-10/apcp_sfc_2000010500_c00.nc'
#archivo1 = '../../DATOS/GEFSv12/apcp_sfc/2000/2000010500/d10-35/apcp_sfc_2000010500_c00.nc'

v1 = 'apcp_sfc'; 
v2 = 'tmp_2m'
fecha = '20100106'
nens = 'p02'
a0 = gefs_class(v1, fecha, nens)
a1 = gefs_class(v2, fecha, nens)
#print(a1.fechas)
#print(a1.grupos)
#print(a1.fechas_d)
a0.calc_daily()
a1.calc_daily()
a0.save_netcdf('test_a0.nc')
a1.save_netcdf('test_a1.nc')
#fecha = '20000105'; nens = 'p01'

'''
fechas = pd.bdate_range(start='2000-01-05', end='2019-12-25', freq='W-WED')
ensembles = ['c00', 'p01', 'p02', 'p03', 'p04', 'p05', 'p06', 'p07', 'p08', 'p09', 'p10']

for ffecha in fechas[0:1]:
    print(ffecha)
    fecha = ffecha.strftime('%Y%m%d')
    for nens in ensembles:
        print(nens)
        a0 = gefs_class(v1, fecha, nens)
        a0.calc_daily()
        ncfile = './' + fecha + '_' + nens + '.nc'
        a0.save_netcdf(ncfile)


end = time.time()
print( np.round(end - start,3), ' seconds')


print(a0.datos)
print(a1.datos)

print(a0.units, a1.units)
print(a0.nx, a0.ny, a1.nx, a1.ny)
print(a0.lat)
print(a1.lat)
print(a0.lon)
print(a1.lon)
'''
