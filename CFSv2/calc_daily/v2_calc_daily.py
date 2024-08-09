import time
import iris
import datetime as dt
import xarray as xr
import numpy as np
import pandas as pd
import glob
import os
from funciones import calculo_diario, variable_diaria

import sys

# Variables dadas en el llamado
year = sys.argv[1]
mes = sys.argv[2].zfill(2)
variable = sys.argv[3]

var_d = variable_diaria(variable)

# Variables iniciales

if int(year) < 2011:
    print('Este script trabaja con datos desde 2011')
    exit()

#############################
# Informacion para interpolar a reticula 0.25 simil GEFSv12
nctestigo = '/shera/datos/SISSA/Diarios/GEFSv12/tmax/2000/20000105/tmax_20000105_c00.nc'
c1 = iris.load(nctestigo)
if variable == 'apcp_sfc':
    esquema = iris.analysis.AreaWeighted(mdtol=0.5)
else:
    esquema = iris.analysis.Linear()
#Obtenemos el cubo
v025 = c1[0]


######################################
print('Working in year ' + year)
print('at month:', mes)
c0 = '/shera/datos/SISSA/CFSv2/'
c1 = '/shera/datos/SISSA/Diarios/CFSv2/'
carpeta_i = c0 + variable + '/' + year + '/' + mes + '/'
carpeta_o = c1 + var_d + '/' + year + '/' + mes + '/'
os.makedirs(carpeta_o, exist_ok=True)

archivos = sorted(glob.glob(carpeta_i + '/*.nc'))


starttime_carpeta = time.time()
for archivo in archivos:
    n_archivo = var_d + '.' + '.'.join(os.path.basename(archivo).split('.')[1:])
    try:
        ds = xr.open_dataset(archivo)
        print('Procesando archivo:', archivo)
        # Calculos Diarios
        daily = calculo_diario(ds, variable, v025, esquema)
        outfile = carpeta_o + n_archivo
        print('Guardando archivo:', outfile)
        iris.save(daily, outfile)
    except:
        print('Error con el archivo:', archivo)
        continue
# END FOR
endtime_carpeta = time.time()
carpeta_sec = endtime_carpeta - starttime_carpeta
carpeta_min = np.round(carpeta_sec/60.,2)
carpeta_hou = np.round(carpeta_min/60.,2)
print('Minutos por carpeta', carpeta_min)
print('Horas por carpeta', carpeta_hou)

