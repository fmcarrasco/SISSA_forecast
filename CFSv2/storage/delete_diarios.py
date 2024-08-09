import shutil
import os
import numpy as np

base = '/shera/datos/SISSA/CFSv2/'
variables = sorted(os.listdir(base)) 
print(variables)
years = np.arange(2000,2011)

for variable in ['ugrd_hgt']:
    for year in years:
        carpeta = base + variable + '/' + str(year) + '/'
        if os.path.exists(carpeta):
            print('Borrando carpeta:', carpeta)
            shutil.rmtree(carpeta)
        print(carpeta, u'¿exist?', os.path.exists(carpeta))
print('Termino el proceso')
