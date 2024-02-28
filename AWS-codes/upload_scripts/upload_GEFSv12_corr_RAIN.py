import numpy as np
import boto3
import time
import os
import glob
import pandas as pd

s3 = boto3.resource('s3')

b_name = 'sissa-forecast-database'

carpeta_local = '/shera/datos/SISSA/Diarios/GEFSv12_corr/'
variables = ['rain']
fechas = pd.date_range('2017-01-04', '2017-12-27', freq='W-WED')

start = time.time()
for variable in variables:
    print(variable)
    for fecha in fechas:
        print(fecha)
        year = fecha.strftime('%Y')
        yrmoda = fecha.strftime('%Y%m%d')
        archivos = glob.glob(carpeta_local+variable+'/'+year+'/'+yrmoda+'/*.nc')
        for local_file in archivos:
            narchivo = os.path.basename(local_file)
            b_file = 'subseasonal/GEFSv12_corr/' + variable + '/' + year + '/' + yrmoda +'/' + narchivo
            print('Subiendo archivo:',local_file)
            print('AWS Cloud:', b_file)
            s3.Bucket(b_name).upload_file(local_file, b_file)
end = time.time()
secs = np.round(end - start, 2)
mins = np.round(secs/60., 2)
hors = np.round(mins/60., 2)

print('Se demoro (s):', secs)
print('Se demoro (m):', mins)
print('Se demoro (H):', hors)

