import numpy as np
import boto3
import time
import os

s3 = boto3.resource('s3')

b_name = 'sissa-forecast-database'

carpeta_local = '/shera/datos/SISSA/Diarios/ERA5/'
variables = sorted([x[0].split('/')[-1] for x in os.walk(carpeta_local)][1:])
years = np.arange(2000, 2020)

start = time.time()
for variable in variables:
    for year in years:
        print(year)
        narchivo = str(year) + '.nc'
        local_file = carpeta_local + variable + '/' + narchivo
        b_file = 'ERA5/' + variable +'/' + narchivo
        s3.Bucket(b_name).upload_file(local_file, b_file)
end = time.time()
secs = np.round(end - start, 2)
mins = np.round(secs/60., 2)
hors = np.round(mins/60., 2)

print('Se demoro (s):', secs)
print('Se demoro (m):', mins)
print('Se demoro (H):', hors)

