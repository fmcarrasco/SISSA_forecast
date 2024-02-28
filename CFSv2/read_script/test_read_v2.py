import xarray as xr
import pandas as pd
import glob
import os

from aux_functions import get_cut_grib_v2, save_netcdf

carpeta = '/shera/datos/CFSv2/2011/06/2011051100/'
#archivo = 'wnd10m.01.2011051100.daily.grb2'

archivos = glob.glob(carpeta + '*.grb2')

variables = pd.read_csv('variables_aceptadas.txt',index_col=['var'])

for archivo in archivos:
    narchivo = os.path.basename(archivo)
    varname = narchivo.split('.')[0]
    yymmdd = narchivo.split('.')[2][0:8]
    hora = narchivo.split('.')[2][8:]
    print(narchivo)
    print(varname)
    print(yymmdd)
    print(hora)




#tiempos, lat, latb, lon, lonb, datos, unidades = get_cut_grib_v2(carpeta+archivo)

'''
for key in datos.keys():
    ncfile = './' + key + '.nc'
    print('Procesando:', key)
    new_datos = {'nvar': key, 'units': unidades[key], 
                 'standard_name': variables.loc[key,'standard_name'],
                 'long_name': variables.loc[key,'long_name'],
                 'valores': datos[key]}
    print(new_datos)
    #save_netcdf(ncfile, tiempos, lat, latb, lon, lonb, new_datos)
'''    
#carpeta = './'
#archivo = 'test_wgrib.grb'

#ds = xr.open_dataset(carpeta+archivo, engine='pynio')
#print(ds[lvar])
#print(ds['lat_0'])

#ds.close()
