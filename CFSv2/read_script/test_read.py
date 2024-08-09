import xarray as xr
import pandas as pd
import glob

from aux_functions import get_cut_grib, save_netcdf

carpeta = '/shera/datos/CFSv2/2000/01/19991212/'
archivo = 'flxf2000053118.01.1999121218.grb2'

archivos = glob.glob(carpeta + '*.1999121200.grb2')
variables = pd.read_csv('variables_aceptadas.txt',index_col=['var'])

tiempos, lat, latb, lon, lonb, datos, unidades = get_cut_grib(carpeta+archivo)
print(tiempos)

print(unidades)
for key in datos.keys():
    ncfile = './' + key + '.nc'
    print('Procesando:', key)
    new_datos = {'nvar': key, 'units': unidades[key], 
                 'standard_name': variables.loc[key,'standard_name'],
                 'long_name': variables.loc[key,'long_name'],
                 'valores': datos[key]}
    print(datos[key].shape)
    save_netcdf(ncfile, tiempos, lat, latb, lon, lonb, new_datos)
    
#carpeta = './'
#archivo = 'test_wgrib.grb'

#ds = xr.open_dataset(carpeta+archivo, engine='pynio')
#print(ds[lvar])
#print(ds['lat_0'])

#ds.close()
