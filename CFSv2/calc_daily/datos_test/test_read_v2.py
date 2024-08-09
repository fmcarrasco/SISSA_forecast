import datetime as dt
import numpy as np
import xarray as xr
import iris
import os
import glob
import time


start = time.time()

nctestigo = '../archivo_guia/tmax_20000105_c00.nc'
t1 = xr.open_dataset(nctestigo)
lat0 = t1.lat.to_numpy()
lon0 = t1.lon.to_numpy()

year = '2012'
mes = str(1).zfill(2)
fecha_ini = dt.date(int(year),int(mes),15)
variable = 'prate'
#carpeta = '/shera/datos/SISSA/CFSv2/'
carpeta = '../../storage/'
directory = carpeta + variable + '/' + year + '/' + mes + '/'
archivos= sorted(glob.glob(carpeta + '/*.nc'))

c1 = iris.load(nctestigo)
if variable == 'apcp_sfc':
    esquema = iris.analysis.AreaWeighted(mdtol=0.5)
else:
    esquema = iris.analysis.Linear()
#Obtenemos el cubo
v025 = c1[0]

for archivo in archivos[3:4]:
    print(archivo)
    ds = xr.open_dataset(archivo)
    c1 = iris.load(archivo)
    print(c1[0])
    print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
    v050_reg = c1[0].regrid(v025, esquema)
    print(v050_reg)
    iris.save(v050_reg, "test_v2_interpolado_iris.nc")
    exit()
    horas = ds.time.dt.hour.to_numpy()
    h1 = np.where(horas == 12)[0][0]
    faux = str(ds.time.dt.strftime('%Y-%m-%d')[h1].values)
    fecha2 = ds.time.dt.strftime('%Y-%m-%d')[-1].values
    fecha1 = dt.datetime.strptime(faux, '%Y-%m-%d')
    f1 = dt.datetime(fecha1.year, fecha1.month, fecha1.day, 12,0,0)
    ds = ds[variable].sel(time=slice(f1,fecha2))
    # Calculos Diarios
    #daily = ds.resample({'time':'24H', 'origin':'start'}).sum()
    daily0 = ds.resample({'time':'24H'}, origin='start')
    print(daily0)
    fecha_fin = daily.time.dt.date[-1]
    daily = daily.sel(time=slice(fecha_ini, fecha_fin))
    daily.to_netcdf('./test_v2_original.nc', engine='netcdf4', encoding={'time':{'units':'days since 1900-01-01 00:00:00'}})
    #var_ = daily['tmax_2m']
    var_ = daily['u10']
    var_.time.encoding["units"] = 'days since 1900-01-01 00:00:00'
    # IRIS

    daily_ir = var_.to_iris()
    v050 = daily_ir
    v050_reg = v050.regrid(v025, esquema)
    iris.save(v050, "test_v2_original_iris.nc")
    iris.save(v050_reg, "test_v2_interpolado_iris.nc")
    #
    dsi = daily.interp(lat=lat0, lon=lon0)
    print('Fecha inicial:',fecha_ini)
    print('Fecha final',fecha_fin)
    print('######################## Dato original ###################')
    print(ds)
    print('######################## Dato diario ###################')
    print(daily)
    print('######################## Dato diario iris ###################')
    print(daily_ir)
    print('######################## Dato iris 0.25 ###################')
    print(v025)
    print('######################## Dato iris diario 0.5 ###################')
    print(v050)
    print('######################## Dato iris interpolado a 0.25 ###################')
    print(v050_reg)
    print('######################## Dato interpolado con xarray ###################')
    print(dsi)
    dsi.to_netcdf('./test_v2_interpolado.nc', engine='netcdf4', encoding={'time':{'units':'days since 1900-01-01 00:00:00'}})

end = time.time()
print( np.round(end-start,2), 'segundos')

