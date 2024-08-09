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

year = '2004'
mes = str(1).zfill(2)
fecha_ini = dt.date(int(year),int(mes), 15)
horas = ['00', '06', '12', '18']
variable = 'tmax_2m'
carpeta = '/shera/datos/SISSA/CFSv2/'
directory = carpeta + variable + '/' + year + '/' + mes + '/'
fechas = sorted([ f.path for f in os.scandir(directory) if f.is_dir() ])

c1 = iris.load(nctestigo)
if variable == 'apcp_sfc':
    esquema = iris.analysis.AreaWeighted(mdtol=0.5)
else:
    esquema = iris.analysis.Linear()
#Obtenemos los cubos
v025 = c1[0]


for fecha in fechas[0:1]:
    for hora in horas[0:1]:
        c1 = fecha + '/' + hora + '/'
        archivos = sorted(glob.glob(c1 + '*.nc'))
        ds = xr.open_mfdataset(archivos)
        daily = ds.resample(time='1D').max()
        fecha_fin = daily.time.dt.date[-1]
        daily = daily.sel(time=slice(fecha_ini, fecha_fin))
        daily.to_netcdf('./test_v1_original.nc', engine='netcdf4', encoding={'time':{'units':'days since 1900-01-01 00:00:00'}})
        var_ = daily['tmax_2m']
        var_.time.encoding["units"] = 'days since 1900-01-01 00:00:00'
        daily_ir = var_.to_iris()
        v050 = daily_ir
        v050_reg = v050.regrid(v025, esquema)
        iris.save(v050, "test_v1_original_iris.nc")
        iris.save(v050_reg, "test_v1_interpolado_iris.nc")
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
        dsi.to_netcdf('./test_v1_interpolado.nc', engine='netcdf4', encoding={'time':{'units':'days since 1900-01-01 00:00:00'}})



end = time.time()
print( np.round(end-start,2), 'segundos')

