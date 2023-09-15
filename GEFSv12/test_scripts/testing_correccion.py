import numpy as np
import pandas as pd
from netCDF4 import Dataset, num2date
import datetime as dt

from statsmodels.distributions.empirical_distribution import ECDF

from scipy.interpolate import RegularGridInterpolator


import time

start = time.time()

ensambles = ['c00', 'p01', 'p02', 'p03', 'p04', 'p05', 'p06', 'p07', 'p08', 'p09', 'p10']

lat0 =  -33.86  # Perg: -33.86 ; Cuiaba: -13.69 ; Temuco: -38.55
lon0 =  -60.61  # Perg: -60.61 ; Cuiaba: -54.52 ; Temuco: -72.84

gefs_f = '/Volumes/Almacenamiento/python_proyects/DATOS/SISSA/GEFSv12/'
era5_f = '/Volumes/Almacenamiento/python_proyects/DATOS/SISSA/ERA5/'

fp = '20110216'
localidad = 'Pergamino'
###############

fecha_p = dt.datetime.strptime(fp, '%Y%m%d')
print('Extrayendo datos del pronostico')
fdiario = gefs_f + 'Diarios/tmean/' + fp +'/tmean_' + fp + '_c00.nc'
print(fdiario)
nc_prono = Dataset(fdiario, 'r')
#
lat = nc_prono.variables['lat'][:]
lon = nc_prono.variables['lon'][:]
tiempos = nc_prono.variables['time'][:]
datos = nc_prono.variables['tmean'][:]
nc_prono.close()
#
nt = len(tiempos)
#
prono_o = np.empty((nt))
for t in range(0,nt):
    dato_p = datos[t,:,:]
    f = RegularGridInterpolator((lon,lat), dato_p.T, method='nearest')
    prono_o[t] = f((lon0, lat0))

##############################################
# Serie datos ERA5 2000-2009

print('Extrayendo datos ERA5')
lista_o = []
for year in np.arange(2000,2010):
    fobs = era5_f + 'tmean/' + str(year) + '.nc'
    nc_era = Dataset(fobs, 'r')
    #
    lat = nc_era.variables['latitude'][:]
    lon = nc_era.variables['longitude'][:]
    tiempos = nc_era.variables['time']
    fechas = num2date(tiempos[:], units=tiempos.units, calendar=tiempos.calendar,
                      only_use_cftime_datetimes=False,
                      only_use_python_datetimes=True)
    meses = np.array([a.month for a in fechas])
    indice = (meses == 1) | (meses == 2) | (meses == 3)
    datos = nc_era.variables['tmean'][indice,:,:]
    nc_era.close()
    nt0, ny, nx = datos.shape
    #
    for t in range(0,nt0):
        dato_p = datos[t,:,:]
        f = RegularGridInterpolator((lon,lat), dato_p.T, method='nearest')
        #f = interp2d(lon, lat, , kind='linear')
        lista_o.append(f((lon0, lat0)).data.item())
#era5_o = np.array(lista)
ecdf_o = ECDF(lista_o)

##############################################
#
cdf_limit = 0.9999999
prono_corr = prono_o.copy()
dict_ecdf = {}
for i, val in enumerate(prono_o[0:10]):
    plazo_str = str(i).zfill(2)
    print('Trabajando en el plazo:', plazo_str)
    #######################
    # Distribucion plazo
    fmod = gefs_f + 'Distrib/tmean/tmean_'+ plazo_str + '.nc'
    print(fmod)
    nc_mod = Dataset(fmod, 'r')
    lat = nc_mod.variables['lat'][:]
    lon = nc_mod.variables['lon'][:]
    tiempos = nc_mod.variables['time']
    fechas = num2date(tiempos[:], units=tiempos.units, calendar=tiempos.calendar,
                      only_use_cftime_datetimes=False,
                      only_use_python_datetimes=True)
    meses = np.array([a.month for a in fechas])
    indice = (meses == 1) | (meses == 2) | (meses == 3)
    d_ens = {}
    print('Extrayendo datos distrib modelo')
    for ens in ensambles:
        lista = []
        datos = nc_mod.variables[ens][indice,:,:]
        nt0, ny, nx = datos.shape
        #
        for t in range(0,nt0):
            dato_p = datos[t,:,:]
            f = RegularGridInterpolator((lon,lat), dato_p.T, method='nearest')
            lista.append(f((lon0, lat0)).data.item())
        d_ens[ens] = lista
    nc_mod.close()
    df = pd.DataFrame(d_ens).mean(axis=1).to_numpy()
    ecdf_m = ECDF(df)
    dict_ecdf[plazo_str] = ecdf_m
    ########

    #########################
    # Metodo de correccion
    print('Trabajando en la correccion')
    p1 = ecdf_m(val)
    if p1 > cdf_limit:
        p1 = cdf_limit
    corr_o = np.nanquantile(lista_o, p1, method='linear')
    prono_corr[i] = corr_o

print(prono_o)
print(prono_corr)

# Figuras
import matplotlib.pyplot as plt
import random

# DATO ERA5
x_era5 = []
fobs = era5_f + 'tmean/2011.nc'
nc_era = Dataset(fobs, 'r')
#
lat = nc_era.variables['latitude'][:]
lon = nc_era.variables['longitude'][:]
tiempos = nc_era.variables['time']
fechas = num2date(tiempos[:], units=tiempos.units, calendar=tiempos.calendar,
                    only_use_cftime_datetimes=False,
                    only_use_python_datetimes=True)
meses = np.array([a.month for a in fechas])
indice = (meses == 2)
xfechas = fechas[indice]
datos = nc_era.variables['tmean'][indice,:,:]
nc_era.close()
nt0, ny, nx = datos.shape
#
for t in range(0,nt0):
    dato_p = datos[t,:,:]
    f = RegularGridInterpolator((lon,lat), dato_p.T, method='nearest')
    #f = interp2d(lon, lat, , kind='linear')
    x_era5.append(f((lon0, lat0)).data.item())
y_era5 = np.array(x_era5)
####################
iobs = np.array(xfechas == dt.datetime(2011,2,16))
itemindex = np.where(iobs)[0][0]

number_of_colors = 10

color = ["#"+''.join([random.choice('0123456789ABCDEF') for j in range(6)])
             for i in range(number_of_colors)]

# Fig 1, cambios en pronostico
titulo = u'Pronóstico fecha inicial: ' + fecha_p.strftime('%d-%m-%Y')
xplazos = np.arange(0,len(prono_o))
fig, ax = plt.subplots()
ax.plot(xplazos[0:13], y_era5[itemindex:itemindex+13], '--<k', label='ERA 5')
ax.plot(xplazos, prono_o, '--or', label='prono original')
ax.plot(xplazos, prono_corr, '--xg', label='prono corregido')
ax.set_xlim([0,12])
ax.set_ylim([0,28])
ax.set_title(titulo)
ax.set_xlabel(u'dias de pronóstico')
ax.set_ylabel('Temperatura (ºC)')
ax.legend()
fig.savefig('./figuras/prono_' + localidad+fecha_p.strftime('_%Y%m%d')+'.jpg', dpi=150)

# Fig 2, Distribuciones
titulo = u'Distribución ERA5 / Pronóstico x plazo'
fig, ax = plt.subplots()
ax.plot(ecdf_o.x, ecdf_o.y, '-k', label='ERA5')
for i, (val, color) in enumerate(zip(prono_o[0:10], color)):
    plazo_str = str(i).zfill(2)
    ax.plot(dict_ecdf[plazo_str].x, dict_ecdf[plazo_str].y, '--',
            color=color, label='GEFSv12 plazo: '+ plazo_str)
ax.set_xlim([-5,35])
ax.set_title(titulo)
#ax.set_xlabel(u'dias de pronóstico')
ax.set_xlabel('Temperatura (ºC)')
ax.legend()
fig.savefig('./figuras/distrib_' + localidad+fecha_p.strftime('_%Y%m%d')+'.jpg', dpi=150)


############################
end = time.time()
print('Tiempo de demora: ', np.round(end-start, 2), ' segundos.')