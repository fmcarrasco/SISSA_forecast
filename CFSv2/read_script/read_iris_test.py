import numpy as np
import pandas as pd
import iris
import time
import datetime as dt
from netCDF4 import Dataset, num2date

import iris.quickplot as qplt
import cartopy.crs as ccrs
import matplotlib.pyplot as plt

from cftime import date2num

start = time.time()

#f1 = 'tmp_2m_2000010500_c00.nc'
f1 = 'tmin_20100106_c00.nc'
f2 = 'flxf2000020500.nc'
f3 = 'tmin_2m.nc'

c1 = iris.load(f1)
c2  = iris.load(f3)
c3 = Dataset(f3, 'r')
print(c3.variables)
var_n = 'tmin_2m'
original = np.squeeze(c3.variables[var_n][10,:,:])
lon_o = np.squeeze(c3.variables['lon'][:])
lat_o = np.squeeze(c3.variables['lat'][:])
print(lat_o)
scheme = iris.analysis.AreaWeighted(mdtol=0.5)
#scheme = iris.analysis.Linear()
#Obtenemos los cubos
pp025 = c1[0]
pp05 = c2[0]
# Seteamos los bordes de lat/lon para regrid pesado por area
# No es necesario este paso si el metodo es de tipo lineal.
#pp025.coord('longitude').guess_bounds()
#pp025.coord('latitude').guess_bounds()

#pp05.coord('longitude').guess_bounds()
#pp05.coord('latitude').guess_bounds()
# Hacemos el regrid.
pp05_reg = pp05.regrid(pp025, scheme)
print(pp05_reg)
# Preparamos los datos para graficar
lat0 = pp05_reg.coord('latitude').points
lon0 = pp05_reg.coord('longitude').points
lat1 = pp05.coord('latitude').points
lon1 = pp05.coord('longitude').points

print(lat0)
print(lat1)
#Obtenemos las fechas
calendario = 'proleptic_gregorian'
unidad = pp05.coord('time').units.cftime_unit
valores = pp05.coord('time').points
fechas = num2date(valores, unidad, calendar=calendario,
                  only_use_cftime_datetimes=False,
                  only_use_python_datetimes=True)
print(fechas)
for i, fecha in enumerate(fechas[10:11]):
    time_constr = iris.Constraint(time=lambda t: t.point == fecha)
    vreg = pp05_reg.extract(time_constr).data
    vori = pp05.extract(time_constr).data
    v0 = np.empty(vreg.shape)
    v0 = vreg.data
    v1 = np.empty(vori.shape)
    v1 = vori.data
    print(type(v0))
    print(v0.shape)
    print(type(v1))
    print(v1.shape)
    #print(np.sum(vreg*7.29e8))
    #print(np.round(np.sum(vreg*7.29e8)/np.sum(vori*2.916e9), 4))
    fig, ax = plt.subplots(nrows=1, ncols=2, subplot_kw={'projection': ccrs.PlateCarree()})

    ax[0].pcolormesh(lon0, lat0, v0, transform=ccrs.PlateCarree(), cmap='gist_rainbow')#,vmin=0, vmax=50)
    ax[0].coastlines()
    ax[0].set_title('Reggrid AreaWeighted')
    
    im = ax[1].pcolormesh(lon_o, lat_o, v1, transform=ccrs.PlateCarree(), cmap='gist_rainbow')#,vmin=0, vmax=50)
    ax[1].coastlines()
    ax[1].set_title('Original')
    
    fig.subplots_adjust(right=0.8)
    cbar_ax = fig.add_axes([0.85, 0.15, 0.05, 0.7])
    fig.colorbar(im, cax=cbar_ax)
    #plt.show()
    plt.savefig(str(i) + '.jpg', dpi=150)
    plt.close(fig)


'''

fig, ax = plt.subplots()
p = ppglob2d.plot.pcolormesh(subplot_kws=dict(projection=ccrs.Robinson(), facecolor="gray"),
            transform=ccrs.PlateCarree(),cmap='gist_rainbow', vmin=0.1, vmax=50)
p.axes.set_global()
p.axes.coastlines()
ax = plt.gca()
ax.plot([min_lon, min_lon],[min_lat, max_lat], '--k', transform=ccrs.Geodetic())
ax.plot([min_lon, max_lon],[min_lat, min_lat], '--k', transform=ccrs.Geodetic())
ax.plot([max_lon, max_lon],[min_lat, max_lat], '--k', transform=ccrs.Geodetic())
ax.plot([min_lon, max_lon],[max_lat, max_lat], '--k', transform=ccrs.Geodetic())

plt.savefig('global.jpg')
'''
end = time.time()
print( np.round(end - start,3), ' seconds')
