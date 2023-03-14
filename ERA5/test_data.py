import numpy as np
import pandas as pd
import os
import time

import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.io.img_tiles as cimgt
import cartopy.feature as cfeature
from cartopy.io.shapereader import Reader
from cartopy.io.shapereader import natural_earth
from mpl_toolkits.axes_grid1.inset_locator import InsetPosition
from shapely.geometry.polygon import LinearRing

import sys
sys.path.append('./lib/')
from erah_class import erah_class

start = time.time()

year = 2003

o1 = erah_class(year)
print(o1.var)
print((o1.dtime))
#print(o1.fun_daily.keys())
d1, m1, u1, fv = o1.get_variables('t2m')
print(d1.shape)
datos = {}; mask = {}
datos['t2m'] = d1
mask['t2m'] = m1
dato_diario = o1.calc_daily('tmean', datos, mask)
print(dato_diario['tmean'].shape)
print(time.time()-start)
exit()

stamen_terrain = cimgt.Stamen('terrain')
# get country borders
resolution = '10m'
category = 'cultural'
name = 'admin_0_countries'

shpfilename = Reader(natural_earth(resolution, category, name))
paises = shpfilename.records()

shp = Reader(natural_earth(resolution='10m', category='cultural',
                           name='admin_1_states_provinces_lines'))
countries = shp.records()

era5_extent = [-81, -34., -56, -9.5]
extent = [-84, -30., -58, -6]
lonmin, lonmax, latmin, latmax = era5_extent

fig = plt.figure(figsize=(10, 8))
ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
ax.set_extent(extent, crs=ccrs.PlateCarree())
ax.add_image(stamen_terrain, 7)
#ax.add_feature(cfeature.LAND)
#ax.add_feature(cfeature.OCEAN)
#ax.add_feature(cfeature.COASTLINE)
#ax.add_feature(cfeature.BORDERS, linewidth=0.7, linestyle='-', zorder=1)
for pais in paises:
    ax.add_geometries( [pais.geometry], ccrs.PlateCarree(),
                        edgecolor='black', facecolor='none',
                        linewidth=0.7, zorder=1 )

ax.plot([lonmin, lonmax, lonmax, lonmin, lonmin], [latmin, latmin, latmax, latmax, latmin],
         color='red', linewidth=1, marker='.',
         transform=ccrs.PlateCarree())

plt.savefig('border_map.jpg', dpi=200, bbox_inches='tight')
#ax.add_feature(cfeature.BORDERS, linestyle='-', zorder=1)
'''
for country in countries:
    if country.attributes['adm0_name'] == 'Argentina':
        ax.add_geometries( [country.geometry], ccrs.PlateCarree(),
                            edgecolor='black', facecolor='none',
                            linewidth=0.7, zorder=1 )

# inset location relative to main plot (ax) in normalized units
inset_x = 1
inset_y = 1
inset_size = 0.2

ax2 = plt.axes([0, 0, 1, 1], projection=ccrs.PlateCarree())
ax2.set_extent([-73, -55, -30, -20], crs=ccrs.PlateCarree())
#ax2.set_global()
ax2.add_feature(cfeature.LAND)
ax2.add_feature(cfeature.OCEAN)
ax2.add_feature(cfeature.COASTLINE)
ax2.add_feature(cfeature.BORDERS, linewidth=0.7, linestyle='-', zorder=1)
countries = shp.records()
for country in countries:
    if country.attributes['adm0_name'] == 'Argentina':
        ax2.add_geometries( [country.geometry], ccrs.PlateCarree(),
                             edgecolor='black', facecolor='none',
                             linewidth=0.5, zorder=1 )
'''
