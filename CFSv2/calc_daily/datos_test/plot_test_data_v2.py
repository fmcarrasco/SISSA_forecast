import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

v1_o = './test_v2_original.nc'
v1_i = './test_v2_interpolado.nc'
v1_iro = './test_v2_original_iris.nc'
v1_iri = './test_v2_interpolado_iris.nc'

ds1_o = xr.open_dataset(v1_o)
ds1_i = xr.open_dataset(v1_i)
ds2_o = xr.open_dataset(v1_iro)
ds2_i = xr.open_dataset(v1_iri)

# datos para los mapas
paises = cfeature.NaturalEarthFeature(
        category='cultural',
        name='admin_0_countries',
        scale='10m',
        facecolor='none')
provincias = cfeature.NaturalEarthFeature(
        category='cultural',
        name='admin_1_states_provinces_lines',
        scale='10m',
        facecolor='none')



# figuras para datos entre 2000-2011

tiempos = ds1_i.time
indices = np.arange(0,len(tiempos), 30)

tmax_o = ds1_o.tmax_2m
tmax_i = ds1_i.tmax_2m
tmax_iro = ds2_o.tmax_2m
tmax_iri = ds2_i.tmax_2m



for indice, tiempo in zip(indices, tiempos):

    #print(tmax_iri.sel(time=tmax_o.time[indice]))
    f_strf = tmax_o.time[indice].dt.strftime('%Y%m%d').values

    fig, ax = plt.subplots(nrows=2, ncols=2, subplot_kw={'projection': ccrs.PlateCarree()})
    
    tmax_o.sel(time=tmax_o.time[indice]).plot(ax=ax[0,0], cmap='gist_rainbow_r',vmin=273, vmax=323, add_colorbar=False)
    ax[0,0].set_title('')
    ax[0,0].set_title('Netcdf 0.5 con Xarray', fontsize=5, loc='left')
    ax[0,0].set_title(f_strf, fontsize=5, loc='right')
    ax[0,0].coastlines()
    ax[0,0].add_feature(paises, edgecolor='black')
    ax[0,0].add_feature(provincias, edgecolor='gray')

    tmax_i.sel(time=tmax_i.time[indice]).plot(ax=ax[0,1], cmap='gist_rainbow_r',vmin=273, vmax=323, add_colorbar=False)
    ax[0,1].set_title('')
    ax[0,1].set_title('Netcdf interpolado 0.25 con Xarray', fontsize=5, loc='left')
    ax[0,1].set_title(f_strf, fontsize=5, loc='right')
    ax[0,1].coastlines()
    ax[0,1].add_feature(paises, edgecolor='black')
    ax[0,1].add_feature(provincias, edgecolor='gray')

    tmax_iro.sel(time=tmax_i.time[indice]).plot(ax=ax[1,0], cmap='gist_rainbow_r',vmin=273, vmax=323, add_colorbar=False)
    ax[1,0].set_title('')
    ax[1,0].set_title('Netcdf 0.5 guardado con Iris', fontsize=5, loc='left')
    ax[1,0].set_title(f_strf, fontsize=5, loc='right')
    ax[1,0].coastlines()
    ax[1,0].add_feature(paises, edgecolor='black')
    ax[1,0].add_feature(provincias, edgecolor='gray')

    im = tmax_iri.sel(time=tmax_i.time[indice]).plot(ax=ax[1,1], cmap='gist_rainbow_r',vmin=273, vmax=323, add_colorbar=False)
    ax[1,1].set_title('')
    ax[1,1].set_title('Netcdf intepolado 0.25 con Iris', fontsize=5, loc='left')
    ax[1,1].set_title(f_strf, fontsize=5, loc='right')
    ax[1,1].coastlines()
    ax[1,1].add_feature(paises, edgecolor='black')
    ax[1,1].add_feature(provincias, edgecolor='gray')

    fig.subplots_adjust(right=0.8)
    cbar_ax = fig.add_axes([0.85, 0.15, 0.05, 0.7])
    fig.colorbar(im, cax=cbar_ax)

    plt.savefig('v2_' + str(indice).zfill(2) + '.jpg', dpi=150, bbox_inches='tight')
    plt.close(fig)

