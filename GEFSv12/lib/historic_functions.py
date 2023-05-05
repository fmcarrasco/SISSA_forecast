import numpy as np
import numpy.ma as ma
from netCDF4 import Dataset, num2date


def get_era5hist_data(nomvar, era5_f, meses):
    print('## Extrayendo datos ERA5 ##')
    out_d = {}
    for mes in meses:
        if mes - 1 <= 0:
            cnd = [12, 1, 2]
        elif mes + 1 >= 13:
            cnd = [11, 12, 1]
        else:
            cnd = [mes - 1, mes, mes + 1]
        lista_datos = []
        for year in np.arange(2000,2010):
            fobs = era5_f + nomvar + '/' + str(year) + '.nc'
            nc_era = Dataset(fobs, 'r')
            #
            tiempos = nc_era.variables['time']
            fechas = num2date(tiempos[:], units=tiempos.units, calendar=tiempos.calendar,
                            only_use_cftime_datetimes=False,
                            only_use_python_datetimes=True)
            meses_d = np.array([a.month for a in fechas])
            indice = (meses_d == cnd[0]) | (meses_d == cnd[1]) | (meses_d == cnd[2])
            datos = nc_era.variables[nomvar][indice,:,:]
            nc_era.close()
            lista_datos.append(datos)
        out_d[str(mes)] = np.concatenate(lista_datos, axis=0)
    return out_d

def get_era5_serie(d_era5,i,j):
    out_d = {}
    for key in d_era5.keys():
        out_d[key] = d_era5[key][:,j,i]
    return out_d


def get_gefshist_data(nomvar, gefs_f, meses, plazos):
    print('## Extrayendo datos GEFS ##')
    ensambles = ['c00', 'p01', 'p02', 'p03', 'p04', 'p05', 'p06', 'p07', 'p08', 'p09', 'p10']
    #######################
    # Distribucion plazo
    out_d = {}
    for mes, plazo in zip(meses, plazos):
        plazo_str = str(int(plazo)).zfill(2)
        if mes - 1 <= 0:
            cnd = [12, 1, 2]
        elif mes + 1 >= 13:
            cnd = [11, 12, 1]
        else:
            cnd = [mes - 1, mes, mes + 1]
        lista_datos = []
        fmod = gefs_f + 'Distrib/' + nomvar + '/' + nomvar + '_' + plazo_str + '.nc'
        nc_mod = Dataset(fmod, 'r')
        tiempos = nc_mod.variables['time']
        fechas = num2date(tiempos[:], units=tiempos.units, calendar=tiempos.calendar,
                          only_use_cftime_datetimes=False,
                          only_use_python_datetimes=True)
        meses_d = np.array([a.month for a in fechas])
        indice = (meses_d == cnd[0]) | (meses_d == cnd[1]) | (meses_d == cnd[2])
        lista_datos = []
        for ens in ensambles:
            datos = nc_mod.variables[ens][indice,:,:]
            lista_datos.append(datos)
        M1 = np.stack(lista_datos, axis=0)
        llave = str(mes) + '_' + plazo_str
        out_d[llave] = ma.mean(M1, axis=0)
        
        nc_mod.close()
    return out_d

def get_gefshist_serie(d_gefs,i,j):
    out_d = {}
    for key in d_gefs.keys():
        out_d[key] = d_gefs[key][:,j,i]
    return out_d