"""
Autores: Dr.Cristian Waimann
         Mg. Ing. Felix Carrasco

Clase de python para trabajar con archivos netcdf de ERA5
Permite extraer las variables en un objeto (lat/lon/tiempo + variables)
Permite hacer calculos en escala diaria para cada punto de retícula

Programada para trabajar en concultoria a Allkem ltda.
"""

from netCDF4 import Dataset, num2date
import numpy as np
import numpy.ma as ma
import pandas as pd
import datetime as dt

########################################################
############ FUNCIONES PARA CALCULAR DATOS DIARIOS #####

## https://confluence.ecmwf.int/pages/viewpage.action?pageId=197702790

def calc_tmax(t2m):
    return ma.max(t2m - 273.15, axis=0)

def calc_tmin(t2m):
    return ma.min(t2m - 273.15, axis=0)

def calc_tmean(t2m):
    return ma.mean(t2m - 273.15, axis=0)

def calc_umean(vcomp):
    return ma.mean(vcomp, axis=0)

def calc_rh(t2m, d2m):
    d2m_c = d2m - 273.15
    t2m_c = t2m - 273.15
    a = (17.625*d2m_c)/(243.04 + d2m_c)
    b = (17.625*t2m_c)/(243.04 + t2m_c)
    c = 100*(np.exp(a)/np.exp(b))
    return ma.mean(c, axis=0)

def calc_u10(u10, v10):
    a = ma.sqrt(u10**2 + v10**2)  # 0.75 approx to wind at 2 m
    return ma.mean(a, axis=0)

def calc_radsup(ssrd, dtime):
    y = ssrd/3600.  # W/m2
    horas = np.array([a.hour + a.minute/60 for a in dtime])
    x = horas * 60 * 60 # segundos
    tot_rad = np.trapz(y, x, axis=0)  # j/m2
    return tot_rad

def calc_pvmean(d2m):
    d2m_c = d2m - 273.15
    a = (7.5*d2m_c)/(237.3 + d2m_c)
    b = 6.11*10**a
    return ma.mean(b, axis=0)

def calc_rain(tp):
    return ma.sum(tp*1000., axis=0)  # mm [tp in m]

def calc_mslmean(msl):
    return ma.mean(msl, axis=0)  # Pa

def calc_spmean(sp):
    return ma.mean(sp, axis=0)  # Pa

class erah_class:
    def __init__(self, yr):
        self.year = yr
        self.file1 = '/datos2/SISSA/ERA5/' + str(yr) + '_pressure.nc'
        self.file2 = '/datos2/SISSA/ERA5/' + str(yr+1) + '_pressure.nc'
        #self.file1 = '/datos2/SISSA/ERA5/' + str(yr) + '.nc'
        #self.file2 = '/datos2/SISSA/ERA5/' + str(yr+1) + '.nc'
        #self.file1 = '/shera/datos/ERA5/' + str(yr) + '.nc'
        #self.file2 = '/shera/datos/ERA5/' + str(yr+1) + '.nc'
        self.nc = Dataset(self.file1, 'r')
        self.var = list(self.nc.variables.keys())
        self.var.remove('time')
        self.var.remove('longitude')
        self.var.remove('latitude')

        self.get_latlon()
        self.get_time()
        self.load_daylyfunc()
        self.nc.close()

    def get_latlon(self):
        self.units = {}
        self.units['latitude'] = self.nc.variables['latitude'].units
        self.units['longitud'] = self.nc.variables['longitude'].units
        self.lat = self.nc.variables['latitude'][:]
        self.lon = self.nc.variables['longitude'][:]
        self.nx = len(self.lon)
        self.ny = len(self.lat)

    def get_time(self):
        time = self.nc.variables['time']
        aux_t = num2date(time[:], time.units,
                         only_use_cftime_datetimes=False,
                         only_use_python_datetimes=True)
        self.dtime_utc = aux_t
        self.dtime_local = [a - dt.timedelta(hours=3) for a in aux_t]
        nc2 = Dataset(self.file2, 'r')
        time2 = nc2.variables['time']
        aux_t2 = num2date(time2[:], time2.units,
                          only_use_cftime_datetimes=False,
                          only_use_python_datetimes=True)
        aux_local = [dt.datetime(a.year, a.month, a.day, a.hour, 0, 0) for a in aux_t]
        aux_local1 = [dt.datetime(a.year, a.month, a.day, a.hour, 0, 0)  for a in aux_t2]
        aux_local.extend(aux_local1)
        years = np.array([a.year for a in aux_local])
        # Tiempo para todas las variables, excepto precipitacion
        self.units['time'] = self.nc.variables['time'].units
        self.indices = years == self.year
        self.dtime = np.array(aux_local)[self.indices]
        # Tiempo para variable precipitacion
        d1 = dt.datetime(self.year, 1, 1, 12, 0, 0)
        d2 = dt.datetime(self.year+1, 1, 1, 11, 0, 0) 
        self.indices_pp = [True if ((a >= d1) & (a <= d2)) else False for a in aux_local]
        self.dtime_pp = np.array(aux_local)[self.indices_pp]
        #
        nc2.close()

    def get_variables(self, v):
        if v not in self.var:
            print(v, ' No se encuentra en este archivo, utilizar:')
            print(self.var)
            exit()
        nc1 = Dataset(self.file1, 'r')
        nc2 = Dataset(self.file2, 'r')
        fill_value = nc1.variables[v]._FillValue
        a = ma.getdata(nc1.variables[v][:])
        b = ma.getdata(nc2.variables[v][:])
        c = np.concatenate((a,b), axis=0)
        if v == 'tp':
            datos = c[self.indices_pp,:,:]
            mask = datos == fill_value
        else:
            datos = c[self.indices,:,:]
            mask = datos == fill_value
        units = nc1.variables[v].units
        nc1.close()
        nc2.close()
        return datos, mask, units, fill_value

    def load_daylyfunc(self):
        self.fun_daily = {'tmax':calc_tmax, 'tmin': calc_tmin, 'tmean': calc_tmean,
                          'rh':calc_rh, 'u10':calc_u10, 'ROCsfc':calc_radsup, 'rain':calc_rain,
                          'mslmean':calc_mslmean, 'spmean':calc_spmean, 'ROLnet': calc_radsup,
                          'pvmean': calc_pvmean, 'u10mean': calc_umean, 'v10mean':calc_umean,
                          'tdmean': calc_tmean, 'z200':calc_tmean}

        self.units_daily = {'tmax':'Celsius', 'tmin':'Celsius', 'tmean':'Celsius',
                            'rh':'%', 'u10':'m s**-1', 'ROCsfc':'J m-2 d-1', 'rain':'mm',
                            'mslmean':'Pa', 'spmean':'Pa', 'ROLnet':'J m-2 d-1', 'pvmean':'hPa',
                            'u10mean':'m s-1', 'v10mean':'m s-1', 'tdmean':'Celsius','z200':'m**2 s**-2'}

        self.longname_daily = {'tmax':'maximum temperature at 2m', 'tmin':'minimum temperature at 2m',
                               'tmean':'mean temperature at 2m', 'rh':'mean relative humidity',
                               'u10':'mean wind speed at 10 m', 'ROCsfc':'incoming solar radiation',
                               'rain':'total precipitation', 'mslmean':'mean sea level pressure',
                               'spmean':'mean surface pressure', 'ROLnet':'Net LongWave radiation',
                               'pvmean':'mean Vapor Pressure', 'u10mean':'mean U wind component',
                               'v10mean':'mean V wind component', 'tdmean':'mean Dew-point pressure',
                               'z200':'mean 200hPa geopotential height'}

    def calc_daily(self, v, datos, mask):
        var_diarias = list(self.fun_daily.keys())
        if v not in var_diarias:
            print(v, ' No se encuentra para calcular variables diarias, utiliza alguna de: ')
            print(var_diarias)
            exit()
        datos_diarios = {}
        n1 = len(self.dtime)
        nd = int(n1/24)
        print('Processing ',v)
        out = np.empty((nd, self.ny, self.nx ))
        for it, t in enumerate(np.arange(0, n1, 24)):
            if v == 'tmean':#(v == 'tmax') | (v == 'tmin') | (v == 'tmean'):
                var = ma.masked_array(datos['t2m'], mask['t2m'])
                out[it, :, :] = self.fun_daily[v](var[t:t+24,:,:])
            elif v == 'tmax':
                #var = ma.masked_array(datos['mx2t'], mask['mx2t'])
                var = ma.masked_array(datos['t2m'], mask['t2m'])
                out[it, :, :] = self.fun_daily[v](var[t:t+24,:,:])
            elif v == 'tmin':
                #var = ma.masked_array(datos['mn2t'], mask['mn2t'])
                var = ma.masked_array(datos['t2m'], mask['t2m'])
                out[it, :, :] = self.fun_daily[v](var[t:t+24,:,:])
            elif v == 'rh':
                var0 = ma.masked_array(datos['t2m'], mask['t2m'])
                var1 = ma.masked_array(datos['d2m'], mask['d2m'])
                out[it, :, :] = self.fun_daily[v](var0[t:t+24,:,:], var1[t:t+24,:,:])
            elif v == 'ROCsfc':
                var = ma.masked_array(datos['ssrd'], mask['ssrd'])
                out[it, :, :] = self.fun_daily[v](var[t:t+24,:,:], self.dtime[t:t+24])
            elif v == 'ROLnet':
                var = ma.masked_array(datos['str'], mask['str'])
                out[it, :, :] = self.fun_daily[v](var[t:t+24,:,:], self.dtime[t:t+24])
            elif v == 'u10':
                var0 = ma.masked_array(datos['u10'], mask['u10'])
                var1 = ma.masked_array(datos['v10'], mask['v10'])
                out[it, :, :] = self.fun_daily[v](var0[t:t+24,:,:], var1[t:t+24,:,:])
            elif v == 'rain':
                var = ma.masked_array(datos['tp'], mask['tp'])
                out[it, :, :] = self.fun_daily[v](var[t:t+24,:,:])
            elif v == 'mslmean':
                var = ma.masked_array(datos['msl'], mask['msl'])
                out[it, :, :] = self.fun_daily[v](var[t:t+24,:,:])
            elif v == 'spmean':
                var = ma.masked_array(datos['sp'], mask['sp'])
                out[it, :, :] = self.fun_daily[v](var[t:t+24,:,:])
            elif v == 'tdmean':
                var = ma.masked_array(datos['d2m'], mask['d2m'])
                out[it, :, :] = self.fun_daily[v](var[t:t+24,:,:])
            elif v == 'u10mean':
                var = ma.masked_array(datos['u10'], mask['u10'])
                out[it, :, :] = self.fun_daily[v](var[t:t+24,:,:])
            elif v == 'v10mean':
                var = ma.masked_array(datos['v10'], mask['v10'])
                out[it, :, :] = self.fun_daily[v](var[t:t+24,:,:])
            elif v == 'pvmean':
                var = ma.masked_array(datos['d2m'], mask['d2m'])
                out[it, :, :] = self.fun_daily[v](var[t:t+24,:,:])
            elif v == 'z200':
                var = ma.masked_array(datos['z'], mask['z'])
                out[it, :, :] = self.fun_daily[v](var[t:t+24,:,:])

            datos_diarios[v] = out

        return datos_diarios

    def get_daily_datetimes(self):
        ini = self.dtime[0].strftime('%Y-%m-%d')
        fin = self.dtime[-1].strftime('%Y-%m-%d')

        return pd.date_range(ini, fin).to_pydatetime()

    def interp_data(self, lon0, lat0, var):
        '''
        This function returns a DataFrame from all variables at lon0, lat0
        The interpolation is bilinear using interp2d method from scipy.interpolate
        '''
        from scipy.interpolate import interp2d
        out_v = np.empty((len(self.dtime)))
        for t in range(0,len(self.dtime)):
            f = interp2d(self.lon, self.lat, self.datos[var][t,:,:]*1000, kind='linear')
            out_v[t] = f(lon0, lat0)


        return out_v
