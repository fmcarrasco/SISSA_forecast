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

def calc_rh(t2m, d2m):
    d2m_c = d2m - 273.15
    t2m_c = t2m - 273.15
    a = (17.625*d2m_c)/(243.04 + d2m_c)
    b = (17.625*t2m_c)/(243.04 + t2m_c)
    c = 100*(np.exp(a)/np.exp(b))
    return ma.mean(c, axis=0)

def calc_u2(u10, v10):
    a = 0.75*ma.sqrt(u10**2 + v10**2)  # 0.75 approx to wind at 2 m
    return ma.mean(a, axis=0)

def calc_radsup(ssrd, dtime):
    y = ssrd/3600.  # W/m2
    horas = np.array([a.hour + a.minute/60 for a in dtime])
    x = horas * 60 * 60 # segundos
    tot_rad = np.trapz(y, x, axis=0) * 1.e-6  # Mj/m2
    return tot_rad

def calc_rain(tp):
    return ma.sum(tp*1000., axis=0)  # mm [tp in m]

def calc_mslmean(msl):
    return ma.mean(msl*0.001, axis=0)  # kPa

def calc_spmean(sp):
    return ma.mean(sp*0.001, axis=0)  # kPa

class erah_class:
    def __init__(self, yr):
        self.year = yr
        self.file1 = './shera/ERA5/' + str(yr) + '.nc'
        self.file2 = './shera/ERA5/' + str(yr+1) + '.nc'
        self.nc = Dataset(self.file1, 'r')
        self.var = list(self.nc.variables.keys())
        self.var.remove('time')
        self.var.remove('longitude')
        self.var.remove('latitude')

        self.get_latlon()
        self.get_time()
        #self.get_variables()
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
        if self.year != 2022:
            nc2 = Dataset(self.file2, 'r')
            time2 = nc2.variables['time']
            aux_t2 = num2date(time2[:], time2.units,
                              only_use_cftime_datetimes=False,
                              only_use_python_datetimes=True)
            aux_local = [a - dt.timedelta(hours=3) for a in [*aux_t, *aux_t2]]
            years = np.array([a.year for a in aux_local])
            self.units['time'] = self.nc.variables['time'].units
            self.indices = years == self.year
            self.dtime = np.array(aux_local)[self.indices]
            nc2.close()
        else:
            aux_local = [a - dt.timedelta(hours=3) for a in aux_t]
            d_local = [a for a in aux_local if ((a.month ==1) | (a.month ==2) | (a.month ==3))]
            self.units['time'] = self.nc.variables['time'].units
            self.indices = [True if ((a.month ==1) | (a.month ==2) | (a.month ==3)) else False for a in aux_local]
            self.dtime = np.array(aux_local)[self.indices]

    def get_variables(self):
        self.datos = {}
        self.mask  = {}
        if self.year != 2022:
            nc2 = Dataset(self.file2, 'r')
            for v in self.var[0:1]:
                fill_value = self.nc.variables[v]._FillValue
                a = ma.getdata(self.nc.variables[v][:])
                b = ma.getdata(nc2.variables[v][:])
                if len(b.shape) > 3:
                    c = np.concatenate((a,b[:,0,:,:]), axis=0)
                else:
                    c = np.concatenate((a,b), axis=0)
                self.datos[v] = c[self.indices,:,:]
                self.mask[v] = self.datos[v] == fill_value
                self.units[v] = self.nc.variables[v].units
            nc2.close()
            self._FillValue = fill_value
        else:
            ndim = len(self.nc.dimensions.keys())
            vari = self.var.copy()
            if  ndim > 3:
                vari.remove('expver')
            for v in vari:
                fill_value = self.nc.variables[v]._FillValue
                if ndim > 3:
                    a = ma.getdata(self.nc.variables[v][:,0,:,:])
                else:
                    a = ma.getdata(self.nc.variables[v][:])
                self.datos[v] = a[self.indices,:,:]
                self.mask[v] = self.datos[v] == fill_value
                self.units[v] = self.nc.variables[v].units
            self._FillValue = fill_value

    def load_daylyfunc(self):
        self.fun_daily = {'tmax':calc_tmax, 'tmin': calc_tmin, 'tmean': calc_tmean,
                          'rh':calc_rh, 'u2':calc_u2, 'rs':calc_radsup, 'rain':calc_rain,
                          'mslmean':calc_mslmean, 'spmean':calc_spmean}

        self.units_daily = {'tmax':'Celsius', 'tmin':'Celsius', 'tmean':'Celsius',
                            'rh':'%', 'u2':'m s**-1', 'rs':'MJ m-2 d-1', 'rain':'mm',
                            'mslmean':'kPa', 'spmean':'kPa'}
        self.longname_daily = {'tmax':'maximum temperature at 2m', 'tmin':'minimum temperature at 2m',
                               'tmean':'mean temperature at 2m', 'rh':'mean relative humidity',
                               'u2':'mean wind speed at 2 m', 'rs':'incoming solar radiation',
                               'rain':'total precipitation', 'mslmean':'mean sea level pressure',
                               'spmean':'mean surface pressure'}

    def calc_daily(self):
        var_diarias = ['tmax', 'tmin', 'tmean', 'rh', 'u2', 'rs', 'rain', 'mslmean', 'spmean']
        datos_diarios = {}
        n1 = len(self.dtime)
        nd = int(n1/24)
        for v in var_diarias:
            print('Processing ',v)
            out = np.empty((nd, self.ny, self.nx ))
            for it, t in enumerate(np.arange(0, n1, 24)):
                if (v == 'tmax') | (v == 'tmin') | (v == 'tmean'):
                    var = ma.masked_array(self.datos['t2m'], self.mask['t2m'])
                    out[it, :, :] = self.fun_daily[v](var[t:t+24,:,:])
                elif v == 'rh':
                    var0 = ma.masked_array(self.datos['t2m'], self.mask['t2m'])
                    var1 = ma.masked_array(self.datos['d2m'], self.mask['d2m'])
                    out[it, :, :] = self.fun_daily[v](var0[t:t+24,:,:], var1[t:t+24,:,:])
                elif v == 'rs':
                    var = ma.masked_array(self.datos['ssrd'], self.mask['ssrd'])
                    out[it, :, :] = self.fun_daily[v](var[t:t+24,:,:], self.dtime[t:t+24])
                elif v == 'u2':
                    var0 = ma.masked_array(self.datos['u10'], self.mask['u10'])
                    var1 = ma.masked_array(self.datos['v10'], self.mask['v10'])
                    out[it, :, :] = self.fun_daily[v](var0[t:t+24,:,:], var1[t:t+24,:,:])
                elif v == 'rain':
                    var = ma.masked_array(self.datos['tp'], self.mask['tp'])
                    out[it, :, :] = self.fun_daily[v](var[t:t+24,:,:])
                elif v == 'mslmean':
                    var = ma.masked_array(self.datos['msl'], self.mask['msl'])
                    out[it, :, :] = self.fun_daily[v](var[t:t+24,:,:])
                elif v == 'spmean':
                    var = ma.masked_array(self.datos['sp'], self.mask['sp'])
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
