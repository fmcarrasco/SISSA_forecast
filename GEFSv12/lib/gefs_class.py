import os
import numpy as np
import numpy.ma as ma
import pandas as pd
import datetime as dt
from netCDF4 import Dataset, num2date, date2num
from numba import jit

########################################################
############ FUNCIONES PARA CALCULAR DATOS DIARIOS #####

def calc_precip(precip):
    return ma.sum(precip, axis=0)

def calc_tmean(t2):
    return ma.mean(t2-273., axis=0)

def calc_tmax(t2):
    return ma.max(t2-273., axis=0)

def calc_tmin(t2):
    return ma.min(t2-273., axis=0)

def calc_spmean(ps):
    return ma.mean(ps, axis=0)

def calc_u10mean(u10):
    return ma.mean(u10, axis=0)

@jit(nopython=True)
def calc_ROCsfc(us):
    sfcSW = us
    dval = np.nan
    if np.sum(np.isnan(sfcSW)) > 0:
        #HAY NANS
        dval = np.nan
    else:
        x = np.arange(0,24)*60.*60. # segundos
        y = np.empty(24) # W/m2
        y[0:6] = sfcSW[0]
        y[6:12] = sfcSW[1]
        y[12:18] = sfcSW[2]
        y[18:24] = sfcSW[3]
        dval = np.trapz(y,x) # J/m2
    return dval

@jit(nopython=True)
def calc_ROLnet(dl):
    netLW = dl
    dval = np.nan
    if np.sum(np.isnan(netLW)) > 0:
        #HAY NANS
        dval = np.nan
    else:
        x = np.arange(0,24)*60.*60. # segundos
        y = np.empty(24) # W/m2
        y[0:6] = netLW[0]
        y[6:12] = netLW[1]
        y[12:18] = netLW[2]
        y[18:24] = netLW[3]
        dval = np.trapz(y,x) # J/m2
    return dval

@jit(nopython=True)
def calc_u10(vientos):
    return np.mean(vientos)

@jit(nopython=True)
def calc_rh(hrs):
    return np.mean(hrs)

class gefs_class:
    def __init__(self, variable, fecha, nens):
        self.nens = nens
        self.variable = variable
        if variable == 'wspd':
            v_arch = 'ugrd_hgt'
        elif variable == 'LWnet':
            v_arch = 'dlwrf_sfc'
        elif variable == 'rel_hum':
            v_arch = 'spfh_2m'
        elif variable == 'dew_p':
            v_arch = 'tmp_2m'
        elif variable == 'vap_press':
            v_arch = 'tdmean'
        else:
            v_arch = variable
        self.fecha = dt.datetime.strptime(fecha, '%Y%m%d')
        self.carpeta = '/shera/datos/SISSA/GEFSv12/'
        self.archivo =  v_arch + '_' + self.fecha.strftime('%Y%m%d') + '00_' + nens+ '.nc'
        self.archivo1 =  self.carpeta + v_arch + self.fecha.strftime('/%Y/%Y%m%d00/')+'d1-10/' + self.archivo
        self.archivo2 =  self.carpeta + v_arch + self.fecha.strftime('/%Y/%Y%m%d00/')+'d10-35/' + self.archivo
        print(self.archivo1)
        print(self.archivo2)
        # Check if both files exist
        f1 = not os.path.isfile(self.archivo1)
        f2 = not os.path.isfile(self.archivo2)
        if (f1 or f2):
            print('No existen los dos archivos para la fecha, no se genera archivo diario para')
            print(self.fecha.strftime('%Y-%m-%d'), self.variable, self.nens)
            self.continuar = True
        else:
            self.continuar = False
            self.variable_names = {'apcp_sfc':'rain', 'tmp_2m':'tmean', 'tmax_2m':'tmax', 'tmin_2m':'tmin', 'pres_sfc':'spmean',
                    'ugrd_hgt':'u10mean', 'vgrd_hgt':'v10mean', 'wspd':'u10', 'dswrf_sfc':'ROCsfc', 'ulwrf_sfc':'uROLsfc',
                    'dlwrf_sfc':'dROLsfc', 'LWnet':'ROLnet', 'rel_hum':'rh'}
            self.long_names = {'apcp_sfc':'daily precipitation 12UTC 12UTC', 'tmp_2m':'2m daily mean temperature 0-23 UTC',
                               'tmax_2m':'2m max temperature 0-23 UTC', 'tmin_2m':'2m min temperature 0-23 UTC',
                               'pres_sfc':'mean surface pressure 0-23 UTC', 'ugrd_hgt':'10m zonal mean wind 0-23 UTC',
                               'vgrd_hgt':'10m meridional mean wind 0-23 UTC', 'wspd':'10m mean wind velocity',
                               'dswrf_sfc':'Downward Surface Short Wave radiation 0-23 UTC',
                               'ulwrf_sfc':'Upward Surface Short Wave radiation 0-23 UTC',
                               'dlwrf_sfc':'Downward Surface Short Wave radiation 0-23 UTC',
                               'LWnet':'Surface Long Wave NET radiation 0-23 UTC',
                               'rel_hum':'2m mean relative humidity'}
            self.std_names = {'apcp_sfc':'daily_precipitation', 'tmp_2m':'mean_air_temperature',
                              'tmax_2m':'max_air_temperature', 'tmin_2m':'min_air_temperature',
                              'pres_sfc':'mean_surface_pressure', 'ugrd_hgt':'mean_zonal_wind',
                              'vgrd_hgt':'mean_meridional_wind', 'wspd': 'mean_wind_speed',
                              'dswrf_sfc':'surface_downward_shortwave_radiation','ulwrf_sfc':'upward_surface_longwave_radiation',
                              'dlwrf_sfc':'downward_surface_longwave_radiation', 'LWnet':'surface_longwave_net_radiation',
                              'rel_hum': 'mean_relative_humidity'}
            self.get_latlon()
            self.get_time()
            self.get_variable()
        
    def get_latlon(self):
        #Solo datos en 0.25
        self.nc = Dataset(self.archivo1, 'r')
        #
        self.lat = self.nc.variables['lat'][:]
        self.lat_bnds = self.nc.variables['lat_bnds'][:]
        self.lon = self.nc.variables['lon'][:]
        self.lon_bnds = self.nc.variables['lon_bnds'][:]
        #
        self.nc.close()
    
    def get_time(self):
        self.nc1 = Dataset(self.archivo1, 'r')
        self.nc2 = Dataset(self.archivo2, 'r')
        t1 = num2date(self.nc1['time'][:], self.nc1['time'].units,
                      calendar=self.nc1['time'].calendar,
                      only_use_cftime_datetimes=False,
                      only_use_python_datetimes=True)
        t2 = num2date(self.nc2['time'][:], self.nc2['time'].units,
                      calendar=self.nc2['time'].calendar,
                      only_use_cftime_datetimes=False,
                      only_use_python_datetimes=True)
        self.nc1.close()
        self.nc2.close()
        # Indice PP
        fechas = [dt.datetime(a.year, a.month, a.day, a.hour) for a in t1] +\
        [dt.datetime(a.year, a.month, a.day, a.hour) for a in t2]
        d1 = pd.Series(index=fechas, data=fechas)
        if self.variable == 'apcp_sfc':
            grupos = d1.groupby(pd.Grouper(freq='24H', origin=fechas[2], label='left')).ngroup().to_numpy()
            aux = pd.to_datetime(d1.groupby(pd.Grouper(freq='24H', origin=fechas[1], label='left')).nth(0)).to_list()
            fechas_d = [dt.datetime(a.year, a.month, a.day) for a in aux[1:-1]]
        else:
            grupos = d1.groupby(pd.Grouper(freq='24H', origin=fechas[0], label='left')).ngroup().to_numpy()
            aux = pd.to_datetime(d1.groupby(pd.Grouper(freq='24H', origin=fechas[0], label='left')).nth(0)).to_list()
            fechas_d = [dt.datetime(a.year, a.month, a.day) for a in aux[0:-1]]
        #print(fechas_d)
        self.fechas = fechas
        self.grupos = grupos
        time_unit = 'days since 1900-01-01 00:00'
        time_calendar = 'proleptic_gregorian'
        time_vals = date2num(fechas_d, units=time_unit, calendar=time_calendar)
        self.fechas_d = time_vals
        
    
    def get_variable(self):
        import iris
        c1 = iris.load(self.archivo1)
        c2  = iris.load(self.archivo2)
        if self.variable == 'apcp_sfc':
            esquema = iris.analysis.AreaWeighted(mdtol=0.5)
        else:
            esquema = iris.analysis.Linear()
        #Obtenemos los cubos
        v025 = c1[0]
        v050 = c2[0]
        # Hacemos el regrid.
        v050_reg = v050.regrid(v025, esquema)
        A1 = v025.data
        A2 = v050_reg.data
        B = np.concatenate((A1,A2), axis=0)
        self.data = B
        self.standard_name = self.std_names[self.variable]
        self.units = v025.units
        self.long_name = self.long_names[self.variable]
        self.name_var = self.variable_names[self.variable]
    
    def load_daylyfunc(self):
        self.fun_daily = {'tmp_2m': calc_tmean, 'apcp_sfc':calc_precip, 'tmax_2m':calc_tmax, 'tmin_2m':calc_tmin, 'pres_sfc':calc_spmean,
                'ugrd_hgt':calc_u10mean, 'vgrd_hgt':calc_u10mean, 'wspd': calc_u10, 'dswrf_sfc':calc_ROCsfc,'LWnet':calc_ROLnet,
                'rel_hum':calc_rh}
        self.units_daily = {'tmp_2m':'Celsius', 'apcp_sfc':'mm', 'tmax_2m':'Celsius', 'tmin_2m':'Celsius','pres_sfc':'Pa',
                'ugrd_hgt':'m s-1', 'vgrd_hgt':'m s-1', 'wspd':'m s-1', 'dswrf_sfc':'J m-2', 'ulwrf_sfc': 'J m-2', 'dlwrf_sfc':'J m-2',
                'LWnet':'J m-2', 'rel_hum':'%'}
        self.longname_daily = {'tmp_2m': 'mean temperature at 2m 0-23 UTC', 'apcp_sfc': 'total precipitation 12-12 UTC',
                               'tmax_2m': 'max temperature at 2m 0-23 UTC', 'tmin_2m': 'min temperature at 2m 0-23 UTC',
                               'pres_sfc': 'mean surface pressure 0-23 UTC', 'ugrd_hgt': 'mean zonal wind at 10m 0-23 UTC',
                               'vgrd_hgt': 'mean merdidional wind at 10m 0-23 UTC', 'wspd': 'mean wind speed at 10m 0-23 UTC',
                               'dswrf_sfc': 'Surface Downward Shortwave radiation 0-23 UTC', 
                               'ulwrf_sfc': 'Surface Upward Longwave radiation 0-23 UTC', 
                               'dlwrf_sfc': 'Surface Downward Longwave radiation 0-23 UTC', 
                               'LWnet':'Surface Longwave NET radiation 0-23 UTC',
                               'rel_hum':'mean 2m Relative Humidity 0-23 UTC'}

    def calc_daily(self):
        self.load_daylyfunc()
        nt = len(self.fechas_d) # cantidad de dias
        nt1, ny, nx = self.data.shape
        daily_d = np.empty((nt,ny,nx))
        if self.variable == 'apcp_sfc':
            for i in np.arange(1,35):
                igroup = (i == self.grupos)
                if np.sum(igroup)<3:
                    continue
                else:
                    daily_d[i-1,:,:] = self.fun_daily[self.variable](self.data[igroup,:,:])
        else:
            for i in np.arange(0,34):
                igroup = (i == self.grupos)
                if np.sum(igroup)<3:
                    continue
                else:
                    if self.variable == 'dswrf_sfc':
                        v1 = self.data[igroup,:,:].data
                        daily_d[i,:,:] = np.apply_along_axis(self.fun_daily[self.variable], 0, v1)
                    else:
                        daily_d[i,:,:] = self.fun_daily[self.variable](self.data[igroup,:,:])
        self.daily_data = daily_d
    
    def calc_daily_compound(self, d_var, nfunc):
        self.load_daylyfunc()
        nt = len(self.fechas_d) # cantidad de dias
        nt1, ny, nx = self.data.shape
        daily_d = np.empty((nt,ny,nx))
        if nfunc == 'calc_ROLnet':
            v1 = self.data.data
            v2 = d_var['ulwrf_sfc'].data
            LWnet = v1 - v2
            for i in np.arange(0,34):
                igroup = (i == self.grupos)
                if np.sum(igroup) < 3:
                    continue
                else:
                    daily_d[i,:,:] = np.apply_along_axis(calc_ROLnet, 0, LWnet[igroup,:,:])
        elif nfunc == 'calc_u10':
            v1 = self.data.data
            v2 = d_var['vgrd_hgt'].data
            vientos = np.sqrt(np.power(v1, 2) + np.power(v2, 2))
            for i in np.arange(0,34):
                igroup = (i == self.grupos)
                if np.sum(igroup) < 3:
                    continue
                else:
                    daily_d[i,:,:] = np.apply_along_axis(calc_u10, 0, vientos[igroup,:,:])
        elif nfunc == 'calc_rh':
            v0 = self.data.data # kg/kg adimensionless
            v1 = d_var['pres_sfc'].data # Pa
            v2 = d_var['tmp_2m'].data # Kelvin
            T0 = 273.16
            av0 = np.divide(v2 - T0, v2 - 29.65)
            av1 = np.exp(17.63 * av0)
            av2 = np.power(av1, -1)
            hrs = 0.263 * v1 * v0 * av2
            for i in np.arange(0,34):
                igroup = (i == self.grupos)
                if np.sum(igroup) < 3:
                    continue
                else:
                    daily_d[i,:,:] = np.apply_along_axis(calc_rh, 0, hrs[igroup,:,:])
        # END of IFs
        self.daily_data = daily_d
    
    def save_netcdf(self, ncfile):
        ds = Dataset(ncfile, 'w', format='NETCDF4')
        #---- Atributos globales
        ds.Conventions = 'CF-1.7'
        ds.title = 'ensemble member forecast run'
        ds.institution = 'NCEP'
        ds.source = 'GEFSv12 reforecast dataset'
        ds.history = 'Process for SISSA at ' + dt.datetime.utcnow().strftime('%d/%m/%Y %H:%M')
        ds.references = ''
        ds.comment = ''

        #---- Dimension Tiempo
        ds.createDimension('time', None)
        #---- Variable Tiempo
        t0 = ds.createVariable('time', 'u8', ('time',))
        t0.long_name = 'time'
        t0.standard_name = 'time'
        t0.units = 'days since 1900-01-01 00:00:00'
        t0.calendar = 'proleptic_gregorian'
        t0.axis = 'T'
        t0[:] = self.fechas_d

        #---- Dimension bounds
        ds.createDimension('bounds2', 2)

        #---- Dimension Lat
        ds.createDimension('lat', len(self.lat))
        #---- Variable lat BOUNDS
        y0b = ds.createVariable('lat_bnds', 'f8', ('lat', 'bounds2',))
        y0b[:] = self.lat_bnds
        #---- Variable lat
        y0 = ds.createVariable('lat', 'f8', ('lat',))
        y0.units = 'degrees_north'
        y0.long_name = 'latitude'
        y0.standard_name = 'latitude'
        y0.axis = 'Y'
        y0.bounds = 'lat_bnds'
        y0[:] = self.lat

        #---- Dimension Lon
        ds.createDimension('lon', len(self.lon))
        #---- Variable lon BOUNDS
        x0b = ds.createVariable('lon_bnds', 'f8', ('lon', 'bounds2',))
        x0b[:] = self.lon_bnds
        #---- Varible lon
        x0 = ds.createVariable('lon', 'f8', ('lon',))
        x0.units = 'degrees_east'
        x0.long_name = 'longitude'
        x0.standard_name = 'longitude'
        x0.axis = 'X'
        x0.bounds = 'lon_bnds'
        x0[:] = self.lon

        #---- Agregamos el dato
        fvar = ds.createVariable(self.name_var, datatype='f8', dimensions=('time', 'lat', 'lon',))
        fvar.standard_name = self.standard_name
        fvar.units = self.units_daily[self.variable]
        fvar.long_name = self.longname_daily[self.variable]
        fvar[:] = self.daily_data

        #---- Cerramos el archivo NetCDF
        ds.close()
    
    
