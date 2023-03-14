import os
import numpy as np
import numpy.ma as ma
import pandas as pd
import datetime as dt
from netCDF4 import Dataset, num2date, date2num

########################################################
############ FUNCIONES PARA CALCULAR DATOS DIARIOS #####

def calc_precip(precip):
    return ma.sum(precip, axis=0)

def calc_tmean(t2):
    return ma.mean(t2-273., axis=0)


class gefs_class:
    def __init__(self, variable, fecha, nens):
        self.nens = nens
        self.variable = variable
        self.fecha = dt.datetime.strptime(fecha, '%Y%m%d')
        self.carpeta = '/shera/datos/SISSA/GEFSv12/'
        self.archivo =  variable + '_' + self.fecha.strftime('%Y%m%d') + '00_' + nens+ '.nc'
        self.archivo1 =  self.carpeta + variable + self.fecha.strftime('/%Y/%Y%m%d00/')+'d1-10/' + self.archivo
        self.archivo2 =  self.carpeta + variable + self.fecha.strftime('/%Y/%Y%m%d00/')+'d10-35/' + self.archivo
        # Check if both files exist
        f1 = not os.path.isfile(self.archivo1)
        f2 = not os.path.isfile(self.archivo2)
        if (f1 or f2):
            print('No existen los dos archivos para la fecha, no se genera archivo diario para')
            print(self.fecha.strftime('%Y-%m-%d'), self.variable, self.nens)
            self.continuar = True
        else:
            self.continuar = False
            self.variable_names = {'apcp_sfc':'rain', 'tmp_2m':'tmean'}
            self.long_names = {'apcp_sfc':'daily precipitation 12UTC 12UTC', 'tmp_2m':'2m daily mean temperature 0-23 UTC'}
            self.std_names = {'apcp_sfc':'daily_precipitation', 'tmp_2m':'mean_air_temperature'}
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
        self.fun_daily = {'tmp_2m': calc_tmean, 'apcp_sfc':calc_precip}
        self.units_daily = {'tmp_2m':'Celsius', 'apcp_sfc':'mm'}
        self.longname_daily = {'tmp_2m':'mean temperature at 2m 0-23 UTC','apcp_sfc':'total precipitation 12-12 UTC'}

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
                    daily_d[i,:,:] = self.fun_daily[self.variable](self.data[igroup,:,:])
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
    
    
