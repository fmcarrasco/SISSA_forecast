"""
Autores: Dr.Cristian Waimann
         Mg. Ing. Felix Carrasco

Clase de python para trabajar con archivos netcdf de ERA5 a escala diaria
Permite extraer las variables en un objeto (lat/lon/tiempo + variables)

Programada para trabajar en concultoria a Allkem ltda.
"""

from netCDF4 import Dataset, num2date
import numpy as np
import numpy.ma as ma
import pandas as pd
import datetime as dt

class erad_class:
    def __init__(self, yr):
        self.year = yr
        self.file = '../../2.OutputDaily/' + str(yr) + '.nc'
        self.nc = Dataset(self.file, 'r')
        self.var = list(self.nc.variables.keys())
        self.var.remove('time')
        self.var.remove('longitude')
        self.var.remove('latitude')

        self.get_latlon()
        self.get_time()
        self.get_variables()

    def get_latlon(self):
        self.units = {}
        self.units['latitude'] = self.nc.variables['latitude'].units
        self.units['longitude'] = self.nc.variables['longitude'].units
        self.lat = self.nc.variables['latitude'][:]
        self.lon = self.nc.variables['longitude'][:]
        self.nx = len(self.lon)
        self.ny = len(self.lat)

    def get_time(self):
        time = self.nc.variables['time']
        aux_t = num2date(time[:], time.units,
                         only_use_cftime_datetimes=False,
                         only_use_python_datetimes=True)
        self.units['time'] = self.nc.variables['time'].units
        self.dtime = [dt.datetime(a.year, a.month, a.day) for a in aux_t]

    def get_variables(self):
        self.datos = {}
        self.mask  = {}
        for v in self.var:
            fill_value = self.nc.variables[v]._FillValue
            a = ma.getdata(self.nc.variables[v][:])
            self.datos[v] = a
            self.mask[v] = self.datos[v] == fill_value
            self.units[v] = self.nc.variables[v].units
        self._FillValue = fill_value

    def interp_data(self, lon0, lat0):
        '''
        This function returns a DataFrame from all variables at lon0, lat0
        The interpolation is bilinear using interp2d method from scipy.interpolate
        '''
        from scipy.interpolate import interp2d
        sel_data = {}
        for v in self.var:
            out_v = np.empty((len(self.dtime)))
            for t in range(0,len(self.dtime)):
                f = interp2d(self.lon, self.lat, self.datos[v][t,:,:], kind='linear')
                out_v[t] = f(lon0, lat0)
            sel_data[v] = out_v
        sel_data['Fechas'] = self.dtime
        df = pd.DataFrame.from_dict(sel_data)
        df.set_index('Fechas', inplace=True)

        return df
