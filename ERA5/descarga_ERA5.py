#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 24 16:29:30 2022

@author: cristian
2022/11: Modificado por F. Carrasco para descarga de datos ERA5 proyecto SISSA
"""
import numpy as np
import cdsapi
import os

carpeta = './descargas/shera/ERA5/'
os.makedirs(carpeta, exist_ok=True)

c = cdsapi.Client()

years = [str(year) for year in np.arange(2011,2012)]

for iyear in years:
    print('Descargando el año: ' + iyear)
    c.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'format': 'netcdf',
            'variable': [
                '10m_u_component_of_wind', '10m_v_component_of_wind', '2m_dewpoint_temperature',
                '2m_temperature', 'instantaneous_surface_sensible_heat_flux', 'mean_sea_level_pressure',
                'surface_pressure', 'surface_net_solar_radiation', 'surface_sensible_heat_flux',
                'surface_solar_radiation_downwards', 'total_cloud_cover',
                'total_sky_direct_solar_radiation_at_surface','total_precipitation',
            ],
            'year': iyear,
            'month': [
                '01', '02', '03',
                '04', '05', '06',
                '07', '08', '09',
                '10', '11', '12',
            ],
            'day': [
                '01', '02', '03',
                '04', '05', '06',
                '07', '08', '09',
                '10', '11', '12',
                '13', '14', '15',
                '16', '17', '18',
                '19', '20', '21',
                '22', '23', '24',
                '25', '26', '27',
                '28', '29', '30',
                '31',
            ],
            'time': [
                '00:00', '01:00', '02:00',
                '03:00', '04:00', '05:00',
                '06:00', '07:00', '08:00',
                '09:00', '10:00', '11:00',
                '12:00', '13:00', '14:00',
                '15:00', '16:00', '17:00',
                '18:00', '19:00', '20:00',
                '21:00', '22:00', '23:00',
            ],
            'area': [
                -9.5, -81, -56,
                -34,
            ],
        },
        carpeta + iyear + '.nc')
