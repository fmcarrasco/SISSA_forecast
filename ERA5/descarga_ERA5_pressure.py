"""
Created on Fri Jun 24 16:29:30 2022

@author: cristian
2022/11: Modificado por F. Carrasco para descarga de datos ERA5 proyecto SISSA
"""
import numpy as np
import cdsapi
import os

carpeta = '/datos2/SISSA/ERA5/'
os.makedirs(carpeta, exist_ok=True)
c = cdsapi.Client()

years = [str(year) for year in np.arange(1998,1999)]

for iyear in years:
    print('Descargando el año: ' + iyear)
    c.retrieve(
        'reanalysis-era5-pressure-levels',
        {
            'product_type': 'reanalysis',
            'format': 'netcdf',
            'pressure_level': '200',
            'variable': [
                'geopotential',
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
                -8, -82, -57,
                -33,
            ],
        },
        carpeta + iyear + '_pressure.nc')
