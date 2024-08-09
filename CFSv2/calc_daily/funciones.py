import xarray as xr


def variable_diaria(var):
    if var == 'tmax_2m':
        return 'tmax'
    elif var == 'tmin_2m':
        return 'tmin'
    elif var == 'tmp_2m':
        return 'tmean'
    elif var == 'prate':
        return 'rain'

def calculo_diario(ds, nvar, v025, esquema):
    if nvar == 'tmax_2m':
        var = ds[nvar]
        var = var.rename('tmax')
        daily = var.resample(time='1D').max() - 273.15
        daily.time.encoding["units"] = 'days since 1900-01-01 00:00:00'
        daily = daily.assign_attrs({'standard_name':'air_temperature','units':'degC', 'long_name':'maximum temperature at 2m'})
        # interpolate using IRIS
        daily_ir = daily.to_iris()
        v050_reg = daily_ir.regrid(v025, esquema)
        
        return v050_reg
    
    elif nvar == 'tmin_2m':
        var = ds[nvar]
        var = var.rename('tmin')
        daily = var.resample(time='1D').min() - 273.15
        daily.time.encoding["units"] = 'days since 1900-01-01 00:00:00'
        daily = daily.assign_attrs({'standard_name':'air_temperature', 'units':'degC', 'long_name':'minimum temperature at 2m'})
        # interpolate using IRIS
        daily_ir = daily.to_iris()
        v050_reg = daily_ir.regrid(v025, esquema)
        
        return v050_reg

    elif nvar == 'tmp_2m':
        var = ds[nvar]
        var = var.rename('tmean')
        daily = var.resample(time='1D').mean() - 273.15
        daily.time.encoding["units"] = 'days since 1900-01-01 00:00:00'
        daily = daily.assign_attrs({'standard_name':'air_temperature', 'units':'degC', 'long_name':'minimum temperature at 2m'})
        # interpolate using IRIS
        daily_ir = daily.to_iris()
        v050_reg = daily_ir.regrid(v025, esquema)
        
        return v050_reg



