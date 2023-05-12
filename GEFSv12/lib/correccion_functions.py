import numpy as np
import numpy.ma as ma
import pandas as pd
import datetime as dt
import xarray as xr

from statsmodels.distributions.empirical_distribution import ECDF

def qq_corr(val, ecdf_m, observaciones):
    cdf_limit = 0.9999999
    p1 = ecdf_m(val)
    if p1 > cdf_limit:
        p1 = cdf_limit
    corr_o = np.nanquantile(observaciones, p1, method='linear')
    return corr_o

def series_qqcorr(prono, era5_hist, gefs_hist):
    '''
    prono = un vector/ pronosticos para todos en dim time
    hist_era5 = una matriz (time, hist_time) / para cada valor en dim time hay una serie en dim hist_time
    hist_gefs = una matriz (time, hist_time) / para cada valor en dim time hay una serie en dim hist_time
    '''
    prono_corr = prono.copy()
    for i, val in enumerate(prono):
        if np.isnan(val):
            prono_corr[i] = np.nan
            continue
        else:
            df_m = ma.masked_invalid(gefs_hist[i,:])
            df_o = era5_hist[i,:]
            ecdf_m = ECDF(df_m)
            # Q-Q correction
            cdf_limit = 0.9999999
            p1 = ecdf_m(val)
            if p1 > cdf_limit:
                p1 = cdf_limit
            prono_corr[i] = np.nanquantile(df_o, p1, method='linear')

    return prono_corr

def qqcorr(prono, hist_era5, hist_gefs):
    '''
    prono = un numero / un valor de pronostico para un tiempo dado en dim time
    hist_era5 = un vector / para cada valor en dim time hay una serie en dim hist_time
    hist_gefs = un vector / para cada valor en dim time hay una serie en dim hist_time
    '''
    data_m1 = ma.masked_invalid(hist_era5)
    data_m2 = ma.masked_invalid(hist_gefs)
    ecdf_m = ECDF(data_m2)
    # Q-Q correction
    cdf_limit = 0.9999999
    p1 = ecdf_m(prono)
    if p1 > cdf_limit:
        p1 = cdf_limit
    prono_corr = ma.nanquantile(data_m1, p1, method='linear')

    return prono_corr

def qq_correcion(x,y,z):
    return xr.apply_ufunc(series_qqcorr, x, y, z, input_core_dims=[['time'], ['time', 'hist_time0'], ['time', 'hist_time1']],
                          output_core_dims = [['time']], dask='parallelized',vectorize=True,  # !Important!
                          output_dtypes=[float])
    
