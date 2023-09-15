import time
import numpy as np
import numpy.ma as ma
import pandas as pd
import datetime as dt
import xarray as xr
from numba import jit

#from memory_profiler import profile

from statsmodels.distributions.empirical_distribution import ECDF

#@jit(nopython=True)
def ecdf_numpy(x):
    x = np.sort(x)
    n = len(x)
    def _ecdf(v):
        # side='right' because we want Pr(x <= v)
        return (np.searchsorted(x, v, side='right')) / n
    return _ecdf

def test_qqcorr(prono, era5_hist, gefs_hist):
    prono_corr0 = prono.copy()
    prono_corr1 = prono.copy()
    for i, val in enumerate(prono):
        if np.isnan(val):
            prono_corr0[i] = np.nan
            prono_corr1[i] = np.nan
            continue
        else:
            inan_m = np.logical_not(np.isnan(gefs_hist[i,:]))
            inan_o = np.logical_not(np.isnan(era5_hist[i,:]))
            df_m = gefs_hist[i, inan_m]
            df_o = era5_hist[i, inan_o]
            ecdf_m0 = ecdf_numpy(df_m)
            ecdf_m1 = ECDF(df_m)
            cdf_limit = 0.9999999
            # Q-Q correction with numpy implementation
            p1 = ecdf_m0(val)
            if p1 > cdf_limit:
                p1 = cdf_limit
            prono_corr0[i] = np.nanquantile(df_o, p1, method='linear')
            # Q-Q correction with statmodels implementation
            p1 = ecdf_m1(val)
            if p1 > cdf_limit:
                p1 = cdf_limit
            prono_corr1[i] = np.nanquantile(df_o, p1, method='linear')

    return prono_corr0, prono_corr1

#@jit(nopython=True)
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
            inan_m = np.logical_not(np.isnan(gefs_hist[i,:]))
            inan_o = np.logical_not(np.isnan(era5_hist[i,:]))
            df_m = gefs_hist[i, inan_m]
            df_o = era5_hist[i, inan_o]
            ecdf_m = ecdf_numpy(df_m)
            # Q-Q correction
            cdf_limit = 0.9999999
            p1 = ecdf_m(val)
            if p1 > cdf_limit:
                p1 = cdf_limit
            prono_corr[i] = np.nanquantile(df_o, p1, method='linear')

    return prono_corr

#@profile
def run():
    prono = np.load('prono.npy')
    era5_hist = np.load('era5_hist.npy')
    gefs_hist = np.load('gefs_hist.npy')

    start = time.time()
    for i in range(1):
        prono_corr0, prono_corr1 = test_qqcorr(prono, era5_hist, gefs_hist)
        for val, val0, val1 in zip(prono, prono_corr0, prono_corr1):
            print(np.round(val,2), np.round(val0,2), np.round(val1,2))

    end = time.time()
    segundos = np.round(end-start,4)


    print('El ciclo duro:', segundos, ' segundos')

if __name__ == '__main__':
    run()


