import numpy as np
import numpy.ma as ma
import pandas as pd
import datetime as dt
import xarray as xr
from numba import jit

from statsmodels.distributions.empirical_distribution import ECDF

@jit(nopython=True)
def ecdf_numpy_val(x, n, v):
    return (np.searchsorted(x, v, side='right')) / n

@jit(nopython=True)
def qq_corr(val, era_hist, gefs_hist):
    if np.isnan(val):
        return np.nan
    else:
        cdf_limit = 0.9999999
        df_h = era_hist[np.logical_not(np.isnan(era_hist))]
        df_m = gefs_hist[np.logical_not(np.isnan(gefs_hist))]
        df_m_sort, df_m_n = np.sort(df_m), len(df_m)
        p1 = ecdf_numpy_val(df_m_sort, df_m_n, val)
        if p1 > cdf_limit:
            p1 = cdf_limit
        corr_o = np.quantile(df_h, p1)
        return corr_o

@jit(nopython=True)
def cycle_matrix(prono, gefs, era5):
    prono_corr = prono.copy()
    for index in np.ndindex(prono.shape[0],prono.shape[1]):
        val = prono[index[0], index[1]]
        ghist = gefs[:, index[0], index[1]]
        ehist = era5[:, index[0], index[1]]
        prono_corr[index[0], index[1]] = qq_corr(val, ghist, ehist)
    return prono_corr

