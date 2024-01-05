import numpy as np
import numpy.ma as ma
import pandas as pd
import datetime as dt
import xarray as xr
from numba import jit
from scipy.stats import gamma


@jit(nopython=True)
def ecdf_numpy_val(x, n, v):
    return (np.searchsorted(x, v, side='right')) / n


@jit(nopython=True)
def qq_corr(val, era_hist, gefs_hist):
    if np.isnan(val):
        corr_o = np.nan
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
        prono_corr[index[0], index[1]] = qq_corr(val, ehist, ghist)
    return prono_corr

#####################################
#### Functions for precipitation

def rain_days(ppdata, ppmin):
    ind_pp = np.array([e > ppmin if ~np.isnan(e) else False for e in ppdata], dtype=np.bool_)
    return ppdata[ind_pp]


def fit_gamma_param(eh, gh, xm_min):
    # Days with precipitacion
    xo_min = 0.2
    ppo_data = rain_days(eh, xo_min)
    ppm_data = rain_days(gh, xm_min)
    validos_gfs = np.round(len(ppm_data)/len(gh),2)
    validos_era = np.round(len(ppo_data)/len(eh),2)
    if (len(ppm_data) < 50) | (len(ppo_data) < 50):
        #print('----')
        #print('Pocos datos para estimar gamma')
        #print(len(ppm_data))
        #print(len(ppo_data))
        #print(validos_gfs)
        #print(validos_era)
        obs_gamma_param = (np.nan, np.nan, np.nan)
        mod_gamma_param = (np.nan, np.nan, np.nan)
    else:
        # Fit a Gamma distribution over days with precipitation
        obs_gamma_param = gamma.fit(ppo_data, floc=0)
        mod_gamma_param = gamma.fit(ppm_data, floc=0)
        
    '''
    if np.isnan(ppo_data).all():
        print('Obs data all nan')
        print(ppo_data)
    if np.isnan(ppm_data).all():
        print('Model data all nan')
        print(xm_min)
        print(ppm_data)
        print(gh)
        print(eh)
        exit()
    '''

    return obs_gamma_param, mod_gamma_param


def qq_corr_rain(val, era_hist, gefs_hist, xm_min, tipo_ajuste='GG'):
    if np.isnan(val):
        corr_o = np.nan
        mask_o = -1
    elif val < xm_min:
        corr_o = 0.
        mask_o = 0
    else:
        cdf_limit = 0.9999999
        if tipo_ajuste == 'GG':
            # Ajustamos una gamma a los valores con precipitacion y corregimos
            obs_gamma, mod_gamma = fit_gamma_param(era_hist, gefs_hist, xm_min)
            if np.isnan(obs_gamma[0]):
                #print('No ajuste una gamma')
                corr_o = val
                mask_o = 1
            else:
                p1 = gamma.cdf(val, *mod_gamma)
                if p1 > cdf_limit:
                    p1 = cdf_limit
                corr_o = gamma.ppf(p1, *obs_gamma)
                mask_o = 2
        elif tipo_ajuste == 'Mult-Shift':
            xo_min = 0.2
            mod_precdias = rain_days(gefs_hist, xm_min)
            obs_precdias = rain_days(era_hist, xo_min)
            xm_mean = np.nanmean(mod_precdias)
            xo_mean = np.nanmean(obs_precdias)
            corr_factor = xo_mean/xm_mean
            corr_o = val*corr_factor
            mask_o = 1
    return corr_o, mask_o


def cycle_matrix_rain(prono, gefs, era5, minimo_pp):
    prono_corr = prono.copy()
    mask_corr = np.empty(prono.shape)
    for index in np.ndindex(prono.shape[0],prono.shape[1]):
        val = prono[index[0], index[1]]
        xm_min = minimo_pp[index[0], index[1]]
        if np.isnan(xm_min):
            xm_min = 0.
        ghist = gefs[:, index[0], index[1]]
        ehist = era5[:, index[0], index[1]]
        corregido, mascara = qq_corr_rain(val, ehist, ghist, xm_min)
        prono_corr[index[0], index[1]] = corregido
        mask_corr[index[0], index[1]] = mascara
    return prono_corr, mask_corr
