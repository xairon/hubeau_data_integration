"""SPI/STI grille ERA5 (McKee 1993). Vectorisé numpy/scipy.

SPI : cumul de précipitations → CDF gamma (méthode des moments, paramètres précalculés
dans gold.fct_era5_climatology_grid) mélangée avec la probabilité de cumul nul
(H(x) = q + (1-q)·G(x)) → quantile normal. STI : z-score de la température moyenne
de fenêtre contre la normale 1991-2020. Mêmes seuils de classes que l'IPS (indices.py).
"""
from __future__ import annotations

import numpy as np
from scipy import stats

# Seuil WMO : nombre minimal d'années valides dans la référence pour un indice fiable.
MIN_YEARS_REF = 25

_CDF_CLIP = (0.001, 0.999)


def compute_spi(cumul, gamma_alpha, gamma_beta, prob_zero):
    """SPI vectorisé. NaN si alpha/beta invalides (<=0 ou NaN).

    Args: arrays alignés — cumul (mm), gamma_alpha, gamma_beta (scale), prob_zero [0,1].
    Returns: np.ndarray float64, arrondi à 3 décimales, NaN si non calculable.
    """
    cumul = np.asarray(cumul, dtype=float)
    alpha = np.asarray(gamma_alpha, dtype=float)
    beta = np.asarray(gamma_beta, dtype=float)
    q = np.nan_to_num(np.asarray(prob_zero, dtype=float), nan=0.0)

    valid = np.isfinite(cumul) & np.isfinite(alpha) & np.isfinite(beta) & (alpha > 0) & (beta > 0)
    out = np.full(cumul.shape, np.nan)
    if not valid.any():
        return out

    g = stats.gamma.cdf(np.clip(cumul[valid], 0, None), alpha[valid], scale=beta[valid])
    h = q[valid] + (1.0 - q[valid]) * g
    h = np.clip(h, *_CDF_CLIP)
    out[valid] = np.round(stats.norm.ppf(h), 3)
    return out


def compute_sti(temp, temp_moyenne, temp_stddev):
    """STI vectorisé : (t − μ)/σ. NaN si σ <= 0 ou entrées non finies."""
    t = np.asarray(temp, dtype=float)
    mu = np.asarray(temp_moyenne, dtype=float)
    sigma = np.asarray(temp_stddev, dtype=float)

    valid = np.isfinite(t) & np.isfinite(mu) & np.isfinite(sigma) & (sigma > 0)
    out = np.full(t.shape, np.nan)
    out[valid] = np.round((t[valid] - mu[valid]) / sigma[valid], 3)
    return out
