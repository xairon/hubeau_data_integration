"""SPI/STI grille ERA5 (McKee 1993). Vectorisé numpy/scipy.

SPI : cumul de précipitations → CDF gamma (méthode des moments, paramètres précalculés
dans gold.fct_era5_climatology_grid) mélangée avec la probabilité de cumul nul
(H(x) = q + (1-q)·G(x)) → quantile normal. STI : z-score de la température moyenne
de fenêtre contre la normale 1991-2020. Mêmes seuils de classes que l'IPS (indices.py).

SPEI : ajustement du cumul bilan hydrique (P-ETP) par une loi logistique généralisée
(GLO, Hosking) via L-moments — remplace l'ancienne log-logistique (asymétrie positive
uniquement) car ~27% des mailles ERA5 ont une L-asymétrie τ₃ négative, hors du domaine
de la log-logistique. La GLO accepte les deux signes ; c'est la loi utilisée par
l'implémentation de référence (R `SPEI::parglo`).
"""
from __future__ import annotations

import numpy as np
from scipy import stats
from scipy.special import gamma as _gamma_fn

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


# Fit fiable seulement au-delà d'un petit échantillon (L-moments d'ordre 2).
_MIN_FIT_SAMPLES = 4


# Motifs de rejet possibles en 4e position de _fit_loglogistic_detailed.
# "n_insuffisant" : n < _MIN_FIT_SAMPLES.
# "pwm_degenere" : dénominateur PWM nul ou non fini.
# "beta_hors_domaine" : beta non fini ou beta <= 1.0.
# "alpha_invalide" : alpha non fini ou alpha <= 0.


def _fit_loglogistic_detailed(samples):
    """Comme fit_loglogistic_lmoments, mais expose en 4e position le motif de
    rejet (None si succès). Logique de calcul strictement inchangée — cette
    fonction ne fait qu'ajouter de l'observabilité, pas de nouvelles règles.
    """
    x = np.asarray(samples, dtype=float)
    x = np.sort(x[np.isfinite(x)])
    n = x.size
    if n < _MIN_FIT_SAMPLES:
        return (np.nan, np.nan, np.nan, "n_insuffisant")

    # PWM en position de tracé p_i = (i − 0.35)/n (convention SPEI de référence).
    i = np.arange(1, n + 1)
    p = (i - 0.35) / n
    w0 = x.mean()
    w1 = np.sum((1.0 - p) * x) / n
    w2 = np.sum((1.0 - p) ** 2 * x) / n

    denom = 6.0 * w1 - w0 - 6.0 * w2
    if denom == 0 or not np.isfinite(denom):
        return (np.nan, np.nan, np.nan, "pwm_degenere")
    beta = (2.0 * w1 - w0) / denom
    # beta>0 requis ; 1/beta<1 requis pour que Γ(1−1/beta) converge (beta>1).
    if not np.isfinite(beta) or beta <= 1.0:
        return (np.nan, np.nan, np.nan, "beta_hors_domaine")

    g = _gamma_fn(1.0 + 1.0 / beta) * _gamma_fn(1.0 - 1.0 / beta)
    alpha = (w0 - 2.0 * w1) * beta / g
    if not np.isfinite(alpha) or alpha <= 0:
        return (np.nan, np.nan, np.nan, "alpha_invalide")
    gamma_loc = w0 - alpha * g
    return (float(alpha), float(beta), float(gamma_loc), None)


def fit_loglogistic_lmoments(samples):
    """Ajuste une log-logistique à 3 paramètres (loi de Fisk translatée) par
    L-moments (PWM en position de tracé, Vicente-Serrano 2010).

    Args: samples — échantillon 1D des cumuls D=P−ETP de référence (une cellule×
        mois calendaire×fenêtre, ~30 valeurs annuelles).
    Returns: (alpha, beta, gamma) ; (nan, nan, nan) si l'ajustement est dégénéré.
    """
    alpha, beta, gamma_loc, _reason = _fit_loglogistic_detailed(samples)
    return (alpha, beta, gamma_loc)


def compute_spei(d_cumul, ll_alpha, ll_beta, ll_gamma):
    """SPEI vectorisé : F log-logistique du cumul D → quantile normal.

    NaN si un paramètre est invalide (alpha≤0, beta≤0, non fini) ou si D≤gamma
    (hors du support de la loi).
    """
    x = np.asarray(d_cumul, dtype=float)
    a = np.asarray(ll_alpha, dtype=float)
    b = np.asarray(ll_beta, dtype=float)
    gloc = np.asarray(ll_gamma, dtype=float)

    valid = (
        np.isfinite(x) & np.isfinite(a) & np.isfinite(b) & np.isfinite(gloc)
        & (a > 0) & (b > 0) & (x > gloc)
    )
    out = np.full(x.shape, np.nan)
    if not valid.any():
        return out

    ratio = (a[valid] / (x[valid] - gloc[valid])) ** b[valid]
    cdf = 1.0 / (1.0 + ratio)
    cdf = np.clip(cdf, *_CDF_CLIP)
    out[valid] = np.round(stats.norm.ppf(cdf), 3)
    return out


# Motifs de rejet possibles en 4e position de _fit_glo_detailed.
# "n_insuffisant" : n < _MIN_FIT_SAMPLES.
# "l2_degenere" : λ₂ (w0 − 2·w1) <= 0 ou non fini.
# "k_hors_domaine" : k = −τ₃ non fini ou |k| >= 1 (Γ(1±k) diverge sinon).
# "alpha_invalide" : alpha non fini ou alpha <= 0.

# En-deçà de ce seuil, k est traité comme nul (cas limite logistique) pour éviter
# la division par k et l'instabilité numérique de π/sin(kπ) au voisinage de 0.
_GLO_K_ZERO_TOL = 1e-6


def _fit_glo_detailed(samples):
    """Ajuste une logistique généralisée (GLO, Hosking) par L-moments et expose
    en 4e position le motif de rejet (None si succès).

    k = −τ₃ ; α = λ₂ / (Γ(1+k)·Γ(1−k)) ; ξ = λ₁ − α·(1/k − π/sin(kπ)).
    Cas limite k≈0 : loi logistique, ξ = λ₁ (le terme correctif tend vers 0).
    """
    x = np.asarray(samples, dtype=float)
    x = np.sort(x[np.isfinite(x)])
    n = x.size
    if n < _MIN_FIT_SAMPLES:
        return (np.nan, np.nan, np.nan, "n_insuffisant")

    # PWM en position de tracé p_i = (i − 0.35)/n (même convention que la log-logistique).
    i = np.arange(1, n + 1)
    p = (i - 0.35) / n
    w0 = x.mean()
    w1 = np.sum((1.0 - p) * x) / n
    w2 = np.sum((1.0 - p) ** 2 * x) / n

    lam1 = w0
    lam2 = w0 - 2.0 * w1
    if not np.isfinite(lam2) or lam2 <= 0:
        return (np.nan, np.nan, np.nan, "l2_degenere")
    lam3 = w0 - 6.0 * w1 + 6.0 * w2
    tau3 = lam3 / lam2

    k = -tau3
    if not np.isfinite(k) or abs(k) >= 1.0:
        return (np.nan, np.nan, np.nan, "k_hors_domaine")

    if abs(k) < _GLO_K_ZERO_TOL:
        alpha = lam2
        xi = lam1
    else:
        alpha = lam2 / (_gamma_fn(1.0 + k) * _gamma_fn(1.0 - k))
        xi = lam1 - alpha * (1.0 / k - np.pi / np.sin(k * np.pi))
    if not np.isfinite(alpha) or alpha <= 0:
        return (np.nan, np.nan, np.nan, "alpha_invalide")
    return (float(alpha), float(k), float(xi), None)


def fit_glo_lmoments(samples):
    """Ajuste une logistique généralisée (GLO) par L-moments.

    Args: samples — échantillon 1D des cumuls D=P−ETP de référence (une cellule×
        mois calendaire×fenêtre, ~30 valeurs annuelles).
    Returns: (alpha, k, xi) ; (nan, nan, nan) si l'ajustement est dégénéré.
    """
    alpha, k, xi, _reason = _fit_glo_detailed(samples)
    return (alpha, k, xi)


def compute_spei_glo(d_cumul, glo_alpha, glo_k, glo_xi):
    """SPEI vectorisé : F logistique généralisée (GLO) du cumul D → quantile normal.

    NaN si un paramètre est invalide (alpha non fini/<=0, k non fini/|k|>=1, xi non
    fini) ou si 1 − k(x−ξ)/α <= 0 (hors du support de la loi). Cas limite k≈0 :
    loi logistique, F(x) = 1/(1+exp(−(x−ξ)/α)).
    """
    x = np.asarray(d_cumul, dtype=float)
    a = np.asarray(glo_alpha, dtype=float)
    k = np.asarray(glo_k, dtype=float)
    xi = np.asarray(glo_xi, dtype=float)
    x, a, k, xi = np.broadcast_arrays(x, a, k, xi)

    out = np.full(x.shape, np.nan)
    params_ok = np.isfinite(a) & np.isfinite(k) & np.isfinite(xi) & (a > 0) & (np.abs(k) < 1.0)
    if not params_ok.any():
        return out

    cdf = np.full(x.shape, np.nan)

    near_zero = params_ok & (np.abs(k) < _GLO_K_ZERO_TOL)
    if near_zero.any():
        cdf[near_zero] = 1.0 / (1.0 + np.exp(-(x[near_zero] - xi[near_zero]) / a[near_zero]))

    general = params_ok & ~near_zero
    if general.any():
        base = 1.0 - k[general] * (x[general] - xi[general]) / a[general]
        cdf_general = np.full(base.shape, np.nan)
        in_support = base > 0
        cdf_general[in_support] = 1.0 / (1.0 + base[in_support] ** (1.0 / k[general][in_support]))
        cdf[general] = cdf_general

    valid = np.isfinite(cdf)
    if not valid.any():
        return out
    cdf_valid = np.clip(cdf[valid], *_CDF_CLIP)
    out[valid] = np.round(stats.norm.ppf(cdf_valid), 3)
    return out
