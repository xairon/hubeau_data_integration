"""Golden tests du calcul SPI/STI grille ERA5 (McKee 1993, gamma méthode des moments)."""
import numpy as np
from scipy import stats

from hubeau_pipeline.ml.era5_indices import (
    MIN_YEARS_REF,
    _fit_glo_detailed,
    _fit_loglogistic_detailed,
    compute_spei,
    compute_spei_glo,
    compute_spi,
    compute_sti,
    fit_glo_lmoments,
    fit_loglogistic_lmoments,
)


def test_spi_median_of_gamma_is_near_zero():
    # Le cumul égal à la médiane de la gamma de référence doit donner SPI ≈ 0
    alpha, beta = 4.0, 25.0  # moyenne 100 mm
    median = stats.gamma.ppf(0.5, alpha, scale=beta)
    spi = compute_spi(np.array([median]), np.array([alpha]), np.array([beta]), np.array([0.0]))
    assert abs(spi[0]) < 1e-6


def test_spi_golden_value_exact():
    # Valeur dorée : cumul 50 mm sous gamma(4, scale=25) → cdf ~0.1429 → z ~ -1.0672
    alpha, beta, x = 4.0, 25.0, 50.0
    expected = round(float(stats.norm.ppf(stats.gamma.cdf(x, alpha, scale=beta))), 3)
    spi = compute_spi(np.array([x]), np.array([alpha]), np.array([beta]), np.array([0.0]))
    assert spi[0] == expected


def test_spi_prob_zero_shifts_distribution():
    # Avec q=0.2, H(x) = 0.2 + 0.8*G(x) : un cumul nul doit donner Φ⁻¹(0.2) (clip inclus)
    spi = compute_spi(np.array([0.0]), np.array([4.0]), np.array([25.0]), np.array([0.2]))
    assert spi[0] == round(float(stats.norm.ppf(0.2)), 3)


def test_spi_invalid_params_gives_nan():
    spi = compute_spi(np.array([100.0, 100.0]),
                      np.array([np.nan, -1.0]),
                      np.array([25.0, 25.0]),
                      np.array([0.0, 0.0]))
    assert np.isnan(spi).all()


def test_spi_extreme_clipped_to_ppf_bounds():
    # CDF clippée à [0.001, 0.999] → SPI borné ≈ ±3.09
    spi = compute_spi(np.array([1e6]), np.array([4.0]), np.array([25.0]), np.array([0.0]))
    assert spi[0] == round(float(stats.norm.ppf(0.999)), 3)


def test_sti_basic_zscore():
    sti = compute_sti(np.array([22.0]), np.array([20.0]), np.array([2.0]))
    assert sti[0] == 1.0


def test_sti_zero_sigma_gives_nan():
    sti = compute_sti(np.array([20.0]), np.array([20.0]), np.array([0.0]))
    assert np.isnan(sti[0])


def test_min_years_ref_constant():
    assert MIN_YEARS_REF == 25


def test_fit_loglogistic_recovers_known_params():
    # Synthetic sample drawn from a known 3-param log-logistic (fisk + loc):
    # x = gamma_loc + alpha * (u/(1-u))**(1/beta), u ~ Uniform(0,1) on a fixed grid.
    alpha, beta, gamma_loc = 40.0, 3.0, -10.0
    u = (np.arange(1, 61) - 0.5) / 60.0            # 60 deterministic quantiles
    x = gamma_loc + alpha * (u / (1.0 - u)) ** (1.0 / beta)
    a, b, g = fit_loglogistic_lmoments(x)
    assert np.isfinite([a, b, g]).all()
    assert abs(a - alpha) < 4.0
    assert abs(b - beta) < 0.4
    assert abs(g - gamma_loc) < 6.0


def test_fit_loglogistic_degenerate_returns_nan():
    assert not np.isfinite(fit_loglogistic_lmoments(np.full(30, 5.0))[1])   # constant
    assert not np.isfinite(fit_loglogistic_lmoments(np.array([1.0, 2.0]))[1])  # n < 4


def test_fit_loglogistic_detailed_n_insuffisant():
    a, b, g, reason = _fit_loglogistic_detailed(np.array([1.0, 2.0]))
    assert reason == "n_insuffisant"
    assert not np.isfinite([a, b, g]).any()


def test_fit_loglogistic_detailed_constant_sample_reason():
    # Échantillon constant : PWM non dégénéré numériquement (arrondi flottant),
    # mais le beta résultant est négatif → hors du domaine requis (beta > 1.0).
    a, b, g, reason = _fit_loglogistic_detailed(np.full(30, 5.0))
    assert reason == "beta_hors_domaine"
    assert not np.isfinite([a, b, g]).any()


def test_fit_loglogistic_detailed_valid_fit_reason_is_none():
    alpha, beta, gamma_loc = 40.0, 3.0, -10.0
    u = (np.arange(1, 61) - 0.5) / 60.0
    x = gamma_loc + alpha * (u / (1.0 - u)) ** (1.0 / beta)
    a, b, g, reason = _fit_loglogistic_detailed(x)
    assert reason is None
    assert np.isfinite([a, b, g]).all()


def test_compute_spei_sign_and_center():
    # Median of the reference (x = gamma_loc + alpha) → F = 0.5 → SPEI ≈ 0.
    alpha, beta, gamma_loc = 40.0, 3.0, -10.0
    median = gamma_loc + alpha
    z = compute_spei(
        np.array([median, median + 300.0, gamma_loc + 1.0]),
        np.full(3, alpha), np.full(3, beta), np.full(3, gamma_loc),
    )
    assert abs(z[0]) < 0.05          # centre
    assert z[1] > 1.0                # wet surplus
    assert z[2] < -1.0               # deep deficit


def test_compute_spei_invalid_params_nan():
    z = compute_spei(
        np.array([10.0, 10.0, -999.0, 10.0, 10.0]),
        np.array([40.0, np.nan, 40.0, -5.0, 40.0]),   # rows 1,3: bad alpha (nan, finite<=0)
        np.array([3.0, 3.0, 3.0, 3.0, -1.0]),         # row 4: bad beta (finite<=0)
        np.array([-10.0, -10.0, 5.0, -10.0, -10.0]),  # row 2: x <= gamma → out of support
    )
    assert np.isfinite(z[0])   # valid row: finite result, not fabricated NaN
    assert np.isnan(z[1])      # alpha = NaN
    assert np.isnan(z[2])      # x <= gamma (out of support)
    assert np.isnan(z[3])      # alpha = -5.0, finite but <= 0 (must not fabricate a value)
    assert np.isnan(z[4])      # beta = -1.0, finite but <= 0 (must not fabricate a value)


# --- Logistique généralisée (GLO) : remplace la log-logistique pour le SPEI ---
# 100% des mailles rejetées par la log-logistique ont une L-asymétrie τ₃ < 0
# (hors du domaine de la log-logistique, asymétrie positive uniquement) ; la GLO
# accepte les deux signes de k = −τ₃.

def _glo_quantile(f, alpha, k, xi):
    # x(F) = ξ + α·(1 − ((1−F)/F)^k)/k — fonction quantile GLO (Hosking).
    return xi + alpha * (1.0 - ((1.0 - f) / f) ** k) / k


def test_fit_glo_recovers_known_params_k_positive():
    alpha, k, xi = 40.0, 0.3, -10.0
    n = 60
    f = (np.arange(1, n + 1) - 0.5) / n
    x = _glo_quantile(f, alpha, k, xi)
    a, kk, x0 = fit_glo_lmoments(x)
    assert np.isfinite([a, kk, x0]).all()
    assert abs(a - alpha) < 2.0
    assert abs(kk - k) < 0.1
    assert abs(x0 - xi) < 3.0


def test_fit_glo_recovers_known_params_k_negative():
    alpha, k, xi = 40.0, -0.3, -10.0
    n = 60
    f = (np.arange(1, n + 1) - 0.5) / n
    x = _glo_quantile(f, alpha, k, xi)
    a, kk, x0 = fit_glo_lmoments(x)
    assert np.isfinite([a, kk, x0]).all()
    assert abs(a - alpha) < 2.0
    assert abs(kk - k) < 0.1
    assert abs(x0 - xi) < 3.0


def test_fit_glo_detailed_n_insuffisant():
    a, k, xi, reason = _fit_glo_detailed(np.array([1.0, 2.0]))
    assert reason == "n_insuffisant"
    assert not np.isfinite([a, k, xi]).any()


def test_fit_glo_detailed_l2_degenere_constant_sample():
    # Échantillon constant négatif : avec la position de tracé (i-0.35)/n, λ₂
    # (= L2 classique, 2·b1_classique − b0) devient négatif pour une constante
    # < 0 (biais de la position de tracé) → dégénéré.
    a, k, xi, reason = _fit_glo_detailed(np.full(30, -5.0))
    assert reason == "l2_degenere"
    assert not np.isfinite([a, k, xi]).any()


def test_fit_glo_detailed_k_hors_domaine():
    # Échantillon très fortement asymétrique (7 valeurs extrêmes basses, 1 haute) :
    # |τ₃| ≈ 1.30 >= 1 → k = −τ₃ hors du domaine (-1, 1), λ₂ restant positif.
    x = np.array([-1e12] * 7 + [0.0])
    a, k, xi, reason = _fit_glo_detailed(x)
    assert reason == "k_hors_domaine"
    assert not np.isfinite([a, k, xi]).any()


def test_fit_glo_detailed_valid_fit_reason_is_none():
    alpha, k, xi = 40.0, 0.3, -10.0
    n = 60
    f = (np.arange(1, n + 1) - 0.5) / n
    x = _glo_quantile(f, alpha, k, xi)
    a, kk, x0, reason = _fit_glo_detailed(x)
    assert reason is None
    assert np.isfinite([a, kk, x0]).all()


def test_compute_spei_glo_sign_and_center():
    # Médiane de la référence (F=0.5 → x=ξ) → SPEI ≈ 0.
    alpha, k, xi = 40.0, 0.3, -10.0
    median = xi
    high = _glo_quantile(np.array([0.95]), alpha, k, xi)[0]
    low = _glo_quantile(np.array([0.05]), alpha, k, xi)[0]
    z = compute_spei_glo(
        np.array([median, high, low]),
        np.full(3, alpha), np.full(3, k), np.full(3, xi),
    )
    assert abs(z[0]) < 0.05   # centre
    assert z[1] > 1.0         # excédent
    assert z[2] < -1.0        # déficit


def test_compute_spei_glo_k_near_zero_logistic_case():
    # k≈0 : loi logistique pure, F(x) = 1/(1+exp(−(x−ξ)/α)).
    alpha, xi = 40.0, -10.0
    z = compute_spei_glo(np.array([xi]), np.array([alpha]), np.array([1e-8]), np.array([xi]))
    assert abs(z[0]) < 1e-6


def test_compute_spei_glo_out_of_support_gives_nan():
    # k > 0 : support borné supérieurement par ξ + α/k ; au-delà, 1 − k(x−ξ)/α <= 0.
    alpha, k, xi = 40.0, 0.3, -10.0
    x_max_support = xi + alpha / k
    z = compute_spei_glo(
        np.array([x_max_support + 100.0]),
        np.array([alpha]), np.array([k]), np.array([xi]),
    )
    assert np.isnan(z[0])


def test_compute_spei_glo_invalid_params_nan():
    z = compute_spei_glo(
        np.array([10.0, 10.0, 10.0, 10.0]),
        np.array([np.nan, -5.0, 40.0, 40.0]),   # rows 0,1: bad alpha (nan, finite<=0)
        np.array([0.3, 0.3, np.nan, 1.5]),      # rows 2,3: bad k (nan, |k|>=1)
        np.array([-10.0, -10.0, -10.0, -10.0]),
    )
    assert np.isnan(z).all()
