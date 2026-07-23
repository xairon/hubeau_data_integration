"""Golden tests du calcul SPI/STI grille ERA5 (McKee 1993, gamma méthode des moments)."""
import numpy as np
from scipy import stats

from hubeau_pipeline.ml.era5_indices import (
    MIN_YEARS_REF,
    compute_spi,
    compute_spei,
    compute_sti,
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
