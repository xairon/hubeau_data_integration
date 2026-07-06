"""Golden tests du calcul SPI/STI grille ERA5 (McKee 1993, gamma méthode des moments)."""
import numpy as np
from scipy import stats

from hubeau_pipeline.ml.era5_indices import MIN_YEARS_REF, compute_spi, compute_sti


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
