import numpy as np
import pandas as pd

from hubeau_pipeline.assets.era5_spei_climatology_assets import fit_reference_frame


def test_fit_reference_frame_groups_and_fits():
    alpha, beta, gamma_loc = 40.0, 3.0, -10.0
    u = (np.arange(1, 61) - 0.5) / 60.0
    samples = gamma_loc + alpha * (u / (1.0 - u)) ** (1.0 / beta)
    df = pd.DataFrame({
        "era5_latitude": [48.1] * 60 + [43.5] * 3,     # 2nd cell: too few → dropped
        "era5_longitude": [2.3] * 60 + [5.0] * 3,
        "mois_calendaire": [6] * 60 + [6] * 3,
        "bilan_cumul": list(samples) + [1.0, 2.0, 3.0],
    })
    rows, stats = fit_reference_frame(df, window=3)
    assert len(rows) == 1                        # degenerate cell dropped
    lat, lon, mc, fen, a, b, g, n = rows[0]
    assert (lat, lon, mc, fen, n) == (48.1, 2.3, 6, 3, 60)
    assert abs(b - beta) < 0.4

    assert stats["groupes"] == 2                 # les deux groupes, rejeté inclus
    assert stats["ok"] == len(rows) == 1
    assert stats["n_annees_insuffisant"] == 1     # le groupe à 3 échantillons (< MIN_YEARS_REF)
    for reason in ("pwm_degenere", "beta_hors_domaine", "alpha_invalide", "n_insuffisant"):
        assert stats[reason] == 0
