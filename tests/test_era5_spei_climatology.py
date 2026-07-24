import numpy as np
import pandas as pd

from hubeau_pipeline.assets.era5_spei_climatology_assets import fit_reference_frame


def test_fit_reference_frame_groups_and_fits():
    # GLO quantile function x(F) = ξ + α·(1 − ((1−F)/F)^k)/k (Hosking).
    alpha, k, xi = 40.0, 0.3, -10.0
    f = (np.arange(1, 61) - 0.5) / 60.0
    samples = xi + alpha * (1.0 - ((1.0 - f) / f) ** k) / k
    df = pd.DataFrame({
        "era5_latitude": [48.1] * 60 + [43.5] * 3,     # 2nd cell: too few → dropped
        "era5_longitude": [2.3] * 60 + [5.0] * 3,
        "mois_calendaire": [6] * 60 + [6] * 3,
        "bilan_cumul": list(samples) + [1.0, 2.0, 3.0],
    })
    rows, stats = fit_reference_frame(df, window=3)
    assert len(rows) == 1                        # degenerate cell dropped
    lat, lon, mc, fen, a, kk, xi0, n = rows[0]
    assert (lat, lon, mc, fen, n) == (48.1, 2.3, 6, 3, 60)
    assert abs(kk - k) < 0.1

    assert stats["groupes"] == 2                 # les deux groupes, rejeté inclus
    assert stats["ok"] == len(rows) == 1
    assert stats["n_annees_insuffisant"] == 1     # le groupe à 3 échantillons (< MIN_YEARS_REF)
    for reason in ("l2_degenere", "k_hors_domaine", "alpha_invalide", "n_insuffisant"):
        assert stats[reason] == 0
