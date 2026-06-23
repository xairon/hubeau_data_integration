import numpy as np
from hubeau_pipeline.ml.indices import classify_latest_spli, classify_latest_ssfi, classify_value

# ============================================================================
# CONTRAT DE SYNCHRONISATION CROSS-REPO — NE PAS MODIFIER SANS RÉPLIQUER.
# ----------------------------------------------------------------------------
# Le calcul d'indice IPS/SSFI est dupliqué entre ce repo (ml/indices.py) et le
# repo junon (time-serie-explo/dashboard/utils/drought.py). Cette table golden
# fige le mapping z -> classe (seuils ±0.84/±1.28/±1.75, borne basse inclusive).
# Le MÊME bloc doit exister dans les tests de junon contre drought.classify_value.
# Si l'un des deux dérive, son CI casse -> divergence silencieuse impossible.
# ============================================================================
GOLDEN_Z_TO_CLASS = [
    (-3.00, "EXTREMEMENT_BAS"),
    (-1.76, "EXTREMEMENT_BAS"),
    (-1.75, "TRES_BAS"),          # borne basse inclusive
    (-1.50, "TRES_BAS"),
    (-1.29, "TRES_BAS"),
    (-1.28, "BAS"),               # borne
    (-1.00, "BAS"),
    (-0.85, "BAS"),
    (-0.84, "NORMAL"),            # borne
    (0.00, "NORMAL"),
    (0.83, "NORMAL"),
    (0.84, "HAUT"),               # borne
    (1.00, "HAUT"),
    (1.27, "HAUT"),
    (1.28, "TRES_HAUT"),          # borne
    (1.50, "TRES_HAUT"),
    (1.74, "TRES_HAUT"),
    (1.75, "EXTREMEMENT_HAUT"),   # borne
    (3.00, "EXTREMEMENT_HAUT"),
]


def test_classify_value_golden_contract():
    """Garde-fou anti-divergence cross-repo (cf. bloc ci-dessus)."""
    for z, expected in GOLDEN_Z_TO_CLASS:
        assert classify_value(z) == expected, f"z={z} -> {classify_value(z)} (attendu {expected})"
    assert classify_value(None) == "UNKNOWN"

def test_spli_too_short_is_unknown():
    months = [f"2020-{m:02d}-01" for m in range(1, 13)]  # 12 < 60
    z, cls = classify_latest_spli(months, [1.0] * 12)
    assert z is None and cls == "UNKNOWN"

def test_spli_returns_class_on_long_series():
    months, values = [], []
    for y in range(2010, 2020):
        for m in range(1, 13):
            months.append(f"{y}-{m:02d}-01")
            values.append(10.0 + m + np.random.default_rng(y * 12 + m).normal(0, 0.5))
    months.append("2020-06-01"); values.append(0.0)  # very low June
    z, cls = classify_latest_spli(months, values)
    assert z is not None and z < 0 and cls in ("EXTREMEMENT_BAS", "TRES_BAS", "BAS")

def test_ssfi_returns_class_on_long_series():
    months, values = [], []
    for y in range(2010, 2020):
        for m in range(1, 13):
            months.append(f"{y}-{m:02d}-01")
            values.append(100.0 + m * 5 + abs(np.random.default_rng(y * 12 + m).normal(0, 3)))
    months.append("2020-06-01"); values.append(5.0)  # very low June flow
    z, cls = classify_latest_ssfi(months, values)
    assert z is not None and z < 0
