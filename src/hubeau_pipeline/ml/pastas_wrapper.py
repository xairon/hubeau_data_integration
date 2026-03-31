"""Pastas TFN wrapper for IRF feature extraction.

Adapted from time-serie-explo PastasWrapper. Fits a Pastas RechargeModel
(Gamma IRF + FlexModel recharge) and extracts hydrogeological features:
IRF parameters, derived response characteristics, and fit quality metrics.

Reference: Collenteur et al. (2019), Groundwater — Pastas library.
Methodology: BRGM TEMPO (Bichot & Pinault, 2007, BRGM/RP-55348-FR).
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

try:
    import pastas as ps

    PASTAS_AVAILABLE = True
except ImportError:
    PASTAS_AVAILABLE = False


def fit_single_station(
    code_bss: str,
    gwl: pd.Series,
    precip: pd.Series,
    evap: pd.Series,
) -> dict:
    """Fit a Pastas TFN model for one station and extract IRF features.

    Args:
        code_bss: Station identifier.
        gwl: Groundwater level (m NGF), DatetimeIndex.
        precip: Precipitation (mm/d), DatetimeIndex.
        evap: Evapotranspiration (mm/d, positive), DatetimeIndex.

    Returns:
        Dict with code_bss, IRF params, derived features, fit quality, metadata.
        Always returns a dict (fit_success=False on error).
    """
    if not PASTAS_AVAILABLE:
        return _failure_result(code_bss, gwl, "pastas not installed")

    # Metadata
    series_start = gwl.index.min()
    series_end = gwl.index.max()
    series_length_days = (series_end - series_start).days
    nan_fraction = float(gwl.isna().mean())

    base_meta = {
        "code_bss": code_bss,
        "series_start": series_start.date(),
        "series_end": series_end.date(),
        "series_length_days": series_length_days,
        "nan_fraction": nan_fraction,
    }

    try:
        # Ensure daily frequency (Pastas requires regular timestep)
        gwl = gwl.asfreq("D")
        precip = precip.asfreq("D")
        evap = evap.asfreq("D")

        # Suppress Pastas NaN-fill warnings (expected with gappy data)
        ps.logger.setLevel(logging.ERROR)

        # Build and solve model
        model = ps.Model(gwl.dropna(), name="gwl")
        recharge = ps.RechargeModel(
            precip,
            evap,
            rfunc=ps.Gamma(),
            name="recharge",
            recharge=ps.rch.FlexModel(),
        )
        model.add_stressmodel(recharge)
        model.solve(solver=ps.LeastSquares(), report=False)

        # Extract IRF parameters
        params = model.parameters["optimal"]
        irf_params = {
            "recharge_a": _safe_param(params, "recharge_A"),
            "recharge_n": _safe_param(params, "recharge_n"),
            "recharge_scale": _safe_param(params, "recharge_a"),
            # recharge_f: FlexModel doesn't expose a single f param;
            # try Linear recharge's "recharge_f", else None
            "recharge_f": _safe_param(params, "recharge_f"),
        }

        # Derived features from IRF
        derived = _extract_derived_features(model, params)

        # Fit quality metrics
        quality = _extract_fit_quality(model)

        return {
            **base_meta,
            **irf_params,
            **derived,
            **quality,
            "fit_success": True,
            "pastas_version": ps.__version__,
        }

    except Exception as e:
        return {
            **base_meta,
            **_null_features(),
            "fit_success": False,
            "pastas_version": ps.__version__ if PASTAS_AVAILABLE else None,
            "error": str(e),
        }


def _safe_param(params: pd.Series, key: str) -> float | None:
    """Safely extract a parameter, returning None if missing."""
    try:
        return float(params[key])
    except (KeyError, TypeError):
        return None


def _extract_derived_features(model, params: pd.Series) -> dict:
    """Extract derived IRF features from a fitted Pastas model."""
    result = {
        "tmax_days": None,
        "cutoff_95_days": None,
        "gain": None,
        "mean_response_time": None,
    }

    try:
        # tmax: time to peak of impulse response
        n = _safe_param(params, "recharge_n")
        a = _safe_param(params, "recharge_a")
        if n is not None and a is not None:
            result["tmax_days"] = float(a * (n - 1)) if n > 1 else 0.0
    except Exception:
        pass

    try:
        # Block response (impulse response) for cutoff and gain
        # Pastas 1.10+: use get_block_response (not get_response)
        response = model.get_block_response("recharge")
        if response is not None and len(response) > 0:
            dt = 1.0  # daily timestep
            response_values = response.values.flatten()
            cumsum = np.cumsum(np.abs(response_values)) * dt
            total = cumsum[-1]

            # gain: integral of the impulse response
            if total > 0:
                result["gain"] = float(total)

                # cutoff_95: time at which 95% of the response has passed
                idx_95 = np.searchsorted(cumsum, 0.95 * total)
                result["cutoff_95_days"] = float(idx_95) * dt

                # mean response time: barycenter of the IRF
                t = np.arange(len(response_values)) * dt
                weights = np.abs(response_values)
                result["mean_response_time"] = float(np.average(t, weights=weights))
    except Exception:
        pass

    try:
        # response_tmax from Pastas (more accurate than analytical formula)
        tmax_pastas = model.get_response_tmax("recharge")
        if tmax_pastas is not None:
            result["tmax_days"] = float(tmax_pastas)
    except Exception:
        pass

    return result


def _extract_fit_quality(model) -> dict:
    """Extract calibration quality metrics from a fitted Pastas model."""
    result = {
        "evp": None,
        "rmse": None,
        "nash": None,
        "r2": None,
        "n_observations": None,
    }

    try:
        obs = model.observations()
        sim = model.simulate()

        # Align obs and sim
        common_idx = obs.index.intersection(sim.index)
        obs_aligned = obs.loc[common_idx].values.flatten()
        sim_aligned = sim.loc[common_idx].values.flatten()
        res_aligned = obs_aligned - sim_aligned

        n = len(obs_aligned)
        result["n_observations"] = n

        if n > 0:
            ss_res = np.sum(res_aligned ** 2)
            ss_tot = np.sum((obs_aligned - np.mean(obs_aligned)) ** 2)

            result["rmse"] = float(np.sqrt(ss_res / n))

            if ss_tot > 0:
                r2 = 1.0 - ss_res / ss_tot
                result["nash"] = float(r2)  # Nash-Sutcliffe = R2 for this formulation
                result["r2"] = float(r2)
                result["evp"] = float(r2 * 100)  # Explained variance percentage

    except Exception:
        pass

    return result


def _null_features() -> dict:
    """Return a dict with all feature keys set to None."""
    return {
        "recharge_a": None,
        "recharge_n": None,
        "recharge_scale": None,
        "recharge_f": None,
        "tmax_days": None,
        "cutoff_95_days": None,
        "gain": None,
        "mean_response_time": None,
        "evp": None,
        "rmse": None,
        "nash": None,
        "r2": None,
        "n_observations": None,
        "pastas_version": None,
    }


def _failure_result(code_bss: str, gwl: pd.Series, error: str) -> dict:
    """Build a failure result with metadata from the GWL series."""
    series_start = gwl.index.min() if len(gwl) > 0 else None
    series_end = gwl.index.max() if len(gwl) > 0 else None
    series_length_days = (series_end - series_start).days if series_start and series_end else 0
    nan_fraction = float(gwl.isna().mean()) if len(gwl) > 0 else 1.0

    return {
        "code_bss": code_bss,
        "series_start": series_start.date() if series_start else None,
        "series_end": series_end.date() if series_end else None,
        "series_length_days": series_length_days,
        "nan_fraction": nan_fraction,
        **_null_features(),
        "fit_success": False,
        "error": error,
    }


# ---------------------------------------------------------------------------
# Full re-fit: decomposition + water balance + enriched metrics
# ---------------------------------------------------------------------------


def _diag_pvalue(diag_df: pd.DataFrame, test_name: str) -> float | None:
    """Extract p-value for a named test from model.stats.diagnostics() DataFrame.

    The diagnostics DataFrame is indexed by test name (e.g. 'Shapiroo',
    "D'Agostino", 'Runs test', 'Ljung-Box', 'Durbin-Watson').
    """
    try:
        if test_name not in diag_df.index:
            return None
        val = diag_df.loc[test_name, "P-value"]
        return float(val) if pd.notna(val) else None
    except Exception:
        return None


def _diag_stat(diag_df: pd.DataFrame, test_name: str) -> float | None:
    """Extract statistic value for a named test from diagnostics DataFrame.

    The diagnostics DataFrame is indexed by test name.
    """
    try:
        if test_name not in diag_df.index:
            return None
        val = diag_df.loc[test_name, "Statistic"]
        return float(val) if pd.notna(val) else None
    except Exception:
        return None


def fit_and_extract_all(
    code_bss: str,
    gwl: pd.Series,
    precip: pd.Series,
    evap: pd.Series,
) -> dict:
    """Fit Pastas model once and extract everything: metrics, diagnostics,
    block response, decomposition time series, and water balance.

    Returns:
        {
            "code_bss": str,
            "scalars": dict (kge, mae, aic, bic, pearsonr, diagnostics, block_response),
            "timeseries": pd.DataFrame (simulated, residuals, recharge_contribution, wb_*),
            "fit_success": bool,
            "error": str | None,
        }
    """
    if not PASTAS_AVAILABLE:
        return {"code_bss": code_bss, "scalars": {}, "timeseries": pd.DataFrame(),
                "fit_success": False, "error": "pastas not installed"}

    if len(gwl) == 0 or gwl.dropna().empty:
        return {"code_bss": code_bss, "scalars": {}, "timeseries": pd.DataFrame(),
                "fit_success": False, "error": "empty or all-NaN GWL series"}

    try:
        gwl = gwl.asfreq("D")
        precip = precip.asfreq("D")
        evap = evap.asfreq("D")

        ps.logger.setLevel(logging.ERROR)

        # 1. Build and solve model
        model = ps.Model(gwl.dropna(), name="gwl")
        recharge = ps.RechargeModel(
            precip, evap, rfunc=ps.Gamma(),
            name="recharge", recharge=ps.rch.FlexModel(),
        )
        model.add_stressmodel(recharge)
        model.solve(solver=ps.LeastSquares(), report=False)

        # 2. Enriched fit metrics
        scalars = {}
        for metric in ["kge", "mae", "aic", "bic"]:
            try:
                scalars[metric] = float(getattr(model.stats, metric)())
            except Exception:
                scalars[metric] = None
        try:
            scalars["pearsonr"] = float(model.stats.pearsonr())
        except Exception:
            scalars["pearsonr"] = None

        # 3. Diagnostics
        try:
            diag = model.stats.diagnostics()
            # Pastas index names: 'Shapiroo' (sic), "D'Agostino", 'Runs test',
            # 'Ljung-Box', 'Durbin-Watson'
            scalars["shapiro_pvalue"] = _diag_pvalue(diag, "Shapiroo")
            scalars["dagostino_pvalue"] = _diag_pvalue(diag, "D'Agostino")
            scalars["runs_test_pvalue"] = _diag_pvalue(diag, "Runs test")
            scalars["ljung_box_pvalue"] = _diag_pvalue(diag, "Ljung-Box")
            scalars["durbin_watson_stat"] = _diag_stat(diag, "Durbin-Watson")
        except Exception:
            for k in ["shapiro_pvalue", "dagostino_pvalue", "runs_test_pvalue",
                       "ljung_box_pvalue", "durbin_watson_stat"]:
                scalars[k] = None

        # 4. Block response
        try:
            br = model.get_block_response("recharge")
            if br is not None and len(br) > 0:
                scalars["block_response"] = br.values.flatten().tolist()
                scalars["block_response_length"] = len(br)
            else:
                scalars["block_response"] = None
                scalars["block_response_length"] = 0
        except Exception:
            scalars["block_response"] = None
            scalars["block_response_length"] = 0

        # 5. Decomposition time series
        sim = model.simulate()
        res = model.residuals()
        contrib = model.get_contribution("recharge")

        ts = pd.DataFrame({
            "simulated": sim,
            "residuals": res,
            "recharge_contribution": contrib,
        })

        # 6. Water balance (FlexModel default: 5 components)
        try:
            rm = model.stressmodels["recharge"]
            wb = rm.get_water_balance(model.get_parameters("recharge"))
            for col in wb.columns:
                safe_name = "wb_" + col.lower().replace(" ", "_").replace("(", "").replace(")", "")
                ts[safe_name] = wb[col]
        except Exception as e:
            logger.warning(f"{code_bss}: water balance extraction failed: {e}")

        return {
            "code_bss": code_bss,
            "scalars": scalars,
            "timeseries": ts,
            "fit_success": True,
            "error": None,
        }

    except Exception as e:
        return {
            "code_bss": code_bss,
            "scalars": {},
            "timeseries": pd.DataFrame(),
            "fit_success": False,
            "error": str(e),
        }
