{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_bss']},
      {'columns': ['nash']},
      {'columns': ['tmax_days']}
    ],
    post_hook = [
      "{{ add_primary_key(['code_bss']) }}"
    ]
  )
}}

-- ============================================================================
-- int_pastas_station_profile
--
-- Profil Pastas complet par station piézométrique : paramètres IRF, qualité du
-- modèle, diagnostics résidus, statistiques résidus (global + récent + saisonnier),
-- bilan hydrique moyen, signatures hydrogéo, SGI récent.
--
-- Une seule ligne par station. Base pour dashboards, embeddings et ML.
-- ============================================================================

WITH irf AS (
    SELECT * FROM {{ source('ml', 'pastas_irf_features') }}
    WHERE fit_success = true
),

-- ── Statistiques résidus (global + récent + saisonnier) ──────────────────────
residual_stats AS (
    SELECT
        code_bss,

        -- Global (toute la série)
        AVG(residuals)                          AS residual_mean,
        STDDEV(residuals)                       AS residual_stddev,
        PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY residuals) AS residual_p05,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY residuals) AS residual_p95,
        COUNT(*) FILTER (WHERE residuals IS NOT NULL) AS residual_n_obs,

        -- Tendance globale (pente des résidus en m/an)
        REGR_SLOPE(residuals, EXTRACT(EPOCH FROM date) / 86400.0) * 365.25
            AS residual_trend_m_per_year,

        -- 12 derniers mois
        AVG(residuals) FILTER (
            WHERE date >= CURRENT_DATE - INTERVAL '365 days'
        ) AS residual_mean_last_12m,
        STDDEV(residuals) FILTER (
            WHERE date >= CURRENT_DATE - INTERVAL '365 days'
        ) AS residual_stddev_last_12m,
        REGR_SLOPE(residuals, EXTRACT(EPOCH FROM date) / 86400.0) FILTER (
            WHERE date >= CURRENT_DATE - INTERVAL '365 days'
        ) * 365.25 AS residual_trend_last_12m,

        -- 5 dernières années
        REGR_SLOPE(residuals, EXTRACT(EPOCH FROM date) / 86400.0) FILTER (
            WHERE date >= CURRENT_DATE - INTERVAL '5 years'
        ) * 365.25 AS residual_trend_last_5y,

        -- Saisonnalité des résidus
        AVG(residuals) FILTER (
            WHERE EXTRACT(MONTH FROM date) IN (6, 7, 8)
        ) AS residual_summer_bias,
        AVG(residuals) FILTER (
            WHERE EXTRACT(MONTH FROM date) IN (12, 1, 2)
        ) AS residual_winter_bias,

        -- Fraction de jours négatifs (dernière année)
        COUNT(*) FILTER (
            WHERE residuals < 0 AND date >= CURRENT_DATE - INTERVAL '365 days'
        )::DOUBLE PRECISION / NULLIF(COUNT(*) FILTER (
            WHERE residuals IS NOT NULL AND date >= CURRENT_DATE - INTERVAL '365 days'
        ), 0) AS frac_negative_last_12m

    FROM {{ source('ml', 'pastas_model_timeseries') }}
    WHERE residuals IS NOT NULL
    GROUP BY code_bss
),

-- ── Bilan hydrique moyen annuel ──────────────────────────────────────────────
water_balance AS (
    SELECT
        code_bss,
        AVG(wb_recharge)            AS wb_recharge_mean,
        AVG(wb_actual_evaporation)  AS wb_etp_reelle_mean,
        AVG(wb_surface_runoff)      AS wb_ruissellement_mean,
        AVG(wb_effective_precip)    AS wb_precip_efficace_mean,
        AVG(wb_root_zone_storage)   AS wb_stockage_sol_mean,

        -- Saisonnalité de la recharge
        AVG(wb_recharge) FILTER (WHERE EXTRACT(MONTH FROM date) IN (11, 12, 1, 2, 3))
            AS wb_recharge_hiver_mean,
        AVG(wb_recharge) FILTER (WHERE EXTRACT(MONTH FROM date) IN (5, 6, 7, 8, 9))
            AS wb_recharge_ete_mean

    FROM {{ source('ml', 'pastas_model_timeseries') }}
    WHERE wb_recharge IS NOT NULL
    GROUP BY code_bss
),

-- ── Signatures (37 métriques, 1 row/station) ────────────────────────────────
signatures AS (
    SELECT * FROM {{ source('ml', 'pastas_groundwater_signatures') }}
),

-- ── SGI récent ───────────────────────────────────────────────────────────────
sgi_recent AS (
    SELECT DISTINCT ON (code_bss)
        code_bss,
        date    AS sgi_last_date,
        sgi     AS sgi_last_value
    FROM {{ source('ml', 'pastas_sgi') }}
    ORDER BY code_bss, date DESC
),

sgi_stats AS (
    SELECT
        code_bss,
        AVG(sgi) AS sgi_mean_last_12m,
        MIN(sgi) AS sgi_min_last_12m,
        COUNT(*) FILTER (WHERE sgi < -1) AS sgi_months_below_minus1
    FROM {{ source('ml', 'pastas_sgi') }}
    WHERE date >= CURRENT_DATE - INTERVAL '365 days'
    GROUP BY code_bss
)

-- ── Assemblage final ─────────────────────────────────────────────────────────
SELECT
    -- ▸ Identifiant
    irf.code_bss,

    -- ▸ IRF Gamma
    irf.recharge_a,
    irf.recharge_n,
    irf.tmax_days,
    irf.cutoff_95_days,
    irf.gain,
    irf.mean_response_time,
    irf.block_response_length,

    -- ▸ Qualité du modèle
    irf.nash,
    irf.kge,
    irf.rmse,
    irf.mae,
    irf.evp,
    irf.r2,
    irf.aic,
    irf.bic,
    irf.pearsonr,

    -- ▸ Diagnostics résidus (tests globaux)
    irf.shapiro_pvalue,
    irf.dagostino_pvalue,
    irf.runs_test_pvalue,
    irf.ljung_box_pvalue,
    irf.durbin_watson_stat,

    -- ▸ Classification qualité
    CASE
        WHEN irf.nash >= 0.7 THEN 'EXCELLENT'
        WHEN irf.nash >= 0.5 THEN 'BON'
        WHEN irf.nash >= 0.2 THEN 'MOYEN'
        WHEN irf.nash >= 0   THEN 'FAIBLE'
        ELSE 'MAUVAIS'
    END AS qualite_modele,

    CASE
        WHEN irf.ljung_box_pvalue > 0.05 AND irf.runs_test_pvalue > 0.05 THEN true
        ELSE false
    END AS residus_non_structures,

    -- ▸ Métadonnées série
    irf.n_observations,
    irf.series_start,
    irf.series_end,
    irf.series_length_days,
    irf.nan_fraction,
    irf.pastas_version,
    irf.fitted_at,

    -- ▸ Statistiques résidus — global
    rs.residual_mean,
    rs.residual_stddev,
    rs.residual_p05,
    rs.residual_p95,
    rs.residual_trend_m_per_year,
    rs.residual_n_obs,

    -- ▸ Statistiques résidus — 12 derniers mois
    rs.residual_mean_last_12m,
    rs.residual_stddev_last_12m,
    rs.residual_trend_last_12m,

    -- ▸ Statistiques résidus — 5 dernières années
    rs.residual_trend_last_5y,

    -- ▸ Statistiques résidus — saisonnalité
    rs.residual_summer_bias,
    rs.residual_winter_bias,
    rs.residual_summer_bias - rs.residual_winter_bias AS residual_seasonal_amplitude,
    rs.frac_negative_last_12m,

    -- ▸ Bilan hydrique moyen (mm/j)
    wb.wb_recharge_mean,
    wb.wb_etp_reelle_mean,
    wb.wb_ruissellement_mean,
    wb.wb_precip_efficace_mean,
    wb.wb_stockage_sol_mean,
    wb.wb_recharge_hiver_mean,
    wb.wb_recharge_ete_mean,

    -- ▸ Signatures (30 signatures hydrogéo)
    sig.cv_period_mean,
    sig.cv_date_min,
    sig.cv_date_max,
    sig.cv_fall_rate,
    sig.cv_rise_rate,
    sig.parde_seasonality,
    sig.avg_seasonal_fluctuation,
    sig.interannual_variation,
    sig.low_pulse_count,
    sig.high_pulse_count,
    sig.low_pulse_duration,
    sig.high_pulse_duration,
    sig.bimodality_coefficient,
    sig.mean_annual_maximum,
    sig.rise_rate,
    sig.fall_rate,
    sig.reversals_avg,
    sig.reversals_cv,
    sig.colwell_contingency,
    sig.colwell_constancy,
    sig.recession_constant,
    sig.recovery_constant,
    sig.duration_curve_slope,
    sig.duration_curve_ratio,
    sig.richards_pathlength,
    sig.baselevel_index,
    sig.baselevel_stability,
    sig.magnitude,
    sig.autocorr_time,
    sig.date_min              AS sig_date_min,
    sig.date_max              AS sig_date_max,
    sig.n_signatures_computed,

    -- ▸ Statistiques néerlandaises (GHG/GLG/GVG)
    sig.gg,
    sig.ghg,
    sig.glg,
    sig.gvg,
    sig.q_ghg,
    sig.q_glg,
    sig.q_gvg,

    -- ▸ SGI récent
    sr.sgi_last_date,
    sr.sgi_last_value,
    ss.sgi_mean_last_12m,
    ss.sgi_min_last_12m,
    ss.sgi_months_below_minus1,
    CASE
        WHEN sr.sgi_last_value < -2 THEN 'SECHERESSE_EXCEPTIONNELLE'
        WHEN sr.sgi_last_value < -1 THEN 'SECHERESSE_MODEREE'
        WHEN sr.sgi_last_value > 2  THEN 'HAUTES_EAUX_EXCEPT'
        WHEN sr.sgi_last_value > 1  THEN 'HAUTES_EAUX'
        ELSE 'NORMAL'
    END AS sgi_classification

FROM irf
LEFT JOIN residual_stats rs ON rs.code_bss = irf.code_bss
LEFT JOIN water_balance wb ON wb.code_bss = irf.code_bss
LEFT JOIN signatures sig ON sig.code_bss = irf.code_bss
LEFT JOIN sgi_recent sr ON sr.code_bss = irf.code_bss
LEFT JOIN sgi_stats ss ON ss.code_bss = irf.code_bss
