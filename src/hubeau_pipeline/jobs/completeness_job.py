"""
Data Completeness Check Job

Detects ingestion gaps in the warehouse: a yearly partition reload that failed
silently (e.g. the 2026-03-02 crash that lost January 2026) leaves a hole that
the 7-day daily pipeline never repairs. This job scans the last 13 complete
months in silver and gold and FAILS the run when a month is missing or
abnormally thin, so the gap shows up red in Dagster instead of being noticed
months later in the dashboards.

Checks per source (silver chroniques + gold monthly index, piezo and hydro; gold ERA5
monthly grid):
- MISSING: a month with zero rows.
- PARTIAL: a month with fewer rows than COMPLETENESS_MIN_PCT % (default 50)
  of the median of the non-empty months in the window.

Remediation is the documented backfill procedure: reload the year partition
(piezometry_chroniques_raw / hydrometry_obs_elab_raw), then the dbt chain with
piezometry_reprocess_from_date / streaming_lookback_days /
daily_recompute_window_days, then fct_monthly_index. For the ERA5 grid, see
era5_monthly_grid_lookback_months and the ERA5 backfill procedure in docs/ERA5.md.
"""

import os
import statistics

import psycopg2
from dagster import Failure, MetadataValue, OpExecutionContext, job, op

# (label, table, date column, extra WHERE)
_CHECKS = [
    ("silver piezo", "silver.stg_piezo_chroniques", "date_mesure", ""),
    ("silver hydro", "silver.stg_hydrometry_obs_elab", "date_obs_elab", ""),
    ("gold index piezo", "gold.fct_monthly_index", "month", "AND type = 'piezo'"),
    ("gold index hydro", "gold.fct_monthly_index", "month", "AND type = 'hydro'"),
    # Une ligne par cellule de grille ERA5 (~11496) : un mois avec moins de lignes que
    # la médiane signale un trou de contiguïté amont (cf. fct_era5_indices_grid).
    ("gold ERA5 grille", "gold.fct_era5_monthly_grid", "mois", ""),
]

_MONTHS_WINDOW = 13


def _monthly_counts(cur, table: str, date_col: str, extra_where: str) -> list[tuple]:
    """[(month, row_count)] for the last _MONTHS_WINDOW complete months."""
    cur.execute(f"""
        WITH months AS (
            SELECT generate_series(
                date_trunc('month', CURRENT_DATE) - INTERVAL '{_MONTHS_WINDOW} months',
                date_trunc('month', CURRENT_DATE) - INTERVAL '1 month',
                INTERVAL '1 month')::date AS m
        ),
        counts AS (
            SELECT date_trunc('month', {date_col})::date AS m, count(*) AS n
            FROM {table}
            WHERE {date_col} >= date_trunc('month', CURRENT_DATE) - INTERVAL '{_MONTHS_WINDOW} months'
              AND {date_col} < date_trunc('month', CURRENT_DATE)
              {extra_where}
            GROUP BY 1
        )
        SELECT months.m, COALESCE(counts.n, 0)
        FROM months LEFT JOIN counts USING (m)
        ORDER BY months.m
    """)
    return cur.fetchall()


@op
def check_data_completeness(context: OpExecutionContext) -> None:
    min_pct = float(os.environ.get("COMPLETENESS_MIN_PCT", "50"))
    conn = psycopg2.connect(
        host=os.environ.get("PG_HOST", "postgres"),
        port=int(os.environ.get("PG_PORT", "5432")),
        database=os.environ.get("PG_DB", "postgres"),
        user=os.environ.get("PG_USER", "postgres"),
        password=os.environ.get("PG_PASSWORD"),
    )
    anomalies: list[str] = []
    summary: dict[str, str] = {}
    try:
        with conn, conn.cursor() as cur:
            for label, table, date_col, extra_where in _CHECKS:
                rows = _monthly_counts(cur, table, date_col, extra_where)
                nonzero = [n for _, n in rows if n > 0]
                median = statistics.median(nonzero) if nonzero else 0
                threshold = median * min_pct / 100.0
                issues = []
                for month, n in rows:
                    if n == 0:
                        issues.append(f"{month:%Y-%m}: MISSING")
                    elif n < threshold:
                        issues.append(
                            f"{month:%Y-%m}: PARTIAL ({n:,} rows, "
                            f"{100 * n / median:.0f}% of median {median:,.0f})"
                        )
                summary[label] = "; ".join(issues) if issues else f"OK (median {median:,.0f} rows/month)"
                if issues:
                    anomalies.extend(f"[{label}] {i}" for i in issues)
                context.log.info("%s → %s", label, summary[label])
    finally:
        conn.close()

    if anomalies:
        raise Failure(
            description=(
                f"{len(anomalies)} completeness anomaly(ies) over the last "
                f"{_MONTHS_WINDOW} months — see metadata. Remediation: reload the "
                "year partition then the dbt backfill chain (see module docstring)."
            ),
            metadata={
                "anomalies": MetadataValue.md("\n".join(f"- {a}" for a in anomalies)),
                "summary": MetadataValue.json(summary),
            },
        )
    context.log.info("Completeness check passed for all %d sources.", len(_CHECKS))


@job(
    name="data_completeness_check",
    description=(
        "Quality: detect missing/partial months (last 13 months) in silver "
        "chroniques, gold.fct_monthly_index and gold.fct_era5_monthly_grid. "
        "Fails on any gap."
    ),
    tags={"dagster/concurrency_key": "data_completeness_check"},
)
def data_completeness_job():
    check_data_completeness()
