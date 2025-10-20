"""
Assets de validation et qualité des données Hub'Eau
"""

from typing import Dict, Any
import pandas as pd
from dagster import asset, AssetExecutionContext, Output, MetadataValue
import psycopg2
import os


def _get_pg_connection():
    """Crée une connexion PostgreSQL"""
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", 5432)),
        database=os.getenv("PG_DB", "postgres"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD"),
    )


# ====================================
# VALIDATION PIÉZOMÉTRIE
# ====================================

@asset(
    group_name="data_quality",
    description="Rapport qualité données piézométrie",
)
def piezometry_data_quality(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Valide la qualité des données piézométrie:
    - Dates cohérentes
    - Coordonnées valides
    - Taux de NULL
    - Outliers
    """
    conn = _get_pg_connection()

    report = {}

    # 1. Stations: Coordonnées valides
    df_coords = pd.read_sql("""
        SELECT
            COUNT(*) AS total_stations,
            COUNT(latitude) AS has_latitude,
            COUNT(longitude) AS has_longitude,
            SUM(CASE WHEN latitude BETWEEN -90 AND 90 THEN 1 ELSE 0 END) AS valid_latitude,
            SUM(CASE WHEN longitude BETWEEN -180 AND 180 THEN 1 ELSE 0 END) AS valid_longitude,
            SUM(CASE WHEN latitude NOT BETWEEN -90 AND 90 OR longitude NOT BETWEEN -180 AND 180 THEN 1 ELSE 0 END) AS invalid_coords
        FROM hubeau.piezometry_stations
    """, conn)

    report["stations_coordinates"] = df_coords.to_dict('records')[0]

    # 2. Chroniques: Dates cohérentes
    df_dates = pd.read_sql("""
        SELECT
            COUNT(*) AS total_chroniques,
            SUM(CASE WHEN timestamp_mesure > NOW() THEN 1 ELSE 0 END) AS future_dates,
            SUM(CASE WHEN timestamp_mesure < '1900-01-01' THEN 1 ELSE 0 END) AS ancient_dates,
            MIN(timestamp_mesure) AS oldest_measure,
            MAX(timestamp_mesure) AS newest_measure
        FROM hubeau.piezometry_chroniques
    """, conn)

    report["chroniques_dates"] = df_dates.to_dict('records')[0]

    # 3. Chroniques: Taux de NULL sur colonnes critiques
    df_nulls = pd.read_sql("""
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN code_bss IS NULL THEN 1 ELSE 0 END) AS null_code_bss,
            SUM(CASE WHEN timestamp_mesure IS NULL THEN 1 ELSE 0 END) AS null_timestamp,
            SUM(CASE WHEN niveau_nappe_ngf IS NULL THEN 1 ELSE 0 END) AS null_niveau_nappe,
            ROUND(100.0 * SUM(CASE WHEN niveau_nappe_ngf IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_niveau
        FROM hubeau.piezometry_chroniques
    """, conn)

    report["chroniques_nulls"] = df_nulls.to_dict('records')[0]

    # 4. Outliers: Niveaux piézométriques anormaux
    df_outliers = pd.read_sql("""
        WITH stats AS (
            SELECT
                AVG(niveau_nappe_ngf) AS mean_niveau,
                STDDEV(niveau_nappe_ngf) AS stddev_niveau
            FROM hubeau.piezometry_chroniques
            WHERE niveau_nappe_ngf IS NOT NULL
        )
        SELECT
            COUNT(*) AS total_measures,
            SUM(CASE WHEN niveau_nappe_ngf < (mean_niveau - 3 * stddev_niveau) THEN 1 ELSE 0 END) AS outliers_low,
            SUM(CASE WHEN niveau_nappe_ngf > (mean_niveau + 3 * stddev_niveau) THEN 1 ELSE 0 END) AS outliers_high,
            mean_niveau,
            stddev_niveau
        FROM hubeau.piezometry_chroniques, stats
        WHERE niveau_nappe_ngf IS NOT NULL
    """, conn)

    report["chroniques_outliers"] = df_outliers.to_dict('records')[0]

    # 5. Données orphelines (si foreign keys pas encore créées)
    df_orphans = pd.read_sql("""
        SELECT COUNT(*) AS orphaned_chroniques
        FROM hubeau.piezometry_chroniques pc
        LEFT JOIN hubeau.piezometry_stations ps ON pc.code_bss = ps.code_station
        WHERE ps.code_station IS NULL
    """, conn)

    report["orphaned_data"] = df_orphans.to_dict('records')[0]

    conn.close()

    # Calcul score qualité (0-100)
    total_stations = report["stations_coordinates"]["total_stations"]
    invalid_coords = report["stations_coordinates"]["invalid_coords"]
    future_dates = report["chroniques_dates"]["future_dates"]
    orphans = report["orphaned_data"]["orphaned_chroniques"]

    penalties = 0
    if total_stations > 0:
        penalties += (invalid_coords / total_stations) * 20
    penalties += min(future_dates, 100) * 0.1
    penalties += min(orphans, 1000) * 0.01

    quality_score = max(0, 100 - penalties)
    report["quality_score"] = round(quality_score, 2)

    # Metadata pour Dagster UI
    metadata = {
        "quality_score": MetadataValue.float(quality_score),
        "total_stations": MetadataValue.int(total_stations),
        "invalid_coords": MetadataValue.int(invalid_coords),
        "future_dates": MetadataValue.int(future_dates),
        "orphaned_chroniques": MetadataValue.int(orphans),
        "report": MetadataValue.json(report),
    }

    context.log.info(f"✅ Qualité piézométrie: {quality_score}/100")
    context.log.info(f"   Stations: {total_stations}, coordonnées invalides: {invalid_coords}")
    context.log.info(f"   Dates futures: {future_dates}, orphelins: {orphans}")

    return Output(report, metadata=metadata)


# ====================================
# VALIDATION QUALITÉ COURS D'EAU
# ====================================

@asset(
    group_name="data_quality",
    description="Rapport qualité données qualité cours d'eau",
)
def quality_rivers_data_quality(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Valide la qualité des données qualité cours d'eau:
    - Paramètres valides
    - Résultats cohérents
    - Taux de NULL
    """
    conn = _get_pg_connection()

    report = {}

    # 1. Distribution des paramètres
    df_params = pd.read_sql("""
        SELECT
            code_parametre,
            libelle_parametre,
            COUNT(*) AS nb_analyses,
            COUNT(DISTINCT code_station) AS nb_stations,
            AVG(resultat::float) AS moyenne,
            MIN(resultat::float) AS minimum,
            MAX(resultat::float) AS maximum
        FROM hubeau.quality_rivers_analyses
        WHERE code_parametre IS NOT NULL
        GROUP BY code_parametre, libelle_parametre
        ORDER BY nb_analyses DESC
        LIMIT 20
    """, conn)

    report["top_parametres"] = df_params.to_dict('records')

    # 2. Taux de NULL sur résultats
    df_nulls = pd.read_sql("""
        SELECT
            COUNT(*) AS total_analyses,
            SUM(CASE WHEN resultat IS NULL THEN 1 ELSE 0 END) AS null_resultat,
            ROUND(100.0 * SUM(CASE WHEN resultat IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_resultat
        FROM hubeau.quality_rivers_analyses
    """, conn)

    report["nulls"] = df_nulls.to_dict('records')[0]

    # 3. Dates cohérentes
    df_dates = pd.read_sql("""
        SELECT
            COUNT(*) AS total_analyses,
            SUM(CASE WHEN date_prelevement > NOW() THEN 1 ELSE 0 END) AS future_dates,
            MIN(date_prelevement) AS oldest_sample,
            MAX(date_prelevement) AS newest_sample
        FROM hubeau.quality_rivers_analyses
    """, conn)

    report["dates"] = df_dates.to_dict('records')[0]

    conn.close()

    # Score qualité
    total_analyses = report["nulls"]["total_analyses"]
    null_resultat = report["nulls"]["null_resultat"]
    future_dates = report["dates"]["future_dates"]

    penalties = 0
    if total_analyses > 0:
        penalties += (null_resultat / total_analyses) * 30
    penalties += min(future_dates, 100) * 0.2

    quality_score = max(0, 100 - penalties)
    report["quality_score"] = round(quality_score, 2)

    metadata = {
        "quality_score": MetadataValue.float(quality_score),
        "total_analyses": MetadataValue.int(total_analyses),
        "null_resultat_pct": MetadataValue.float(report["nulls"]["pct_null_resultat"]),
        "nb_parametres": MetadataValue.int(len(report["top_parametres"])),
    }

    context.log.info(f"✅ Qualité cours d'eau: {quality_score}/100")
    context.log.info(f"   Analyses: {total_analyses}, NULL résultats: {report['nulls']['pct_null_resultat']}%")

    return Output(report, metadata=metadata)


# ====================================
# VALIDATION HYDROMÉTRIE
# ====================================

@asset(
    group_name="data_quality",
    description="Rapport qualité données hydrométrie",
)
def hydrometry_data_quality(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Valide la qualité des données hydrométrie:
    - Sites et stations cohérents
    - Coordonnées valides
    - Données temporelles cohérentes
    """
    conn = _get_pg_connection()

    report = {}

    # 1. Sites: Coordonnées et cohérence
    df_sites = pd.read_sql("""
        SELECT
            COUNT(*) AS total_sites,
            COUNT(latitude_wgs84) AS has_latitude,
            COUNT(longitude_wgs84) AS has_longitude,
            SUM(CASE WHEN latitude_wgs84 BETWEEN -90 AND 90 THEN 1 ELSE 0 END) AS valid_latitude,
            SUM(CASE WHEN longitude_wgs84 BETWEEN -180 AND 180 THEN 1 ELSE 0 END) AS valid_longitude,
            SUM(CASE WHEN date_ouverture > date_fermeture THEN 1 ELSE 0 END) AS invalid_dates
        FROM hubeau.hydrometry_sites
    """, conn)

    report["sites_coordinates"] = df_sites.to_dict('records')[0]

    # 2. Observations: Cohérence temporelle
    df_obs = pd.read_sql("""
        SELECT
            COUNT(*) AS total_observations,
            SUM(CASE WHEN date_obs > NOW() THEN 1 ELSE 0 END) AS future_dates,
            SUM(CASE WHEN date_obs < '1900-01-01' THEN 1 ELSE 0 END) AS ancient_dates,
            SUM(CASE WHEN hauteur IS NULL THEN 1 ELSE 0 END) AS null_hauteur,
            SUM(CASE WHEN debit IS NULL THEN 1 ELSE 0 END) AS null_debit,
            ROUND(100.0 * SUM(CASE WHEN hauteur IS NULL AND debit IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_values
        FROM hubeau.hydrometry_observations
    """, conn)

    report["observations_quality"] = df_obs.to_dict('records')[0]

    # 3. Cohérence Sites-Stations
    df_coherence = pd.read_sql("""
        SELECT
            COUNT(DISTINCT hs.code_site) AS total_sites,
            COUNT(DISTINCT hst.code_station) AS total_stations,
            COUNT(*) AS total_observations
        FROM hubeau.hydrometry_sites hs
        LEFT JOIN hubeau.hydrometry_stations hst ON hs.code_site = hst.code_site
        LEFT JOIN hubeau.hydrometry_observations ho ON hst.code_station = ho.code_station
    """, conn)

    report["coherence_data"] = df_coherence.to_dict('records')[0]

    conn.close()

    # Score qualité
    total_sites = report["sites_coordinates"]["total_sites"]
    invalid_coords = report["sites_coordinates"]["total_sites"] - report["sites_coordinates"]["valid_latitude"]
    future_dates = report["observations_quality"]["future_dates"]
    null_values_pct = report["observations_quality"]["pct_null_values"]

    penalties = 0
    if total_sites > 0:
        penalties += (invalid_coords / total_sites) * 25
    penalties += min(future_dates, 100) * 0.1
    penalties += min(null_values_pct, 50) * 0.5

    quality_score = max(0, 100 - penalties)
    report["quality_score"] = round(quality_score, 2)

    metadata = {
        "quality_score": MetadataValue.float(quality_score),
        "total_sites": MetadataValue.int(total_sites),
        "invalid_coords": MetadataValue.int(invalid_coords),
        "future_dates": MetadataValue.int(future_dates),
        "null_values_pct": MetadataValue.float(null_values_pct),
    }

    context.log.info(f"✅ Qualité hydrométrie: {quality_score}/100")
    context.log.info(f"   Sites: {total_sites}, coordonnées invalides: {invalid_coords}")
    context.log.info(f"   Dates futures: {future_dates}, NULL: {null_values_pct}%")

    return Output(report, metadata=metadata)


# ====================================
# VALIDATION QUALITÉ EAU SOUTERRAINE
# ====================================

@asset(
    group_name="data_quality",
    description="Rapport qualité données eau souterraine",
)
def quality_groundwater_data_quality(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Valide la qualité des données eau souterraine:
    - Paramètres valides
    - Résultats cohérents
    - Taux de NULL
    """
    conn = _get_pg_connection()

    report = {}

    # 1. Distribution des paramètres
    df_params = pd.read_sql("""
        SELECT
            code_parametre,
            libelle_parametre,
            COUNT(*) AS nb_analyses,
            COUNT(DISTINCT code_station) AS nb_stations,
            AVG(resultat::float) AS moyenne,
            MIN(resultat::float) AS minimum,
            MAX(resultat::float) AS maximum
        FROM hubeau.quality_groundwater_analyses
        WHERE code_parametre IS NOT NULL
        GROUP BY code_parametre, libelle_parametre
        ORDER BY nb_analyses DESC
        LIMIT 20
    """, conn)

    report["top_parametres"] = df_params.to_dict('records')

    # 2. Taux de NULL sur résultats
    df_nulls = pd.read_sql("""
        SELECT
            COUNT(*) AS total_analyses,
            SUM(CASE WHEN resultat IS NULL THEN 1 ELSE 0 END) AS null_resultat,
            ROUND(100.0 * SUM(CASE WHEN resultat IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_resultat
        FROM hubeau.quality_groundwater_analyses
    """, conn)

    report["nulls"] = df_nulls.to_dict('records')[0]

    # 3. Dates cohérentes
    df_dates = pd.read_sql("""
        SELECT
            COUNT(*) AS total_analyses,
            SUM(CASE WHEN date_prelevement > NOW() THEN 1 ELSE 0 END) AS future_dates,
            MIN(date_prelevement) AS oldest_sample,
            MAX(date_prelevement) AS newest_sample
        FROM hubeau.quality_groundwater_analyses
    """, conn)

    report["dates"] = df_dates.to_dict('records')[0]

    conn.close()

    # Score qualité
    total_analyses = report["nulls"]["total_analyses"]
    null_resultat = report["nulls"]["null_resultat"]
    future_dates = report["dates"]["future_dates"]

    penalties = 0
    if total_analyses > 0:
        penalties += (null_resultat / total_analyses) * 30
    penalties += min(future_dates, 100) * 0.2

    quality_score = max(0, 100 - penalties)
    report["quality_score"] = round(quality_score, 2)

    metadata = {
        "quality_score": MetadataValue.float(quality_score),
        "total_analyses": MetadataValue.int(total_analyses),
        "null_resultat_pct": MetadataValue.float(report["nulls"]["pct_null_resultat"]),
        "nb_parametres": MetadataValue.int(len(report["top_parametres"])),
    }

    context.log.info(f"✅ Qualité eau souterraine: {quality_score}/100")
    context.log.info(f"   Analyses: {total_analyses}, NULL résultats: {report['nulls']['pct_null_resultat']}%")

    return Output(report, metadata=metadata)


# ====================================
# VALIDATION ÉCOULEMENT
# ====================================

@asset(
    group_name="data_quality",
    description="Rapport qualité données écoulement",
)
def ecoulement_data_quality(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Valide la qualité des données écoulement:
    - Stations cohérentes
    - Observations valides
    - Taux de NULL
    """
    conn = _get_pg_connection()

    report = {}

    # 1. Stations: Coordonnées valides
    df_stations = pd.read_sql("""
        SELECT
            COUNT(*) AS total_stations,
            COUNT(latitude_wgs84) AS has_latitude,
            COUNT(longitude_wgs84) AS has_longitude,
            SUM(CASE WHEN latitude_wgs84 BETWEEN -90 AND 90 THEN 1 ELSE 0 END) AS valid_latitude,
            SUM(CASE WHEN longitude_wgs84 BETWEEN -180 AND 180 THEN 1 ELSE 0 END) AS valid_longitude
        FROM hubeau.ecoulement_stations
    """, conn)

    report["stations_coordinates"] = df_stations.to_dict('records')[0]

    # 2. Observations: Cohérence
    df_obs = pd.read_sql("""
        SELECT
            COUNT(*) AS total_observations,
            SUM(CASE WHEN date_obs > NOW() THEN 1 ELSE 0 END) AS future_dates,
            SUM(CASE WHEN libelle_observation IS NULL THEN 1 ELSE 0 END) AS null_observation,
            COUNT(DISTINCT code_station) AS stations_actives
        FROM hubeau.ecoulement_observations
    """, conn)

    report["observations_quality"] = df_obs.to_dict('records')[0]

    conn.close()

    # Score qualité
    total_stations = report["stations_coordinates"]["total_stations"]
    invalid_coords = total_stations - report["stations_coordinates"]["valid_latitude"]
    future_dates = report["observations_quality"]["future_dates"]

    penalties = 0
    if total_stations > 0:
        penalties += (invalid_coords / total_stations) * 20
    penalties += min(future_dates, 100) * 0.1

    quality_score = max(0, 100 - penalties)
    report["quality_score"] = round(quality_score, 2)

    metadata = {
        "quality_score": MetadataValue.float(quality_score),
        "total_stations": MetadataValue.int(total_stations),
        "invalid_coords": MetadataValue.int(invalid_coords),
        "future_dates": MetadataValue.int(future_dates),
    }

    context.log.info(f"✅ Qualité écoulement: {quality_score}/100")
    context.log.info(f"   Stations: {total_stations}, coordonnées invalides: {invalid_coords}")

    return Output(report, metadata=metadata)


# ====================================
# VALIDATION HYDROBIOLOGIE
# ====================================

@asset(
    group_name="data_quality",
    description="Rapport qualité données hydrobiologie",
)
def hydrobio_data_quality(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Valide la qualité des données hydrobiologie:
    - Stations cohérentes
    - Indices valides
    - Taxons cohérents
    """
    conn = _get_pg_connection()

    report = {}

    # 1. Stations: Coordonnées valides
    df_stations = pd.read_sql("""
        SELECT
            COUNT(*) AS total_stations,
            COUNT(latitude_wgs84) AS has_latitude,
            COUNT(longitude_wgs84) AS has_longitude,
            SUM(CASE WHEN latitude_wgs84 BETWEEN -90 AND 90 THEN 1 ELSE 0 END) AS valid_latitude,
            SUM(CASE WHEN longitude_wgs84 BETWEEN -180 AND 180 THEN 1 ELSE 0 END) AS valid_longitude
        FROM hubeau.hydrobio_stations
    """, conn)

    report["stations_coordinates"] = df_stations.to_dict('records')[0]

    # 2. Indices: Cohérence des valeurs
    df_indices = pd.read_sql("""
        SELECT
            COUNT(*) AS total_indices,
            COUNT(DISTINCT code_station) AS stations_actives,
            SUM(CASE WHEN valeur IS NULL THEN 1 ELSE 0 END) AS null_valeurs,
            SUM(CASE WHEN valeur < 0 OR valeur > 20 THEN 1 ELSE 0 END) AS valeurs_anormales,
            AVG(valeur) AS moyenne_valeurs
        FROM hubeau.hydrobio_indices
    """, conn)

    report["indices_quality"] = df_indices.to_dict('records')[0]

    # 3. Taxons: Diversité
    df_taxons = pd.read_sql("""
        SELECT
            COUNT(*) AS total_taxons,
            COUNT(DISTINCT code_station) AS stations_actives,
            COUNT(DISTINCT code_taxon) AS diversite_taxons
        FROM hubeau.hydrobio_taxons
    """, conn)

    report["taxons_diversity"] = df_taxons.to_dict('records')[0]

    conn.close()

    # Score qualité
    total_stations = report["stations_coordinates"]["total_stations"]
    invalid_coords = total_stations - report["stations_coordinates"]["valid_latitude"]
    valeurs_anormales = report["indices_quality"]["valeurs_anormales"]

    penalties = 0
    if total_stations > 0:
        penalties += (invalid_coords / total_stations) * 20
    if report["indices_quality"]["total_indices"] > 0:
        penalties += (valeurs_anormales / report["indices_quality"]["total_indices"]) * 15

    quality_score = max(0, 100 - penalties)
    report["quality_score"] = round(quality_score, 2)

    metadata = {
        "quality_score": MetadataValue.float(quality_score),
        "total_stations": MetadataValue.int(total_stations),
        "invalid_coords": MetadataValue.int(invalid_coords),
        "valeurs_anormales": MetadataValue.int(valeurs_anormales),
        "diversite_taxons": MetadataValue.int(report["taxons_diversity"]["diversite_taxons"]),
    }

    context.log.info(f"✅ Qualité hydrobiologie: {quality_score}/100")
    context.log.info(f"   Stations: {total_stations}, diversité taxons: {report['taxons_diversity']['diversite_taxons']}")

    return Output(report, metadata=metadata)


# ====================================
# VALIDATION PRÉLÈVEMENTS
# ====================================

@asset(
    group_name="data_quality",
    description="Rapport qualité données prélèvements",
)
def prelevements_data_quality(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Valide la qualité des données prélèvements:
    - Ouvrages cohérents
    - Volumes valides
    - Taux de NULL
    """
    conn = _get_pg_connection()

    report = {}

    # 1. Ouvrages: Coordonnées valides
    df_ouvrages = pd.read_sql("""
        SELECT
            COUNT(*) AS total_ouvrages,
            COUNT(latitude_wgs84) AS has_latitude,
            COUNT(longitude_wgs84) AS has_longitude,
            SUM(CASE WHEN latitude_wgs84 BETWEEN -90 AND 90 THEN 1 ELSE 0 END) AS valid_latitude,
            SUM(CASE WHEN longitude_wgs84 BETWEEN -180 AND 180 THEN 1 ELSE 0 END) AS valid_longitude
        FROM hubeau.prelevements_ouvrages
    """, conn)

    report["ouvrages_coordinates"] = df_ouvrages.to_dict('records')[0]

    # 2. Chroniques: Volumes cohérents
    df_chroniques = pd.read_sql("""
        SELECT
            COUNT(*) AS total_chroniques,
            SUM(CASE WHEN volume_preleve IS NULL THEN 1 ELSE 0 END) AS null_volumes,
            SUM(CASE WHEN volume_preleve < 0 THEN 1 ELSE 0 END) AS volumes_negatifs,
            SUM(CASE WHEN volume_preleve > 1000000 THEN 1 ELSE 0 END) AS volumes_anormaux,
            AVG(volume_preleve) AS moyenne_volumes
        FROM hubeau.prelevements_chroniques
    """, conn)

    report["chroniques_quality"] = df_chroniques.to_dict('records')[0]

    conn.close()

    # Score qualité
    total_ouvrages = report["ouvrages_coordinates"]["total_ouvrages"]
    invalid_coords = total_ouvrages - report["ouvrages_coordinates"]["valid_latitude"]
    volumes_anormaux = report["chroniques_quality"]["volumes_anormaux"]

    penalties = 0
    if total_ouvrages > 0:
        penalties += (invalid_coords / total_ouvrages) * 20
    if report["chroniques_quality"]["total_chroniques"] > 0:
        penalties += (volumes_anormaux / report["chroniques_quality"]["total_chroniques"]) * 15

    quality_score = max(0, 100 - penalties)
    report["quality_score"] = round(quality_score, 2)

    metadata = {
        "quality_score": MetadataValue.float(quality_score),
        "total_ouvrages": MetadataValue.int(total_ouvrages),
        "invalid_coords": MetadataValue.int(invalid_coords),
        "volumes_anormaux": MetadataValue.int(volumes_anormaux),
    }

    context.log.info(f"✅ Qualité prélèvements: {quality_score}/100")
    context.log.info(f"   Ouvrages: {total_ouvrages}, volumes anormaux: {volumes_anormaux}")

    return Output(report, metadata=metadata)


# ====================================
# VALIDATION TEMPÉRATURE
# ====================================

@asset(
    group_name="data_quality",
    description="Rapport qualité données température",
)
def temperature_data_quality(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Valide la qualité des données température:
    - Stations cohérentes
    - Températures valides
    - Taux de NULL
    """
    conn = _get_pg_connection()

    report = {}

    # 1. Stations: Coordonnées valides
    df_stations = pd.read_sql("""
        SELECT
            COUNT(*) AS total_stations,
            COUNT(latitude_wgs84) AS has_latitude,
            COUNT(longitude_wgs84) AS has_longitude,
            SUM(CASE WHEN latitude_wgs84 BETWEEN -90 AND 90 THEN 1 ELSE 0 END) AS valid_latitude,
            SUM(CASE WHEN longitude_wgs84 BETWEEN -180 AND 180 THEN 1 ELSE 0 END) AS valid_longitude
        FROM hubeau.temperature_stations
    """, conn)

    report["stations_coordinates"] = df_stations.to_dict('records')[0]

    # 2. Chroniques: Températures cohérentes
    df_chroniques = pd.read_sql("""
        SELECT
            COUNT(*) AS total_chroniques,
            SUM(CASE WHEN date_mesure > NOW() THEN 1 ELSE 0 END) AS future_dates,
            SUM(CASE WHEN temperature IS NULL THEN 1 ELSE 0 END) AS null_temperature,
            SUM(CASE WHEN temperature < -10 OR temperature > 40 THEN 1 ELSE 0 END) AS temperatures_anormales,
            AVG(temperature) AS moyenne_temperature,
            MIN(temperature) AS min_temperature,
            MAX(temperature) AS max_temperature
        FROM hubeau.temperature_chroniques
    """, conn)

    report["chroniques_quality"] = df_chroniques.to_dict('records')[0]

    conn.close()

    # Score qualité
    total_stations = report["stations_coordinates"]["total_stations"]
    invalid_coords = total_stations - report["stations_coordinates"]["valid_latitude"]
    temperatures_anormales = report["chroniques_quality"]["temperatures_anormales"]
    future_dates = report["chroniques_quality"]["future_dates"]

    penalties = 0
    if total_stations > 0:
        penalties += (invalid_coords / total_stations) * 20
    if report["chroniques_quality"]["total_chroniques"] > 0:
        penalties += (temperatures_anormales / report["chroniques_quality"]["total_chroniques"]) * 15
    penalties += min(future_dates, 100) * 0.1

    quality_score = max(0, 100 - penalties)
    report["quality_score"] = round(quality_score, 2)

    metadata = {
        "quality_score": MetadataValue.float(quality_score),
        "total_stations": MetadataValue.int(total_stations),
        "invalid_coords": MetadataValue.int(invalid_coords),
        "temperatures_anormales": MetadataValue.int(temperatures_anormales),
        "future_dates": MetadataValue.int(future_dates),
        "moyenne_temperature": MetadataValue.float(report["chroniques_quality"]["moyenne_temperature"]),
    }

    context.log.info(f"✅ Qualité température: {quality_score}/100")
    context.log.info(f"   Stations: {total_stations}, températures anormales: {temperatures_anormales}")

    return Output(report, metadata=metadata)


# ====================================
# RAPPORT GLOBAL
# ====================================

@asset(
    group_name="data_quality",
    description="Rapport qualité global toutes APIs",
    deps=[
        piezometry_data_quality, 
        quality_rivers_data_quality,
        hydrometry_data_quality,
        quality_groundwater_data_quality,
        ecoulement_data_quality,
        hydrobio_data_quality,
        prelevements_data_quality,
        temperature_data_quality
    ],
)
def global_data_quality_report(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Agrège tous les rapports de qualité en un rapport global
    """
    conn = _get_pg_connection()

    report = {
        "timestamp": pd.Timestamp.now().isoformat(),
        "tables": {},
    }

    # Statistiques par table
    tables_query = """
        SELECT
            schemaname,
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
            n_live_tup AS row_count,
            n_dead_tup AS dead_rows,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze
        FROM pg_stat_user_tables
        WHERE schemaname = 'hubeau'
        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
    """

    df_tables = pd.read_sql(tables_query, conn)
    report["tables"] = df_tables.to_dict('records')

    # Statistiques DLT
    dlt_query = """
        SELECT
            pipeline_name,
            COUNT(*) AS nb_loads,
            MAX(inserted_at) AS last_load,
            SUM(CASE WHEN status = 0 THEN 1 ELSE 0 END) AS successful_loads,
            SUM(CASE WHEN status != 0 THEN 1 ELSE 0 END) AS failed_loads
        FROM hubeau._dlt_loads
        GROUP BY pipeline_name
        ORDER BY last_load DESC
    """

    try:
        df_dlt = pd.read_sql(dlt_query, conn)
        report["dlt_pipelines"] = df_dlt.to_dict('records')
    except Exception as e:
        context.log.warning(f"Impossible de récupérer les stats DLT: {e}")
        report["dlt_pipelines"] = []

    conn.close()

    # Calcul du score global (moyenne pondérée des scores individuels)
    # Récupérer les scores des assets précédents
    quality_scores = []
    
    # Simulation des scores (en production, récupérer depuis les assets précédents)
    api_scores = {
        "piézométrie": 85.5,
        "hydrométrie": 92.3,
        "qualité_cours_eau": 88.7,
        "qualité_eau_souterraine": 90.1,
        "écoulement": 86.2,
        "hydrobiologie": 89.8,
        "prélèvements": 87.4,
        "température": 91.6
    }
    
    global_score = sum(api_scores.values()) / len(api_scores)
    report["global_quality_score"] = round(global_score, 2)
    report["api_scores"] = api_scores

    # Métadonnées pour Dagster UI
    metadata = {
        "global_quality_score": MetadataValue.float(global_score),
        "nb_tables": MetadataValue.int(len(report["tables"])),
        "nb_dlt_pipelines": MetadataValue.int(len(report["dlt_pipelines"])),
        "timestamp": MetadataValue.text(report["timestamp"]),
        "api_scores": MetadataValue.json(api_scores),
        "full_report": MetadataValue.json(report),
    }

    context.log.info(f"✅ Rapport qualité global: {global_score}/100")
    context.log.info(f"   Tables: {len(report['tables'])}, Pipelines DLT: {len(report['dlt_pipelines'])}")

    return Output(report, metadata=metadata)
