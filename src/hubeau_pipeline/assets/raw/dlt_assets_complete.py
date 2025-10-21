from typing import Any, Dict, List, Optional

import time
import io
import logging

import dlt
from dagster import AssetExecutionContext, asset, DailyPartitionsDefinition, StaticPartitionsDefinition

from src.dlt_pipeline.hubeau_source import hubeau_rest_source, load_hubeau_config

# Partitions pour les données historiques (annuelles depuis 2020 + partition "all")
YEARLY_PARTITIONS = StaticPartitionsDefinition(
    ["all"] + [str(year) for year in range(2020, 2026)]  # "all", 2020-2025
)

# ====================================
# UTILITAIRES POUR RÉDUIRE LA REDONDANCE
# ====================================

def _get_partition_date_yearly(context: AssetExecutionContext) -> Optional[str]:
    """
    Convertit une partition annuelle (ex: '2024') en date (ex: '2024-01-01').
    Si partition = 'all', retourne None (pas de filtre temporel).
    """
    partition_key = context.partition_key
    if partition_key == "all":
        return None
    return f"{partition_key}-01-01"

def ingest_dlt(context: AssetExecutionContext, config_path: str, **kwargs) -> Dict[str, Any]:
    """
    Fonction générique pour ingérer des données avec DLT.
    """
    try:
        # Charger la configuration
        full_path = context.resources.file_manager.base_dir / config_path
        config = load_hubeau_config(str(full_path))
        
        # Configuration DLT
        source_name = config["source"]["name"]
        resource_name = config["resource"]["name"]
        dataset_name = config["destinations"]["postgres"]["dataset_name"]
        
        # Vérifier que le schéma hubeau existe
        try:
            import psycopg2
            from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
            
            conn = psycopg2.connect(
                host=context.resources.postgres_host,
                port=context.resources.postgres_port,
                database=context.resources.postgres_database,
                user=context.resources.postgres_user,
                password=context.resources.postgres_password
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            
            with conn.cursor() as cur:
                # Vérifier si le schéma hubeau existe
                cur.execute("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name = 'hubeau'
                """)
                
                if not cur.fetchone():
                    context.log.warning("⚠️ Schema 'hubeau' n'existe pas - création automatique...")
                    # Créer le schéma si nécessaire
                    cur.execute("CREATE SCHEMA IF NOT EXISTS hubeau")
                    context.log.info("✅ Schema 'hubeau' créé")
                else:
                    context.log.info("✅ Schema 'hubeau' existe déjà")
            
            conn.close()
        except Exception as e:
            context.log.error(f"❌ Erreur vérification schéma: {e}")
            context.log.warning("⚠️ Continuation sans vérification du schéma...")

        # Configuration de la destination PostgreSQL
        destination = dlt.destinations.postgres(
            credentials={
                "host": context.resources.postgres_host,
                "port": context.resources.postgres_port,
                "database": context.resources.postgres_database,
                "username": context.resources.postgres_user,
                "password": context.resources.postgres_password,
            },
            dataset_name=dataset_name,
            create_indexes=False,  # Désactiver la création d'index automatique
        )

        # Créer un nom de pipeline unique pour éviter les conflits
        pipeline_name = f"hubeau_{source_name}_{resource_name}_complete"
        context.log.info(f"📦 DLT pipeline name: {pipeline_name} (prevents schema conflicts)")

        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=destination,
            dataset_name=dataset_name,
            # Configuration pour éviter la création de tables parasites
            schema_contract="freeze",  # Utiliser les schémas existants
            full_refresh=False  # Éviter la recréation complète
        )

        # Créer la source Hub'Eau
        source = hubeau_rest_source(
            config_path=str(full_path),
            **kwargs
        )

        # Exécuter le pipeline
        context.log.info(f"🚀 Démarrage de l'ingestion {resource_name}...")
        start_time = time.time()
        
        load_info = pipeline.run(source)
        
        end_time = time.time()
        duration = end_time - start_time
        
        context.log.info(f"✅ Ingestion {resource_name} terminée en {duration:.2f}s")
        context.log.info(f"📊 Informations de chargement: {load_info}")
        
        return {
            "pipeline_name": pipeline_name,
            "resource_name": resource_name,
            "load_info": load_info,
            "duration_seconds": duration,
            "status": "success"
        }
        
    except Exception as e:
        context.log.error(f"❌ Erreur lors de l'ingestion {config_path}: {e}")
        raise

# ====================================
# ASSETS COMPLETS POUR HYDROMÉTRIE
# ====================================

@asset(group_name="hubeau_hydrometry_complete")
def hydrometry_stations_complete_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrometry stations reference data with ALL attributes using dlt."""
    return ingest_dlt(context, "configs/hubeau/hydrometry_stations_complete.yml")

@asset(group_name="hubeau_hydrometry_complete")
def hydrometry_sites_complete_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrometry sites reference data with ALL attributes using dlt."""
    return ingest_dlt(context, "configs/hubeau/hydrometry_sites_complete.yml")

@asset(group_name="hubeau_hydrometry_complete", partitions_def=YEARLY_PARTITIONS, deps=[hydrometry_stations_complete_reference])
def hydrometry_obs_elab_complete(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrometry elaborated observations with ALL attributes."""
    partition_date = _get_partition_date_yearly(context)
    return ingest_dlt(context, "configs/hubeau/hydrometry_obs_elab_complete.yml", partition_date=partition_date)

# ====================================
# ASSETS COMPLETS POUR PIÉZOMÉTRIE
# ====================================

@asset(group_name="hubeau_piezometry_complete")
def piezometry_stations_complete_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests piezometry stations reference data with ALL attributes using dlt."""
    return ingest_dlt(context, "configs/hubeau/piezometry_stations_complete.yml")

@asset(group_name="hubeau_piezometry_complete", partitions_def=YEARLY_PARTITIONS, deps=[piezometry_stations_complete_reference])
def piezometry_chroniques_tr_complete(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests piezometry real-time chroniques with ALL attributes."""
    partition_date = _get_partition_date_yearly(context)
    return ingest_dlt(context, "configs/hubeau/piezometry_chroniques_tr_complete.yml", partition_date=partition_date)

# ====================================
# ASSETS COMPLETS POUR QUALITÉ COURS D'EAU
# ====================================

@asset(group_name="hubeau_quality_rivers_complete")
def quality_rivers_stations_complete_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests quality rivers stations reference data with ALL attributes using dlt."""
    return ingest_dlt(context, "configs/hubeau/quality_rivers_stations_complete.yml")

@asset(group_name="hubeau_quality_rivers_complete")
def quality_rivers_operations_complete_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests quality rivers operations reference data with ALL attributes using dlt."""
    return ingest_dlt(context, "configs/hubeau/quality_rivers_operations_complete.yml")

@asset(group_name="hubeau_quality_rivers_complete", partitions_def=YEARLY_PARTITIONS, deps=[quality_rivers_stations_complete_reference])
def quality_rivers_analyses_complete(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests quality rivers analyses with ALL attributes."""
    partition_date = _get_partition_date_yearly(context)
    return ingest_dlt(context, "configs/hubeau/quality_rivers_analyses_complete.yml", partition_date=partition_date)

@asset(group_name="hubeau_quality_rivers_complete", partitions_def=YEARLY_PARTITIONS, deps=[quality_rivers_stations_complete_reference])
def quality_rivers_conditions_complete(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests quality rivers environmental conditions with ALL attributes."""
    partition_date = _get_partition_date_yearly(context)
    return ingest_dlt(context, "configs/hubeau/quality_rivers_conditions_complete.yml", partition_date=partition_date)

# ====================================
# ASSETS COMPLETS POUR QUALITÉ NAPPES
# ====================================

@asset(group_name="hubeau_quality_groundwater_complete")
def quality_groundwater_stations_complete_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests quality groundwater stations reference data with ALL attributes using dlt."""
    return ingest_dlt(context, "configs/hubeau/quality_groundwater_stations_complete.yml")

@asset(group_name="hubeau_quality_groundwater_complete", partitions_def=YEARLY_PARTITIONS, deps=[quality_groundwater_stations_complete_reference])
def quality_groundwater_analyses_complete(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests quality groundwater analyses with ALL attributes."""
    partition_date = _get_partition_date_yearly(context)
    return ingest_dlt(context, "configs/hubeau/quality_groundwater_analyses_complete.yml", partition_date=partition_date)

# ====================================
# ASSETS COMPLETS POUR TEMPÉRATURE
# ====================================

@asset(group_name="hubeau_temperature_complete")
def temperature_stations_complete_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests temperature stations reference data with ALL attributes using dlt."""
    return ingest_dlt(context, "configs/hubeau/temperature_stations_complete.yml")

@asset(group_name="hubeau_temperature_complete", partitions_def=YEARLY_PARTITIONS, deps=[temperature_stations_complete_reference])
def temperature_chroniques_complete(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests temperature chroniques with ALL attributes."""
    partition_date = _get_partition_date_yearly(context)
    return ingest_dlt(context, "configs/hubeau/temperature_chroniques_complete.yml", partition_date=partition_date)

# ====================================
# ASSETS COMPLETS POUR ÉCOULEMENT
# ====================================

@asset(group_name="hubeau_ecoulement_complete")
def ecoulement_stations_complete_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests ecoulement stations reference data with ALL attributes using dlt."""
    return ingest_dlt(context, "configs/hubeau/ecoulement_stations_complete.yml")

@asset(group_name="hubeau_ecoulement_complete")
def ecoulement_campagnes_complete_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests ecoulement campaigns reference with ALL attributes using dlt."""
    return ingest_dlt(context, "configs/hubeau/ecoulement_campagnes_complete.yml")

@asset(group_name="hubeau_ecoulement_complete", partitions_def=YEARLY_PARTITIONS, deps=[ecoulement_stations_complete_reference, ecoulement_campagnes_complete_reference])
def ecoulement_observations_complete(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests ecoulement observations with ALL attributes."""
    partition_date = _get_partition_date_yearly(context)
    return ingest_dlt(context, "configs/hubeau/ecoulement_observations_complete.yml", partition_date=partition_date)

# ====================================
# ASSETS COMPLETS POUR HYDROBIOLOGIE
# ====================================

@asset(group_name="hubeau_hydrobio_complete")
def hydrobio_stations_complete_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrobiology stations reference data with ALL attributes using dlt."""
    return ingest_dlt(context, "configs/hubeau/hydrobio_stations_complete.yml")

@asset(group_name="hubeau_hydrobio_complete", partitions_def=YEARLY_PARTITIONS, deps=[hydrobio_stations_complete_reference])
def hydrobio_indices_complete(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrobiology indices with ALL attributes."""
    partition_date = _get_partition_date_yearly(context)
    return ingest_dlt(context, "configs/hubeau/hydrobio_indices_complete.yml", partition_date=partition_date)

@asset(group_name="hubeau_hydrobio_complete", partitions_def=YEARLY_PARTITIONS, deps=[hydrobio_stations_complete_reference])
def hydrobio_taxons_complete(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests hydrobiology taxons with ALL attributes."""
    partition_date = _get_partition_date_yearly(context)
    return ingest_dlt(context, "configs/hubeau/hydrobio_taxons_complete.yml", partition_date=partition_date)

# ====================================
# ASSETS COMPLETS POUR PRÉLÈVEMENTS
# ====================================

@asset(group_name="hubeau_prelevements_complete")
def prelevements_ouvrages_complete_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests prelevements ouvrages reference data with ALL attributes using dlt."""
    return ingest_dlt(context, "configs/hubeau/prelevements_ouvrages_complete.yml")

@asset(group_name="hubeau_prelevements_complete")
def prelevements_points_complete_reference(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests prelevements points reference data with ALL attributes using dlt."""
    return ingest_dlt(context, "configs/hubeau/prelevements_points_complete.yml")

@asset(group_name="hubeau_prelevements_complete", partitions_def=YEARLY_PARTITIONS, deps=[prelevements_ouvrages_complete_reference])
def prelevements_chroniques_complete(context: AssetExecutionContext) -> Dict[str, Any]:
    """Ingests prelevements chroniques with ALL attributes."""
    partition_date = _get_partition_date_yearly(context)
    return ingest_dlt(context, "configs/hubeau/prelevements_chroniques_complete.yml", partition_date=partition_date)

