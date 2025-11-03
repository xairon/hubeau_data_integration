"""
Assets Hub'Eau - Ingestion PostgreSQL

Pipeline simple d'ingestion des données Hub'Eau vers PostgreSQL avec support multi-mode :
- FULL : Tout l'historique
- YEAR : Une année spécifique
- INCREMENTAL : Derniers N jours
"""

import os
import dlt
import yaml
import time
import tempfile
from pathlib import Path
from datetime import datetime
from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue,
    Config,
    DynamicPartitionsDefinition,
    StaticPartitionsDefinition,
    AssetKey,
)
from pydantic import Field

# Dagster 1.11.0+ renamed FreshnessPolicy to LegacyFreshnessPolicy
try:
    from dagster import FreshnessPolicy
except ImportError:
    from dagster import LegacyFreshnessPolicy as FreshnessPolicy
from typing import Optional, Literal, Dict, Any

from hubeau_pipeline.sources.hubeau_csv_source import hubeau_csv_source, IngestionMode
from hubeau_pipeline.destinations.postgres_optimized_v2 import get_postgres_destination
from hubeau_pipeline.resources import PostgreSQLResource
from hubeau_pipeline.utils.schema_loader import ensure_table_exists
from hubeau_pipeline.schema.hubeau_type_mappings import TABLE_FK_MAPPING


# ============================================================================
# PARTITIONS DÉFINITIONS
# ============================================================================

# Partitions pour les modes (avec années prédéfinies + possibilité d'ajout dynamique)
# Les années peuvent être ajoutées dynamiquement via API ou UI
HUBEAU_PARTITIONS = DynamicPartitionsDefinition(name="hubeau_time_partitions")

# Partitions statiques pour facilité d'usage
MODE_PARTITIONS = StaticPartitionsDefinition([
    "full",           # Tout l'historique
    "incremental",    # Derniers 2 jours
    "2024",          # Année 2024
    "2023",          # Année 2023
    "2022",          # Année 2022
    "2021",          # Année 2021
    "2020",          # Année 2020
])


# ============================================================================
# MAPPING PARAMÈTRES API PAR RESOURCE
# ============================================================================
# Chaque endpoint Hub'Eau a ses propres noms de paramètres pour filtrer par date
# Format: resource_name → (nom_param_min, nom_param_max, utilise_annee_seule)
DATE_PARAM_MAPPING = {
    "ecoulement_observations": ("date_observation_min", "date_observation_max", False),
    "piezometry_chroniques": ("date_debut_mesure", "date_fin_mesure", False),
    "hydrometry_obs_elab": ("date_debut_obs_elab", "date_fin_obs_elab", False),
    "quality_groundwater_analyses": ("date_debut_prelevement", "date_fin_prelevement", False),
    "quality_rivers_analyses": ("date_debut_prelevement", "date_fin_prelevement", False),  # ✅ CORRECTION: date_debut/fin au lieu de _min/_max
    "quality_rivers_conditions": ("date_debut_prelevement", "date_fin_prelevement", False),  # ✅ CORRECTION: date_debut/fin au lieu de _min/_max
    "quality_rivers_operations": ("date_debut_prelevement", "date_fin_prelevement", False),  # ✅ CORRECTION: date_debut/fin au lieu de _min/_max
    "temperature_chroniques": ("date_debut_mesure", "date_fin_mesure", False),  # ✅ CORRECTION: date_debut_mesure au lieu de date_mesure_temp_min
    "hydrobio_indices": ("date_debut_prelevement", "date_fin_prelevement", False),  # ✅ CORRECTION: date_debut/fin au lieu de _min/_max
    "hydrobio_taxons": ("date_debut_prelevement", "date_fin_prelevement", False),  # ✅ CORRECTION: date_debut/fin au lieu de _min/_max
    "prelevements_chroniques": ("annee", None, True),  # Cas spécial: utilise année uniquement
}


# ============================================================================
# ASSET DEPENDENCIES
# ============================================================================
# Mapping resource_name → assets stations dont il dépend
ASSET_DEPENDENCIES = {
    # Piezometry
    "piezometry_chroniques": ["piezometry_stations_csv"],

    # Quality Rivers
    "quality_rivers_analyses": ["quality_rivers_stations_csv"],
    "quality_rivers_conditions": ["quality_rivers_stations_csv"],
    "quality_rivers_operations": ["quality_rivers_stations_csv"],

    # Quality Groundwater
    "quality_groundwater_analyses": ["quality_groundwater_stations_csv"],

    # Hydrometry (dépend de sites ET stations)
    "hydrometry_obs_elab": ["hydrometry_sites_csv", "hydrometry_stations_csv"],
    # Assurer que les stations ne chargent qu'après les sites (FK code_site)
    "hydrometry_stations": ["hydrometry_sites_csv"],

    # Temperature
    "temperature_chroniques": ["temperature_stations_csv"],

    # Hydrobio
    "hydrobio_indices": ["hydrobio_stations_csv"],
    "hydrobio_taxons": ["hydrobio_stations_csv"],

    # Ecoulement (dépend de stations ET campagnes)
    "ecoulement_observations": ["ecoulement_stations_csv", "ecoulement_campagnes_csv"],

    # Prelevements (dépend de ouvrages ET points)
    "prelevements_chroniques": ["prelevements_ouvrages_csv", "prelevements_points_csv"],
    # Assurer que les points sont chargés après les ouvrages (FK code_ouvrage)
    "prelevements_points": ["prelevements_ouvrages_csv"],
}


# ============================================================================
# FRESHNESS POLICIES
# ============================================================================
# Alerte si chroniques/analyses pas mises à jour depuis > 48h
CHRONIQUES_FRESHNESS_POLICY = FreshnessPolicy(
    maximum_lag_minutes=60 * 48,  # 48 heures
    cron_schedule="0 2 * * *",    # Vérifié quotidiennement à 02h00
)


class IngestionConfig(Config):
    """Configuration pour les assets avec mode selectionnable"""
    mode: Literal["full", "year", "incremental"] = Field(
        default="full",
        description="Mode d'ingestion"
    )
    year: Optional[int] = Field(
        default=None,
        description="Annee a ingerer (mode YEAR uniquement)"
    )
    incremental_days: int = Field(
        default=2,
        description="Nombre de jours a recuperer (mode INCREMENTAL)"
    )


def load_yaml_config(resource_name: str) -> Dict:
    """Charge la configuration YAML d'une ressource"""
    config_path = Path(f"configs/hubeau/{resource_name}.yml")
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def count_rows_in_postgres(table_name: str, pg: PostgreSQLResource, schema: str = "hubeau") -> int:
    """
    Compte les lignes dans une table PostgreSQL.

    Utilisé pour obtenir le vrai nombre de lignes chargées,
    car DLT load_info ne retourne pas toujours les métriques correctement.

    Args:
        table_name: Nom de la table
        pg: PostgreSQL resource (injected by Dagster)
        schema: Schéma PostgreSQL (défaut: hubeau)

    Returns:
        Nombre de lignes dans la table, ou 0 si erreur/table inexistante
    """
    try:
        with pg.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
                count = cur.fetchone()[0]
            return count

    except Exception as e:
        # Si erreur (table n'existe pas encore, connexion échouée, etc.)
        return 0


# Fonctions de monitoring simplifiées - plus utilisées avec 32GB RAM
# (gardées pour compatibilité mais peuvent être supprimées)


def create_csv_asset(resource_name: str, supports_date_filter: bool = True, use_station_slicing: bool = False):
    """
    Factory pour creer un asset DLT CSV multi-mode

    Args:
        resource_name: Nom de la ressource (sans suffixe _csv)
        supports_date_filter: True si l'endpoint supporte les filtres date
        use_station_slicing: True pour slicing par station (piezometry_chroniques)
    """

    group_name = resource_name.split('_')[0]
    asset_name = f"{resource_name}_csv"  # Add _csv suffix to asset name

    # ✅ Détecter les dépendances pour cet asset
    asset_deps = []
    if resource_name in ASSET_DEPENDENCIES:
        asset_deps = [AssetKey(dep_name) for dep_name in ASSET_DEPENDENCIES[resource_name]]

    @asset(
        name=asset_name,
        group_name=group_name,
        compute_kind="dlt",
        op_tags={"format": "csv", "source": "hubeau"},
        partitions_def=MODE_PARTITIONS if supports_date_filter else None,  # Partitions seulement si filtre date supporté
        deps=asset_deps,  # ← Dépendances vers assets stations
        # NOTE: freshness_policy retiré - incompatible avec Dagster 1.11.0+
        # TODO: Migrer vers AutomationCondition + freshness_checks
        metadata={
            "partition_type": "time_based" if supports_date_filter else "none",
            "supports_incremental": supports_date_filter,
            "description": f"Ingestion Hub'Eau pour {resource_name}",
            "depends_on": ASSET_DEPENDENCIES.get(resource_name, []),  # ← Metadata dependencies
            "freshness_check": "48h" if supports_date_filter else "none"  # ← Metadata freshness (info only)
        }
    )
    def csv_asset(
        context: AssetExecutionContext,
        config: IngestionConfig
    ) -> Output:
        """
        Asset DLT CSV avec modes :
        - FULL : Tout l'historique
        - YEAR : Une annee specifique
        - INCREMENTAL : Derniers N jours
        """

        # ============================================================================
        # 🚨 DIAGNOSTIC PARTITION - LOGS ULTRA VERBEUX
        # ============================================================================
        context.log.info("=" * 80)
        context.log.info(f"🚨🚨🚨 DEMARRAGE ASSET: {resource_name}")
        context.log.info(f"🚨 Config initial - mode: {config.mode}, year: {config.year}, incremental_days: {config.incremental_days}")
        context.log.info(f"🚨 Context.has_partition_key: {context.has_partition_key}")
        context.log.info(f"🚨 Context.partition_key (raw): {getattr(context, 'partition_key', 'ATTRIBUTE NOT FOUND')}")
        context.log.info(f"🚨 Context type: {type(context)}")
        context.log.info(f"🚨 Context.run_config: {context.run_config}")

        if context.has_partition_key:
            partition_key_value = context.partition_key
            context.log.info(f"✅ PARTITION DETECTEE: '{partition_key_value}' (type: {type(partition_key_value)})")
        else:
            context.log.warning(f"⚠️⚠️⚠️ AUCUNE PARTITION DETECTEE!")
            context.log.warning(f"⚠️⚠️⚠️ Mode par défaut: {config.mode} - TOUTES les données historiques seront chargées!")
        context.log.info("=" * 80)

        # Étape 1: S'assurer que la table existe (créer via SQL si nécessaire)
        table_name = resource_name
        conn_params = {
            "host": os.getenv("PG_HOST", "postgres"),
            "port": int(os.getenv("PG_PORT", "5432")),
            "database": os.getenv("PG_DB", "postgres"),
            "user": os.getenv("PG_USER", "postgres"),
            "password": os.getenv("PG_PASSWORD", ""),
        }

        try:
            ensure_table_exists(table_name, conn_params)
            context.log.info(f"✅ Table hubeau.{table_name} prête")
        except RuntimeError as e:
            context.log.error(f"❌ Impossible de créer la table: {e}")
            raise

        # ✨ Détecter le mode depuis la partition si disponible
        # NOTE: Config est frozen (Pydantic), on doit créer un NOUVEL objet au lieu de muter
        context.log.info("=" * 80)
        context.log.info(f"🚨 CONVERSION PARTITION → CONFIG")
        context.log.info(f"🚨 Config AVANT conversion - mode: {config.mode}, year: {config.year}")

        if context.has_partition_key:
            partition = context.partition_key
            context.log.info(f"✅ Partition détectée: '{partition}'")
            context.log.info(f"🚨 Conversion de partition '{partition}' en IngestionConfig...")

            if partition == "full":
                context.log.info(f"🚨 Branche 'full' activée")
                config = IngestionConfig(
                    mode="full",
                    year=None,
                    incremental_days=config.incremental_days
                )
                context.log.info(f"🚨 Config créé: mode={config.mode}, year={config.year}")
            elif partition == "incremental":
                context.log.info(f"🚨 Branche 'incremental' activée")
                config = IngestionConfig(
                    mode="incremental",
                    year=None,
                    incremental_days=config.incremental_days
                )
                context.log.info(f"🚨 Config créé: mode={config.mode}, year={config.year}")
            else:
                # C'est une année (2024, 2023, etc.)
                context.log.info(f"🚨 Branche 'year' activée - Tentative de conversion '{partition}' en int")
                try:
                    year_value = int(partition)
                    context.log.info(f"🚨 Conversion réussie: year_value={year_value}")
                    config = IngestionConfig(
                        mode="year",
                        year=year_value,
                        incremental_days=config.incremental_days
                    )
                    context.log.info(f"✅ Config créé: mode={config.mode}, year={config.year}")
                except ValueError as ve:
                    context.log.error(f"❌ Partition invalide: {partition} (attendu: full, incremental, ou YYYY)")
                    context.log.error(f"❌ ValueError: {ve}")
                    raise ValueError(f"Partition invalide: {partition}")
        else:
            context.log.warning(f"⚠️  Aucune partition - config par défaut utilisé: mode={config.mode}, year={config.year}")

        context.log.info(f"🚨 Config APRES conversion - mode: {config.mode}, year: {config.year}")
        context.log.info("=" * 80)

        # Validation
        if config.mode == "year" and not config.year:
            raise ValueError("Mode YEAR necessite le parametre 'year'")

        # Si resource ne supporte pas les filtres date, forcer mode FULL
        if not supports_date_filter and config.mode in ["year", "incremental"]:
            context.log.warning(
                f"{resource_name} ne supporte pas les filtres date. "
                f"Passage en mode FULL."
            )
            config = IngestionConfig(
                mode="full",
                year=None,
                incremental_days=config.incremental_days
            )

        # Charger config YAML
        yaml_config = load_yaml_config(resource_name)

        # Logs
        context.log.info(f"🚀 Ingestion: {resource_name}")
        context.log.info(f"   Mode: {config.mode}")
        if config.mode == "year":
            context.log.info(f"   Annee: {config.year}")
        elif config.mode == "incremental":
            context.log.info(f"   Derniers {config.incremental_days} jours")

        # Use EXACT same PostgreSQL destination as old JSON system
        postgres_config = {
            "dataset_name": os.getenv("HUBEAU_SCHEMA", "hubeau")
        }
        destination = get_postgres_destination(postgres_config)

        # Use temp directory for pipelines like old system
        pipelines_dir = os.path.join(tempfile.gettempdir(), "dlt_pipelines")
        os.makedirs(pipelines_dir, exist_ok=True)

        # Pipeline DLT using same method as JSON assets
        pipeline = dlt.pipeline(
            pipeline_name=f"hubeau_{resource_name}_csv_{config.mode}",
            destination=destination,
            dataset_name=postgres_config["dataset_name"],
            pipelines_dir=pipelines_dir,
            full_refresh=False
        )

        # Préparer filtre FK si applicable (éviter orphelins)
        parent_keys: Optional[set] = None
        parent_table: Optional[str] = None  # Définir au niveau global pour scope dans boucle
        child_col: Optional[str] = None      # Définir au niveau global pour scope dans boucle
        fk_list = TABLE_FK_MAPPING.get(resource_name)

        context.log.info(f"🔍🔍🔍 DEBUG FK MAPPING: resource_name={resource_name}, fk_list={fk_list}")

        # TABLE_FK_MAPPING format: Dict[str, List[Tuple[child_col, parent_table, parent_col]]]
        # For now, we only support filtering on the FIRST FK (multiple FK filtering = future enhancement)
        if fk_list is not None and len(fk_list) > 0:
            import psycopg2

            context.log.info(f"🔍🔍🔍 DEBUG: FK list found, loading parent keys...")

            # Extract first FK tuple
            child_col, parent_table, parent_col = fk_list[0]

            context.log.info(f"🔍🔍🔍 DEBUG: FK tuple unpacked: child_col={child_col}, parent_table={parent_table}, parent_col={parent_col}")

            context.log.info(
                f"🔎 FK filter activé: {table_name}.{child_col} → {parent_table}.{parent_col}"
            )
            try:
                _conn = psycopg2.connect(**conn_params)
                _cur = _conn.cursor()
                _cur.execute(
                    f"SELECT {parent_col} FROM hubeau.{parent_table}"
                )
                parent_keys = {row[0] for row in _cur.fetchall()}
                _cur.close()
                _conn.close()
                context.log.info(
                    f"🔎 {len(parent_keys)} clés parentes chargées pour {parent_table}.{parent_col}"
                )
                context.log.info(f"🔍🔍🔍 DEBUG: parent_keys type={type(parent_keys)}, len={len(parent_keys)}, sample={list(parent_keys)[:5] if parent_keys else []}")
            except Exception as fk_e:
                context.log.error(
                    f"❌ ERREUR CRITIQUE: Impossible de charger les clés parentes ({parent_table}.{parent_col}): {fk_e}"
                )
                context.log.error(f"   Type de fk_list: {type(fk_list)}, contenu: {fk_list}")
                # Re-raise to fail fast instead of silently continuing
                raise RuntimeError(
                    f"Filtrage FK requis pour {table_name} mais échec du chargement des clés parentes. "
                    f"Table parente: {parent_table}. Erreur: {fk_e}"
                )

        # ✅ BYPASS DLT - Use direct pagination to get actual lists
        # Import our new direct iterator function
        from hubeau_pipeline.sources.hubeau_csv_source import get_raw_data_iterator

        # Build config dict for the raw iterator
        config_dict = {
            'resource': yaml_config['resource'],
            'extraction': yaml_config['extraction'],
            'performance': yaml_config['performance'],
            'pagination': yaml_config.get('pagination', {})
        }

        # Add mode-specific parameters to extraction config
        context.log.info("=" * 80)
        context.log.info(f"🚨 AJOUT PARAMETRES DATE")
        context.log.info(f"🚨 Evaluation: config.mode={config.mode}, config.year={config.year}")
        context.log.info(f"🚨 Condition (config.mode == 'year' and config.year): {config.mode == 'year' and config.year}")

        if config.mode == "year" and config.year:
            context.log.info(f"✅ ENTREE DANS BLOC YEAR!")
            context.log.info(f"🚨 resource_name={resource_name}")
            context.log.info(f"🚨 DATE_PARAM_MAPPING.keys()={list(DATE_PARAM_MAPPING.keys())}")
            context.log.info(f"🚨 resource_name in DATE_PARAM_MAPPING: {resource_name in DATE_PARAM_MAPPING}")

            # ✅ Utiliser le mapping correct pour chaque resource
            if resource_name in DATE_PARAM_MAPPING:
                context.log.info(f"✅ Resource trouvée dans DATE_PARAM_MAPPING!")
                param_min, param_max, use_year_only = DATE_PARAM_MAPPING[resource_name]
                context.log.info(f"🚨 Params récupérés: param_min={param_min}, param_max={param_max}, use_year_only={use_year_only}")

                if use_year_only:
                    context.log.info(f"🚨 Branche use_year_only activée")
                    # Cas spécial: prelevements_chroniques utilise "annee" directement
                    config_dict['extraction']['default_params'] = {
                        **config_dict['extraction'].get('default_params', {}),
                        param_min: config.year
                    }
                    context.log.info(f"🗓️  Filtres API: {param_min}={config.year}")
                else:
                    context.log.info(f"🚨 Branche date range activée")
                    # Cas général: date_min et date_max
                    config_dict['extraction']['default_params'] = {
                        **config_dict['extraction'].get('default_params', {}),
                        param_min: f"{config.year}-01-01",
                        param_max: f"{config.year}-12-31"
                    }
                    context.log.info(f"🗓️  Filtres API: {param_min}={config.year}-01-01, {param_max}={config.year}-12-31")

                # 🚨 LOG CRITIQUE - Afficher les default_params FINAUX
                context.log.info(f"✅ PARAMETRES DATE AJOUTES AVEC SUCCES!")
                context.log.info(f"🚨 default_params FINAUX: {config_dict['extraction']['default_params']}")
            else:
                context.log.error(f"❌ {resource_name} PAS TROUVE dans DATE_PARAM_MAPPING!")
                context.log.warning(f"⚠️  {resource_name} n'a pas de mapping de paramètres date défini!")
        else:
            # 🚨 Warning si mode n'est PAS year
            context.log.warning(f"❌ BLOC YEAR NON EXECUTE!")
            context.log.warning(f"🚨 ⚠️  Mode '{config.mode}' (year={config.year}) → AUCUN FILTRE DATE!")
            context.log.warning(f"🚨 ⚠️  Toutes les données historiques seront chargées!")

        context.log.info("=" * 80)

        if config.mode == "incremental":
            # For INCREMENTAL mode, add date filters for last N days
            from datetime import datetime, timedelta
            today = datetime.now()
            start_date = today - timedelta(days=config.incremental_days)

            if resource_name in DATE_PARAM_MAPPING:
                param_min, param_max, use_year_only = DATE_PARAM_MAPPING[resource_name]

                if use_year_only:
                    # Cas spécial: prelevements_chroniques - utiliser année courante
                    config_dict['extraction']['default_params'] = {
                        **config_dict['extraction'].get('default_params', {}),
                        param_min: today.year
                    }
                    context.log.info(f"🗓️  Filtres API: {param_min}={today.year}")
                else:
                    config_dict['extraction']['default_params'] = {
                        **config_dict['extraction'].get('default_params', {}),
                        param_min: start_date.strftime("%Y-%m-%d"),
                        param_max: today.strftime("%Y-%m-%d")
                    }
                    context.log.info(f"🗓️  Filtres API: {param_min}={start_date.strftime('%Y-%m-%d')}, {param_max}={today.strftime('%Y-%m-%d')}")
            else:
                context.log.warning(f"⚠️  {resource_name} n'a pas de mapping de paramètres date défini!")

        # Add station slicing configuration if needed
        if use_station_slicing:
            config_dict['pagination']['station_field'] = 'code_bss'
            if 'piezometry' in resource_name:
                config_dict['pagination']['stations_endpoint'] = '/stations.csv'

        # ✅ SIMPLIFIÉ: Pas de micro-batching - pages complètes directement en base
        # L'API retourne des pages de ~5k records, on les charge directement
        start_time = time.time()

        try:
            # Import de notre custom destination optimisée avec COPY PostgreSQL natif
            from hubeau_pipeline.destinations.postgres_optimized_v2 import postgres_bulk_destination_v2 as postgres_bulk_destination

            table_name = yaml_config['resource']['name']
            primary_keys = yaml_config['resource'].get('primary_key', [])
            if isinstance(primary_keys, str):
                primary_keys = [primary_keys]

            # ✅ Déterminer write_disposition:
            # - FULL: replace (TRUNCATE + INSERT) - Efface tout et recommence
            # - YEAR/INCREMENTAL: merge (UPSERT via PK) - Met à jour sans doublons
            write_disposition = "replace" if config.mode == "full" else "merge"

            context.log.info(f"📥 Streaming pages complètes (~5k records/page, disposition={write_disposition})...")

            total_records = 0
            page_count = 0
            is_first_write = True  # Track first write for TRUNCATE (si replace)

            # ✅ Get raw data iterator - bypasses DLT wrapper
            data_iterator = get_raw_data_iterator(config_dict)

            # ✅ SIMPLE: Itérer sur les pages de l'API (~5k records)
            for page_records in data_iterator:
                page_count += 1

                # Now page_records should be a list of dicts as expected!
                # No need to validate type anymore since we control the iterator
                if len(page_records) == 0:
                    continue

                # ✅ Write disposition pour cette page:
                # - Mode "replace": première page fait TRUNCATE+INSERT, suivantes font MERGE (évite doublons API)
                # - Mode "merge": TOUTES les pages font MERGE (évite doublons)
                if write_disposition == "replace":
                    batch_write_disposition = "replace" if is_first_write else "merge"
                    is_first_write = False
                else:
                    batch_write_disposition = write_disposition

                # DEBUG: Log avant filtrage
                context.log.info(f"🔍 DEBUG AVANT FILTRAGE PK: primary_keys={primary_keys}, len(page_records)={len(page_records)}")
                if len(page_records) > 0:
                    context.log.info(f"🔍 DEBUG PREMIER RECORD: {page_records[0]}")

                # ✅ ÉTAPE 0: Normaliser les clés AVANT filtrage (case-insensitive)
                page_records = [
                    {key.lower(): value for key, value in record.items()}
                    for record in page_records
                ]

                # ============================================================================
                # 🚨 MODIFICATION 3: Filtre NULL amélioré dans PRIMARY KEY
                # ============================================================================
                # Étape 1: Filtrer les records avec NULL dans les colonnes PRIMARY KEY
                if primary_keys:
                    # Normaliser aussi les primary_keys pour matching
                    primary_keys_lower = [pk.lower() for pk in primary_keys]

                    before_pk_filter = len(page_records)
                    page_records = [
                        r for r in page_records
                        if all(
                            r.get(pk) is not None
                            and r.get(pk) != ''
                            and str(r.get(pk)).strip().lower() not in ('null', 'none', 'nan', 'n/a')
                            for pk in primary_keys_lower
                        )
                    ]
                    dropped_pk = before_pk_filter - len(page_records)
                    if dropped_pk > 0:
                        context.log.warning(
                            f"🧹 Filtrage PK: {dropped_pk}/{before_pk_filter} enregistrements ignorés "
                            f"(valeur NULL/vide dans PRIMARY KEY: {primary_keys_lower})"
                        )

                # Étape 2: Filtrer lignes orphelines si FK activée
                context.log.info(f"🔍🔍🔍 DEBUG AVANT FILTRAGE FK: parent_keys is not None={parent_keys is not None}, fk_list={fk_list}, len(page_records)={len(page_records)}")
                if parent_keys is not None and fk_list is not None and len(fk_list) > 0:
                    context.log.info(f"🔍🔍🔍 DEBUG: ENTERING FK FILTERING BLOCK")
                    # Note: child_col et parent_table déjà définis au niveau global
                    if child_col is None or parent_table is None:
                        context.log.error(f"❌ child_col ou parent_table est None! child_col={child_col}, parent_table={parent_table}")
                    before = len(page_records)
                    page_records = [
                        r for r in page_records
                        if (r.get(child_col) is None) or (r.get(child_col) in parent_keys)
                    ]
                    dropped = before - len(page_records)
                    if dropped > 0:
                        context.log.warning(
                            f"🧹 Filtrage FK: {dropped}/{before} enregistrements ignorés (clé parente {child_col} absente dans {parent_table})"
                        )
                    else:
                        context.log.info(f"✅ Tous les {before} enregistrements ont une clé parente valide")
                    context.log.info(f"🔍🔍🔍 DEBUG: FK filtering completed successfully, final len(page_records)={len(page_records)}")
                else:
                    context.log.warning(f"🔍🔍🔍 DEBUG: FK FILTERING SKIPPED! Conditions: parent_keys is None={parent_keys is None}, fk_list is None={fk_list is None}, len(fk_list)={len(fk_list) if fk_list else 0}")

                # Charger page complète en base
                load_start = time.time()
                context.log.info(f"🔍🔍🔍 DEBUG: About to call load_batch: len(page_records)={len(page_records)}, disposition={batch_write_disposition}")
                context.log.info(f"🔍🔍🔍 CALLING load_batch with disposition={batch_write_disposition}, page={page_count}")
                postgres_bulk_destination.load_batch(
                    table_name=table_name,
                    data=page_records,
                    write_disposition=batch_write_disposition,
                    primary_keys=primary_keys if primary_keys else None,
                    column_mappings=None
                )
                load_duration = time.time() - load_start

                total_records += len(page_records)
                throughput = len(page_records) / load_duration if load_duration > 0 else 0

                # Log progression
                context.log.info(
                    f"📥 Page {page_count}: {len(page_records):,} records en {load_duration:.2f}s "
                    f"({throughput:.0f} rec/s, total: {total_records:,})"
                )

            elapsed = time.time() - start_time

            # Log résultats finaux
            if total_records > 0:
                global_throughput = total_records / elapsed if elapsed > 0 else 0
                context.log.info(f"✅ Extraction et chargement terminés en {elapsed:.2f}s")
                context.log.info(f"📊 Total: {total_records:,} records en {page_count} pages")
                context.log.info(f"⚡ Performance globale: {global_throughput:.0f} records/seconde")
            else:
                context.log.warning(f"⚠️  Table {table_name} vide après ingestion ! Vérifier: API Hub'Eau, filtres date, ou réseau.")

            return Output(
                value={"records": total_records, "pages": page_count},
                metadata={
                    "mode": MetadataValue.text(config.mode),
                    "year": MetadataValue.int(config.year) if config.year else None,
                    "incremental_days": MetadataValue.int(config.incremental_days) if config.mode == "incremental" else None,
                    "rows_loaded": MetadataValue.int(total_records),
                    "page_count": MetadataValue.int(page_count),
                    "duration_seconds": MetadataValue.float(round(elapsed, 2)),
                    "throughput_rows_per_sec": MetadataValue.float(round(total_records/elapsed, 2)) if elapsed > 0 else None
                }
            )

        except Exception as e:
            context.log.error(f"❌ Erreur: {e}")
            raise

    return csv_asset


# ========================================
# GENERATION DES ASSETS
# ========================================

# Tables avec support filtre date (chroniques, analyses, observations)
DATE_FILTERABLE_RESOURCES = [
    ("piezometry_chroniques", True),  # (nom, use_station_slicing)
    ("quality_groundwater_analyses", False),
    ("quality_rivers_analyses", False),
    ("quality_rivers_conditions", False),
    ("quality_rivers_operations", False),
    ("temperature_chroniques", False),
    ("hydrometry_obs_elab", False),
    ("hydrobio_indices", False),
    ("hydrobio_taxons", False),
    ("ecoulement_observations", False),
    ("prelevements_chroniques", False)
]

# Tables referentiels (stations, sans filtre date)
REFERENCE_RESOURCES = [
    "piezometry_stations",
    "quality_groundwater_stations",
    "quality_rivers_stations",
    "temperature_stations",
    "hydrometry_sites",
    "hydrometry_stations",
    "hydrobio_stations",
    "ecoulement_stations",
    "ecoulement_campagnes",
    "prelevements_points",
    "prelevements_ouvrages"
]

# Creer assets avec support date
for resource, use_slicing in DATE_FILTERABLE_RESOURCES:
    globals()[f"{resource}_csv"] = create_csv_asset(
        resource,
        supports_date_filter=True,
        use_station_slicing=use_slicing
    )

# Creer assets sans support date (referentiels)
for resource in REFERENCE_RESOURCES:
    globals()[f"{resource}_csv"] = create_csv_asset(
        resource,
        supports_date_filter=False
    )
