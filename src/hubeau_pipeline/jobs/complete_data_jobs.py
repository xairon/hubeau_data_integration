from dagster import job, define_asset_job

from src.hubeau_pipeline.assets.raw.dlt_assets_complete import (
    # Hydrométrie complète
    hydrometry_stations_complete_reference,
    hydrometry_sites_complete_reference,
    hydrometry_obs_elab_complete,
    
    # Piézométrie complète
    piezometry_stations_complete_reference,
    piezometry_chroniques_tr_complete,
    
    # Qualité cours d'eau complète
    quality_rivers_stations_complete_reference,
    quality_rivers_operations_complete_reference,
    quality_rivers_analyses_complete,
    quality_rivers_conditions_complete,
    
    # Qualité nappes complète
    quality_groundwater_stations_complete_reference,
    quality_groundwater_analyses_complete,
    
    # Température complète
    temperature_stations_complete_reference,
    temperature_chroniques_complete,
    
    # Écoulement complet
    ecoulement_stations_complete_reference,
    ecoulement_campagnes_complete_reference,
    ecoulement_observations_complete,
    
    # Hydrobiologie complète
    hydrobio_stations_complete_reference,
    hydrobio_indices_complete,
    hydrobio_taxons_complete,
    
    # Prélèvements complet
    prelevements_ouvrages_complete_reference,
    prelevements_points_complete_reference,
    prelevements_chroniques_complete,
)

# ====================================
# JOB COMPLET POUR TOUTES LES DONNÉES
# ====================================

complete_data_ingestion_job = define_asset_job(
    name="complete_data_ingestion_job",
    description="Job complet pour ingérer TOUTES les données Hub'Eau avec TOUS les attributs",
    selection=[
        # Référentiels (sans partition)
        hydrometry_stations_complete_reference,
        hydrometry_sites_complete_reference,
        piezometry_stations_complete_reference,
        quality_rivers_stations_complete_reference,
        quality_rivers_operations_complete_reference,
        quality_groundwater_stations_complete_reference,
        temperature_stations_complete_reference,
        ecoulement_stations_complete_reference,
        ecoulement_campagnes_complete_reference,
        hydrobio_stations_complete_reference,
        prelevements_ouvrages_complete_reference,
        prelevements_points_complete_reference,
        
        # Données temporelles (avec partitions)
        hydrometry_obs_elab_complete,
        piezometry_chroniques_tr_complete,
        quality_rivers_analyses_complete,
        quality_rivers_conditions_complete,
        quality_groundwater_analyses_complete,
        temperature_chroniques_complete,
        ecoulement_observations_complete,
        hydrobio_indices_complete,
        hydrobio_taxons_complete,
        prelevements_chroniques_complete,
    ],
    partitions_def=None,  # Ce job sera exécuté pour toutes les partitions
)

# ====================================
# JOBS PAR API
# ====================================

hydrometry_complete_job = define_asset_job(
    name="hydrometry_complete_job",
    description="Job complet pour l'hydrométrie avec tous les attributs",
    selection=[
        hydrometry_stations_complete_reference,
        hydrometry_sites_complete_reference,
        hydrometry_obs_elab_complete,
    ],
)

piezometry_complete_job = define_asset_job(
    name="piezometry_complete_job",
    description="Job complet pour la piézométrie avec tous les attributs",
    selection=[
        piezometry_stations_complete_reference,
        piezometry_chroniques_tr_complete,
    ],
)

quality_rivers_complete_job = define_asset_job(
    name="quality_rivers_complete_job",
    description="Job complet pour la qualité des cours d'eau avec tous les attributs",
    selection=[
        quality_rivers_stations_complete_reference,
        quality_rivers_operations_complete_reference,
        quality_rivers_analyses_complete,
        quality_rivers_conditions_complete,
    ],
)

quality_groundwater_complete_job = define_asset_job(
    name="quality_groundwater_complete_job",
    description="Job complet pour la qualité des nappes avec tous les attributs",
    selection=[
        quality_groundwater_stations_complete_reference,
        quality_groundwater_analyses_complete,
    ],
)

temperature_complete_job = define_asset_job(
    name="temperature_complete_job",
    description="Job complet pour la température avec tous les attributs",
    selection=[
        temperature_stations_complete_reference,
        temperature_chroniques_complete,
    ],
)

ecoulement_complete_job = define_asset_job(
    name="ecoulement_complete_job",
    description="Job complet pour l'écoulement avec tous les attributs",
    selection=[
        ecoulement_stations_complete_reference,
        ecoulement_campagnes_complete_reference,
        ecoulement_observations_complete,
    ],
)

hydrobio_complete_job = define_asset_job(
    name="hydrobio_complete_job",
    description="Job complet pour l'hydrobiologie avec tous les attributs",
    selection=[
        hydrobio_stations_complete_reference,
        hydrobio_indices_complete,
        hydrobio_taxons_complete,
    ],
)

prelevements_complete_job = define_asset_job(
    name="prelevements_complete_job",
    description="Job complet pour les prélèvements avec tous les attributs",
    selection=[
        prelevements_ouvrages_complete_reference,
        prelevements_points_complete_reference,
        prelevements_chroniques_complete,
    ],
)

# ====================================
# JOBS PAR TYPE DE DONNÉES
# ====================================

reference_data_complete_job = define_asset_job(
    name="reference_data_complete_job",
    description="Job pour tous les référentiels avec tous les attributs",
    selection=[
        hydrometry_stations_complete_reference,
        hydrometry_sites_complete_reference,
        piezometry_stations_complete_reference,
        quality_rivers_stations_complete_reference,
        quality_rivers_operations_complete_reference,
        quality_groundwater_stations_complete_reference,
        temperature_stations_complete_reference,
        ecoulement_stations_complete_reference,
        ecoulement_campagnes_complete_reference,
        hydrobio_stations_complete_reference,
        prelevements_ouvrages_complete_reference,
        prelevements_points_complete_reference,
    ],
)

temporal_data_complete_job = define_asset_job(
    name="temporal_data_complete_job",
    description="Job pour toutes les données temporelles avec tous les attributs",
    selection=[
        hydrometry_obs_elab_complete,
        piezometry_chroniques_tr_complete,
        quality_rivers_analyses_complete,
        quality_rivers_conditions_complete,
        quality_groundwater_analyses_complete,
        temperature_chroniques_complete,
        ecoulement_observations_complete,
        hydrobio_indices_complete,
        hydrobio_taxons_complete,
        prelevements_chroniques_complete,
    ],
)

