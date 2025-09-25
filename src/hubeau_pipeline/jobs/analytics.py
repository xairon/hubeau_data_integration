"""
Jobs d'analyse - Gold
"""

from dagster import define_asset_job, AssetSelection

# Job Analytics Production (calculs réels)
analytics_production_job = define_asset_job(
    name="analytics_production_job",
    description="🔗 Analytics Production: SOSA + Analyses basées sur données réelles",
    selection=AssetSelection.keys(
        "sosa_ontology_production",
        "integrated_analytics_production"
    )
)