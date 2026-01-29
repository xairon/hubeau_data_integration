{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['code_site'], 'unique': True},
      {'columns': ['code_departement']},
      {'columns': ['geometry'], 'type': 'gist'}
    ],
    post_hook=["{{ add_primary_key(['code_site']) }}"]
  )
}}

-- Staging model for hydrometry sites
-- Source: bronze.hydrometry_sites_raw
-- Silver layer: copie bronze + typage + déduplication + PostGIS + suppression métadonnées dlt
-- Primary Key: code_site

WITH source AS (
    SELECT * FROM {{ source('staging', 'hydrometry_sites_raw') }}
    WHERE code_site IS NOT NULL
      AND longitude_site IS NOT NULL
      AND latitude_site IS NOT NULL
),

deduplicated AS (
    SELECT DISTINCT ON (code_site)
        {{ cast_silver_numeric('longitude_site') }} AS longitude_site,
        {{ cast_silver_numeric('latitude_site') }} AS latitude_site,
        {{ cast_silver_numeric('altitude_site') }} AS altitude_site,
        {{ cast_silver_text('code_site') }} AS code_site,
        {{ cast_silver_text('code_departement') }} AS code_departement,

        {{ cast_silver_text('libelle_site') }} AS libelle_site,
        {{ cast_silver_text('type_site') }} AS type_site,
        {{ cast_silver_numeric('coordonnee_x_site') }} AS coordonnee_x_site,
        {{ cast_silver_numeric('coordonnee_y_site') }} AS coordonnee_y_site,
        {{ cast_silver_text('code_projection') }} AS code_projection,
        {{ cast_silver_text('code_systeme_alti_site') }} AS code_systeme_alti_site,
        {{ cast_silver_numeric('surface_bv') }} AS surface_bv,
        {{ cast_silver_text('statut_site') }} AS statut_site,
        {{ cast_silver_text('premier_mois_etiage_site') }} AS premier_mois_etiage_site,
        {{ cast_silver_text('premier_mois_annee_hydro_site') }} AS premier_mois_annee_hydro_site,
        {{ cast_silver_text('influence_generale_site') }} AS influence_generale_site,
        {{ cast_silver_text('code_entite_hydro_site') }} AS code_entite_hydro_site,
        {{ cast_silver_text('code_troncon_hydro_site') }} AS code_troncon_hydro_site,
        {{ cast_silver_text('code_commune_site') }} AS code_commune_site,
        {{ cast_silver_text('code_zone_hydro_site') }} AS code_zone_hydro_site,
        {{ cast_silver_text('libelle_commune') }} AS libelle_commune,
        {{ cast_silver_text('code_region') }} AS code_region,
        {{ cast_silver_text('libelle_region') }} AS libelle_region,
        {{ cast_silver_text('code_cours_eau') }} AS code_cours_eau,
        {{ cast_silver_text('libelle_cours_eau') }} AS libelle_cours_eau,
        {{ cast_silver_text('uri_cours_eau') }} AS uri_cours_eau,
        {{ cast_silver_text('grandeur_hydro') }} AS grandeur_hydro,
        {{ cast_silver_timestamp('date_maj_site') }} AS date_maj_site,
        {{ cast_silver_text('libelle_departement') }} AS libelle_departement,
        {{ cast_silver_text('commentaire_influence_generale_site') }} AS commentaire_influence_generale_site,
        {{ cast_silver_text('commentaire_site') }} AS commentaire_site,
        {{ cast_silver_text('type_contexte_loi_stat_site') }} AS type_contexte_loi_stat_site,
        {{ cast_silver_text('type_loi_site') }} AS type_loi_site,

        {{ dbt_utils.star(
            from=source('staging', 'hydrometry_sites_raw'),
            except=[
                "longitude_site", "latitude_site", "altitude_site", "code_site", "code_departement",
                "libelle_site", "type_site", "coordonnee_x_site", "coordonnee_y_site",
                "code_projection", "code_systeme_alti_site", "surface_bv", "statut_site",
                "premier_mois_etiage_site", "premier_mois_annee_hydro_site", "influence_generale_site",
                "code_entite_hydro_site", "code_troncon_hydro_site", "code_commune_site", "code_zone_hydro_site",
                "libelle_commune", "code_region", "libelle_region", "code_cours_eau", "libelle_cours_eau", "uri_cours_eau",
                "grandeur_hydro", "date_maj_site", "libelle_departement",
                "commentaire_influence_generale_site", "commentaire_site", "type_contexte_loi_stat_site", "type_loi_site",
                "_dlt_load_id", "_dlt_id"
            ]
        ) }}
    FROM source
    ORDER BY code_site
)

SELECT 
    *,
    {{ make_point('longitude_site', 'latitude_site') }} AS geometry
FROM deduplicated
