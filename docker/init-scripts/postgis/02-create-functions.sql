-- PostGIS Hub'Eau Pipeline - Fonctions spatiales utilitaires

\c water_geo;

-- ==========================================
-- FONCTIONS SPATIALES UTILITAIRES
-- ==========================================

-- Fonction: Trouver les masses d'eau à un point donné
CREATE OR REPLACE FUNCTION get_masses_eau_at_point(lat DOUBLE PRECISION, lon DOUBLE PRECISION)
RETURNS TABLE(
    id VARCHAR, 
    nom VARCHAR, 
    type_masse_eau VARCHAR,
    bassin_district VARCHAR,
    superficie_km2 DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        me.id,
        me.nom,
        me.type_masse_eau,
        me.bassin_district,
        me.superficie_km2
    FROM bdlisa_masses_eau_souterraine me
    WHERE ST_Contains(me.geom, ST_SetSRID(ST_MakePoint(lon, lat), 4326));
END;
$$ LANGUAGE plpgsql;

-- Fonction: Trouver les formations géologiques dans un rayon
CREATE OR REPLACE FUNCTION get_formations_in_radius(
    lat DOUBLE PRECISION, 
    lon DOUBLE PRECISION, 
    radius_km DOUBLE PRECISION
)
RETURNS TABLE(
    id VARCHAR, 
    formation_name VARCHAR, 
    age_geologique VARCHAR,
    lithologie VARCHAR,
    distance_km DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        f.id,
        f.formation_name,
        f.age_geologique,
        f.lithologie,
        ST_Distance(f.geom::geography, ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography) / 1000 as distance_km
    FROM bdlisa_formations_geologiques f
    WHERE ST_DWithin(f.geom::geography, ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography, radius_km * 1000)
    ORDER BY distance_km;
END;
$$ LANGUAGE plpgsql;

-- Fonction: Trouver les stations dans un périmètre
CREATE OR REPLACE FUNCTION get_stations_in_area(
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    radius_km DOUBLE PRECISION,
    source_filter VARCHAR DEFAULT NULL
)
RETURNS TABLE(
    station_id VARCHAR,
    station_name VARCHAR,
    source_type VARCHAR,
    distance_km DOUBLE PRECISION,
    masse_eau_nom VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.station_id,
        s.station_name,
        s.source_type,
        ST_Distance(s.geom::geography, ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography) / 1000 as distance_km,
        me.nom as masse_eau_nom
    FROM hubeau_stations_geo s
    LEFT JOIN bdlisa_masses_eau_souterraine me ON s.masse_eau_id = me.id
    WHERE ST_DWithin(s.geom::geography, ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography, radius_km * 1000)
    AND (source_filter IS NULL OR s.source_type = source_filter)
    ORDER BY distance_km;
END;
$$ LANGUAGE plpgsql;

-- Fonction: Calculer la superficie d'intersection entre géométries
CREATE OR REPLACE FUNCTION calculate_intersection_area(
    geom1 GEOMETRY,
    geom2 GEOMETRY
)
RETURNS DOUBLE PRECISION AS $$
BEGIN
    RETURN ST_Area(ST_Intersection(geom1::geography, geom2::geography)) / 1000000; -- km²
END;
$$ LANGUAGE plpgsql;

-- Fonction: Géocodage inverse simplifié (commune à partir de coordonnées)
CREATE OR REPLACE FUNCTION reverse_geocode_commune(lat DOUBLE PRECISION, lon DOUBLE PRECISION)
RETURNS TABLE(
    commune_id VARCHAR,
    commune_nom VARCHAR,
    code_insee VARCHAR,
    population INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.id,
        c.nom,
        c.code_insee,
        c.population
    FROM bdlisa_limites_administratives c
    WHERE c.type_limite = 'commune'
    AND ST_Contains(c.geom, ST_SetSRID(ST_MakePoint(lon, lat), 4326))
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- ==========================================
-- VUES MATÉRIALISÉES POUR PERFORMANCE
-- ==========================================

-- Vue matérialisée: Statistiques par bassin district
CREATE MATERIALIZED VIEW stats_by_bassin AS
SELECT 
    bassin_district,
    COUNT(*) as nb_masses_eau,
    SUM(superficie_km2) as superficie_totale_km2,
    SUM(volume_exploitable_mm3) as volume_total_mm3,
    AVG(superficie_km2) as superficie_moyenne_km2,
    ST_Union(geom) as geom_union
FROM bdlisa_masses_eau_souterraine
WHERE bassin_district IS NOT NULL
GROUP BY bassin_district;

-- Index spatial sur la vue matérialisée
CREATE INDEX idx_stats_bassin_geom ON stats_by_bassin USING GIST(geom_union);

-- Vue matérialisée: Formations par âge géologique
CREATE MATERIALIZED VIEW formations_by_age AS
SELECT 
    age_geologique,
    COUNT(*) as nb_formations,
    array_agg(DISTINCT lithologie) as lithologies,
    SUM(ST_Area(geom::geography)) / 1000000 as superficie_totale_km2,
    AVG(epaisseur_moyenne_m) as epaisseur_moyenne,
    AVG(porosite_pct) as porosite_moyenne
FROM bdlisa_formations_geologiques
WHERE age_geologique IS NOT NULL
GROUP BY age_geologique;

-- Vue matérialisée: Densité de stations par km²
CREATE MATERIALIZED VIEW station_density AS
SELECT 
    c.id as commune_id,
    c.nom as commune_nom,
    c.superficie_km2,
    COUNT(s.station_id) as nb_stations,
    CASE 
        WHEN c.superficie_km2 > 0 THEN COUNT(s.station_id)::DOUBLE PRECISION / c.superficie_km2
        ELSE 0
    END as densite_stations_km2,
    array_agg(DISTINCT s.source_type) as types_stations
FROM bdlisa_limites_administratives c
LEFT JOIN hubeau_stations_geo s ON ST_Contains(c.geom, s.geom)
WHERE c.type_limite = 'commune'
GROUP BY c.id, c.nom, c.superficie_km2;

-- ==========================================
-- TRIGGERS POUR MISE À JOUR AUTOMATIQUE
-- ==========================================

-- Trigger pour mise à jour automatique des timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Application des triggers
CREATE TRIGGER update_masses_eau_updated_at BEFORE UPDATE
    ON bdlisa_masses_eau_souterraine FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_formations_updated_at BEFORE UPDATE
    ON bdlisa_formations_geologiques FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_limites_updated_at BEFORE UPDATE
    ON bdlisa_limites_administratives FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_stations_geo_updated_at BEFORE UPDATE
    ON hubeau_stations_geo FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ==========================================
-- PERMISSIONS ET UTILISATEURS
-- ==========================================

-- Utilisateur lecture seule pour les dashboards
CREATE USER IF NOT EXISTS geo_dashboard_user WITH PASSWORD 'GeoDashboard2024!';
GRANT CONNECT ON DATABASE water_geo TO geo_dashboard_user;
GRANT USAGE ON SCHEMA public TO geo_dashboard_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO geo_dashboard_user;
GRANT SELECT ON ALL MATERIALIZED VIEWS IN SCHEMA public TO geo_dashboard_user;

-- Utilisateur analytics géospatial
CREATE USER IF NOT EXISTS geo_analytics_user WITH PASSWORD 'GeoAnalytics2024!';
GRANT CONNECT ON DATABASE water_geo TO geo_analytics_user;
GRANT USAGE ON SCHEMA public TO geo_analytics_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO geo_analytics_user;
GRANT SELECT ON ALL MATERIALIZED VIEWS IN SCHEMA public TO geo_analytics_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO geo_analytics_user;

SELECT 'PostGIS: Fonctions spatiales et vues matérialisées créées' as status;
