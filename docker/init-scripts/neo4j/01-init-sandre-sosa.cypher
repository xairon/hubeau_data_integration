// Neo4j Hub'Eau Pipeline - Initialisation automatique Sandre + SOSA
// Exécuté automatiquement au premier démarrage du conteneur

// ==========================================
// CONTRAINTES ET INDEX DE BASE
// ==========================================

// Contraintes d'unicité Sandre
CREATE CONSTRAINT sandre_param_code IF NOT EXISTS FOR (p:SandreParametres) REQUIRE p.code IS UNIQUE;
CREATE CONSTRAINT sandre_unite_code IF NOT EXISTS FOR (u:SandreUnites) REQUIRE u.code IS UNIQUE;
CREATE CONSTRAINT sandre_methode_code IF NOT EXISTS FOR (m:SandreMethodes) REQUIRE m.code IS UNIQUE;
CREATE CONSTRAINT sandre_support_code IF NOT EXISTS FOR (s:SandreSupports) REQUIRE s.code IS UNIQUE;
CREATE CONSTRAINT sandre_fraction_code IF NOT EXISTS FOR (f:SandreFractions) REQUIRE f.code IS UNIQUE;

// Contraintes SOSA
CREATE CONSTRAINT sosa_platform_id IF NOT EXISTS FOR (p:Platform) REQUIRE p.station_id IS UNIQUE;
CREATE CONSTRAINT sosa_sensor_id IF NOT EXISTS FOR (s:Sensor) REQUIRE s.sensor_id IS UNIQUE;
CREATE CONSTRAINT sosa_observation_id IF NOT EXISTS FOR (o:Observation) REQUIRE o.observation_id IS UNIQUE;
CREATE CONSTRAINT sosa_property_id IF NOT EXISTS FOR (prop:ObservableProperty) REQUIRE prop.property_id IS UNIQUE;
CREATE CONSTRAINT sosa_foi_id IF NOT EXISTS FOR (foi:FeatureOfInterest) REQUIRE foi.feature_id IS UNIQUE;

// Index de performance
CREATE INDEX sandre_param_libelle IF NOT EXISTS FOR (p:SandreParametres) ON (p.libelle);
CREATE INDEX sandre_param_domaine IF NOT EXISTS FOR (p:SandreParametres) ON (p.domaine);
CREATE INDEX sandre_unite_symbole IF NOT EXISTS FOR (u:SandreUnites) ON (u.symbole);
CREATE INDEX sosa_platform_location IF NOT EXISTS FOR (p:Platform) ON (p.latitude, p.longitude);
CREATE INDEX sosa_platform_type IF NOT EXISTS FOR (p:Platform) ON (p.type_station);
CREATE INDEX sosa_observation_timestamp IF NOT EXISTS FOR (o:Observation) ON (o.timestamp);

RETURN "Contraintes et index créés" as status;
