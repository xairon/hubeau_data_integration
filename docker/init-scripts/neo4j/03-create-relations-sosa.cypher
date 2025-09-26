// Neo4j Hub'Eau Pipeline - Relations Sandre et structures SOSA

// ==========================================
// RELATIONS SANDRE
// ==========================================

// Relations Paramètres → Unités par défaut
MATCH (p1:SandreParametres {code: "1340"}), (u1:SandreUnites {code: "133"})
CREATE (p1)-[:MESURE_AVEC_UNITE {par_defaut: true, facteur_conversion: 1.0}]->(u1);

MATCH (p2:SandreParametres {code: "1335"}), (u2:SandreUnites {code: "146"})
CREATE (p2)-[:MESURE_AVEC_UNITE {par_defaut: true, facteur_conversion: 1.0}]->(u2);

MATCH (p3:SandreParametres {code: "1301"}), (u3:SandreUnites {code: "27"})
CREATE (p3)-[:MESURE_AVEC_UNITE {par_defaut: true, facteur_conversion: 1.0}]->(u3);

MATCH (p4:SandreParametres {code: "1302"}), (u4:SandreUnites {code: "18"})
CREATE (p4)-[:MESURE_AVEC_UNITE {par_defaut: true, facteur_conversion: 1.0}]->(u4);

MATCH (p5:SandreParametres {code: "1330"}), (u5:SandreUnites {code: "162"})
CREATE (p5)-[:MESURE_AVEC_UNITE {par_defaut: true, facteur_conversion: 1.0}]->(u5);

// Relations Paramètres → Méthodes recommandées
MATCH (p1:SandreParametres {code: "1340"}), (m1:SandreMethodes {code: "METH001"})
CREATE (p1)-[:ANALYSE_PAR_METHODE {recommandee: true, norme: "ISO", fiabilite: 95}]->(m1);

MATCH (p2:SandreParametres {code: "1335"}), (m2:SandreMethodes {code: "METH002"})
CREATE (p2)-[:ANALYSE_PAR_METHODE {recommandee: true, norme: "ISO", fiabilite: 90}]->(m2);

MATCH (p3:SandreParametres {code: "1301"}), (m3:SandreMethodes {code: "METH003"})
CREATE (p3)-[:ANALYSE_PAR_METHODE {recommandee: true, norme: "ISO", fiabilite: 98}]->(m3);

MATCH (p4:SandreParametres {code: "1302"}), (m4:SandreMethodes {code: "METH004"})
CREATE (p4)-[:ANALYSE_PAR_METHODE {recommandee: true, norme: "ISO", fiabilite: 97}]->(m4);

// Relations Paramètres → Fractions
MATCH (p1:SandreParametres {code: "1340"}), (f1:SandreFractions {code: "23"})
CREATE (p1)-[:MESURE_SUR_FRACTION {par_defaut: true, obligatoire: true}]->(f1);

MATCH (p2:SandreParametres {code: "1335"}), (f2:SandreFractions {code: "2"})
CREATE (p2)-[:MESURE_SUR_FRACTION {par_defaut: true, obligatoire: true}]->(f2);

MATCH (p3:SandreParametres {code: "1301"}), (f3:SandreFractions {code: "1"})
CREATE (p3)-[:MESURE_SUR_FRACTION {par_defaut: true, obligatoire: false}]->(f3);

MATCH (p4:SandreParametres {code: "1302"}), (f3:SandreFractions {code: "1"})
CREATE (p4)-[:MESURE_SUR_FRACTION {par_defaut: true, obligatoire: false}]->(f3);

MATCH (p5:SandreParametres {code: "1330"}), (f3:SandreFractions {code: "1"})
CREATE (p5)-[:MESURE_SUR_FRACTION {par_defaut: true, obligatoire: false}]->(f3);

// Relations Méthodes → Supports
MATCH (m1:SandreMethodes {code: "METH001"}), (s1:SandreSupports {code: "3"})
CREATE (m1)-[:APPLICABLE_SUR_SUPPORT {domaine_validite: "Eaux naturelles et traitées"}]->(s1);

MATCH (m2:SandreMethodes {code: "METH002"}), (s1:SandreSupports {code: "3"})
CREATE (m2)-[:APPLICABLE_SUR_SUPPORT {domaine_validite: "Eaux naturelles"}]->(s1);

MATCH (m3:SandreMethodes {code: "METH003"}), (s1:SandreSupports {code: "3"})
CREATE (m3)-[:APPLICABLE_SUR_SUPPORT {domaine_validite: "Toutes eaux"}]->(s1);

MATCH (m4:SandreMethodes {code: "METH004"}), (s1:SandreSupports {code: "3"})
CREATE (m4)-[:APPLICABLE_SUR_SUPPORT {domaine_validite: "Toutes eaux"}]->(s1);

// ==========================================
// ONTOLOGIE SOSA - STRUCTURES DE BASE
// ==========================================

// Observable Properties (Propriétés observées - Lien avec Sandre)
CREATE (prop1:ObservableProperty {
    property_id: "niveau_nappe",
    property_name: "Niveau de nappe",
    parameter_sandre: "1301",
    unite: "m",
    description: "Hauteur du niveau d'eau dans un piézomètre par rapport au sol",
    type_mesure: "Physique",
    frequence_recommandee: "Horaire"
});

CREATE (prop2:ObservableProperty {
    property_id: "debit",
    property_name: "Débit",
    parameter_sandre: "1330",
    unite: "m³/s",
    description: "Débit volumique d'un cours d'eau",
    type_mesure: "Hydrodynamique",
    frequence_recommandee: "15 minutes"
});

CREATE (prop3:ObservableProperty {
    property_id: "temperature_eau",
    property_name: "Température de l'eau",
    parameter_sandre: "1301",
    unite: "°C",
    description: "Température de l'eau mesurée in situ",
    type_mesure: "Physique",
    frequence_recommandee: "Horaire"
});

CREATE (prop4:ObservableProperty {
    property_id: "ph",
    property_name: "pH",
    parameter_sandre: "1302",
    unite: "unité pH",
    description: "Potentiel hydrogène de l'eau",
    type_mesure: "Chimique",
    frequence_recommandee: "Quotidienne"
});

CREATE (prop5:ObservableProperty {
    property_id: "nitrates",
    property_name: "Nitrates",
    parameter_sandre: "1340",
    unite: "mg/L",
    description: "Concentration en nitrates dissous",
    type_mesure: "Chimique",
    frequence_recommandee: "Hebdomadaire"
});

// Features of Interest (Entités d'intérêt)
CREATE (foi1:FeatureOfInterest {
    feature_id: "AQUIFER_LOIRE_001",
    feature_name: "Aquifère alluvial Loire moyenne",
    feature_type: "Aquifère",
    description: "Aquifère libre dans les alluvions de la Loire moyenne",
    profondeur_moyenne: 25,
    surface_km2: 450.5,
    permeabilite: "Très perméable",
    porosite_pct: 22.5
});

CREATE (foi2:FeatureOfInterest {
    feature_id: "RIVER_LOIRE_SECTION_001",
    feature_name: "Loire - Section Orléans-Tours",
    feature_type: "Cours d'eau",
    description: "Tronçon de la Loire entre Orléans et Tours",
    longueur_km: 125.2,
    bassin_versant_km2: 38500.0,
    module_m3s: 385.0,
    regime: "Pluvial océanique"
});

CREATE (foi3:FeatureOfInterest {
    feature_id: "WATERSHED_LOIRE_UPSTREAM",
    feature_name: "Bassin versant Loire amont",
    feature_type: "Bassin versant",
    description: "Bassin versant de la Loire en amont d'Orléans",
    surface_km2: 34500.0,
    altitude_max: 1680,
    precipitation_mm_an: 950,
    evapotranspiration_mm_an: 580
});

// Platforms (Stations) exemples
CREATE (platform1:Platform {
    station_id: "BSS00000001",
    station_name: "Piézomètre Loire-Test-01",
    latitude: 47.8515,
    longitude: 1.9310,
    altitude: 95,
    type_station: "Piézométrie",
    gestionnaire: "BRGM",
    date_creation: "2020-01-01",
    statut: "Active",
    frequence_mesure: "1h",
    profondeur_m: 45,
    aquifere: "Alluvions Loire"
});

CREATE (platform2:Platform {
    station_id: "H000000001",
    station_name: "Station Loire à Orléans",
    latitude: 47.9029,
    longitude: 1.9039,
    altitude: 89,
    type_station: "Hydrométrie",
    cours_eau: "Loire",
    gestionnaire: "SCHAPI",
    date_creation: "1985-06-15",
    statut: "Active",
    frequence_mesure: "15min",
    section_controle: "Pont George V"
});

CREATE (platform3:Platform {
    station_id: "QS00000001",
    station_name: "Qualité Loire à Beaugency",
    latitude: 47.7788,
    longitude: 1.6305,
    altitude: 91,
    type_station: "Qualité surface",
    cours_eau: "Loire",
    gestionnaire: "Agence de l'eau Loire-Bretagne",
    date_creation: "1998-03-20",
    statut: "Active",
    frequence_mesure: "Hebdomadaire"
});

// Sensors (Capteurs)
CREATE (sensor1:Sensor {
    sensor_id: "SENSOR_PIEZO_001",
    sensor_type: "Piézomètre automatique",
    fabricant: "OTT HydroMet",
    modele: "OTT PLS-C",
    precision: "±1 cm",
    frequence_mesure: "1h",
    date_installation: "2020-01-15",
    date_derniere_calib: "2024-06-01",
    statut: "Opérationnel",
    maintenance_freq: "Semestrielle"
});

CREATE (sensor2:Sensor {
    sensor_id: "SENSOR_HYDRO_001",
    sensor_type: "Limnimètre radar",
    fabricant: "KROHNE",
    modele: "OPTIFLEX 1300C",
    precision: "±2 mm",
    frequence_mesure: "15min",
    date_installation: "2019-07-01",
    date_derniere_calib: "2024-05-15",
    statut: "Opérationnel",
    maintenance_freq: "Trimestrielle"
});

CREATE (sensor3:Sensor {
    sensor_id: "SENSOR_TEMP_001",
    sensor_type: "Sonde température",
    fabricant: "YSI",
    modele: "EXO2",
    precision: "±0.01 °C",
    frequence_mesure: "1h",
    date_installation: "2019-07-01",
    date_derniere_calib: "2024-04-10",
    statut: "Opérationnel",
    maintenance_freq: "Trimestrielle"
});

// ==========================================
// RELATIONS SOSA
// ==========================================

// Platform hosts Sensor
MATCH (platform1:Platform {station_id: "BSS00000001"}), (sensor1:Sensor {sensor_id: "SENSOR_PIEZO_001"})
CREATE (platform1)-[:hosts {date_installation: "2020-01-15", responsable: "BRGM"}]->(sensor1);

MATCH (platform2:Platform {station_id: "H000000001"}), (sensor2:Sensor {sensor_id: "SENSOR_HYDRO_001"})
CREATE (platform2)-[:hosts {date_installation: "2019-07-01", responsable: "SCHAPI"}]->(sensor2);

MATCH (platform2:Platform {station_id: "H000000001"}), (sensor3:Sensor {sensor_id: "SENSOR_TEMP_001"})
CREATE (platform2)-[:hosts {date_installation: "2019-07-01", responsable: "SCHAPI"}]->(sensor3);

// Sensor observes Property
MATCH (sensor1:Sensor {sensor_id: "SENSOR_PIEZO_001"}), (prop1:ObservableProperty {property_id: "niveau_nappe"})
CREATE (sensor1)-[:observes {precision: "±1 cm", gamme_mesure: "0-50 m"}]->(prop1);

MATCH (sensor2:Sensor {sensor_id: "SENSOR_HYDRO_001"}), (prop2:ObservableProperty {property_id: "debit"})
CREATE (sensor2)-[:observes {precision: "±2%", gamme_mesure: "0-5000 m³/s"}]->(prop2);

MATCH (sensor3:Sensor {sensor_id: "SENSOR_TEMP_001"}), (prop3:ObservableProperty {property_id: "temperature_eau"})
CREATE (sensor3)-[:observes {precision: "±0.01 °C", gamme_mesure: "-5 à +50 °C"}]->(prop3);

// Platform hasFeatureOfInterest
MATCH (platform1:Platform {station_id: "BSS00000001"}), (foi1:FeatureOfInterest {feature_id: "AQUIFER_LOIRE_001"})
CREATE (platform1)-[:hasFeatureOfInterest {relation_type: "surveillance", distance_m: 0}]->(foi1);

MATCH (platform2:Platform {station_id: "H000000001"}), (foi2:FeatureOfInterest {feature_id: "RIVER_LOIRE_SECTION_001"})
CREATE (platform2)-[:hasFeatureOfInterest {relation_type: "mesure_directe", distance_m: 0}]->(foi2);

MATCH (platform3:Platform {station_id: "QS00000001"}), (foi2:FeatureOfInterest {feature_id: "RIVER_LOIRE_SECTION_001"})
CREATE (platform3)-[:hasFeatureOfInterest {relation_type: "surveillance", distance_m: 500}]->(foi2);

// ObservableProperty → Sandre Parameters
MATCH (prop1:ObservableProperty {property_id: "niveau_nappe"}), (p3:SandreParametres {code: "1301"})
CREATE (prop1)-[:maps_to_sandre {correspondance: "directe", fiabilite: 100}]->(p3);

MATCH (prop2:ObservableProperty {property_id: "debit"}), (p5:SandreParametres {code: "1330"})
CREATE (prop2)-[:maps_to_sandre {correspondance: "directe", fiabilite: 100}]->(p5);

MATCH (prop3:ObservableProperty {property_id: "temperature_eau"}), (p3:SandreParametres {code: "1301"})
CREATE (prop3)-[:maps_to_sandre {correspondance: "directe", fiabilite: 100}]->(p3);

MATCH (prop4:ObservableProperty {property_id: "ph"}), (p4:SandreParametres {code: "1302"})
CREATE (prop4)-[:maps_to_sandre {correspondance: "directe", fiabilite: 100}]->(p4);

MATCH (prop5:ObservableProperty {property_id: "nitrates"}), (p1:SandreParametres {code: "1340"})
CREATE (prop5)-[:maps_to_sandre {correspondance: "directe", fiabilite: 100}]->(p1);

// Relations géographiques entre Features of Interest
MATCH (foi1:FeatureOfInterest {feature_id: "AQUIFER_LOIRE_001"}), (foi2:FeatureOfInterest {feature_id: "RIVER_LOIRE_SECTION_001"})
CREATE (foi1)-[:spatially_connected {type_relation: "hydraulique", influence: "forte"}]->(foi2);

MATCH (foi2:FeatureOfInterest {feature_id: "RIVER_LOIRE_SECTION_001"}), (foi3:FeatureOfInterest {feature_id: "WATERSHED_LOIRE_UPSTREAM"})
CREATE (foi2)-[:part_of {type_relation: "hydrographique"}]->(foi3);

RETURN "Relations Sandre et structures SOSA créées avec succès" as status;
