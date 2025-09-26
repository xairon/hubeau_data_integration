// Neo4j Hub'Eau Pipeline - Données Sandre de base

// ==========================================
// PARAMÈTRES SANDRE ESSENTIELS
// ==========================================

// Paramètres physicochimiques majeurs
CREATE (p1:SandreParametres {
    code: "1340",
    libelle: "Nitrates",
    definition: "Concentration en nitrates dans l'eau",
    formule_chimique: "NO3-",
    cas_number: "14797-55-8",
    unite_defaut: "mg/L",
    fraction_defaut: "Dissoute",
    domaine: "Chimie",
    famille: "Nutriments",
    statut: "Validé",
    date_creation: "1990-01-01",
    date_maj: "2023-01-01",
    seuil_qualite_mg_l: 50.0
});

CREATE (p2:SandreParametres {
    code: "1335",
    libelle: "Atrazine",
    definition: "Herbicide de la famille des triazines",
    formule_chimique: "C8H14ClN5",
    cas_number: "1912-24-9",
    unite_defaut: "µg/L",
    fraction_defaut: "Totale",
    domaine: "Pesticides",
    famille: "Triazines",
    statut: "Validé",
    date_creation: "1995-06-15",
    date_maj: "2023-01-01",
    seuil_qualite_ug_l: 0.1
});

CREATE (p3:SandreParametres {
    code: "1301",
    libelle: "Température de l'eau",
    definition: "Température de l'échantillon d'eau",
    unite_defaut: "°C",
    fraction_defaut: "Non applicable",
    domaine: "Physique",
    famille: "Paramètres physiques",
    statut: "Validé",
    date_creation: "1985-01-01",
    date_maj: "2023-01-01"
});

CREATE (p4:SandreParametres {
    code: "1302",
    libelle: "pH",
    definition: "Potentiel hydrogène - mesure de l'acidité",
    unite_defaut: "unité pH",
    fraction_defaut: "Non applicable",
    domaine: "Physique",
    famille: "Paramètres physiques",
    statut: "Validé",
    date_creation: "1985-01-01",
    date_maj: "2023-01-01"
});

CREATE (p5:SandreParametres {
    code: "1330",
    libelle: "Débit",
    definition: "Débit volumique d'un cours d'eau",
    unite_defaut: "m³/s",
    fraction_defaut: "Non applicable",
    domaine: "Hydrodynamique",
    famille: "Débit",
    statut: "Validé",
    date_creation: "1985-01-01",
    date_maj: "2023-01-01"
});

// ==========================================
// UNITÉS DE MESURE SANDRE
// ==========================================

CREATE (u1:SandreUnites {
    code: "133",
    libelle: "milligramme par litre",
    symbole: "mg/L",
    definition: "Concentration massique en milligrammes par litre",
    facteur_conversion: 1.0,
    unite_si: "kg/m³",
    domaine: "Concentration",
    statut: "Validé"
});

CREATE (u2:SandreUnites {
    code: "146",
    libelle: "microgramme par litre",
    symbole: "µg/L",
    definition: "Concentration massique en microgrammes par litre",
    facteur_conversion: 0.001,
    unite_si: "kg/m³",
    domaine: "Concentration",
    statut: "Validé"
});

CREATE (u3:SandreUnites {
    code: "27",
    libelle: "degré Celsius",
    symbole: "°C",
    definition: "Unité de température",
    facteur_conversion: 1.0,
    unite_si: "K",
    domaine: "Température",
    statut: "Validé"
});

CREATE (u4:SandreUnites {
    code: "18",
    libelle: "unité pH",
    symbole: "unité pH",
    definition: "Échelle logarithmique de mesure de l'acidité",
    facteur_conversion: 1.0,
    unite_si: "sans dimension",
    domaine: "Sans dimension",
    statut: "Validé"
});

CREATE (u5:SandreUnites {
    code: "162",
    libelle: "mètre cube par seconde",
    symbole: "m³/s",
    definition: "Débit volumique en mètres cubes par seconde",
    facteur_conversion: 1.0,
    unite_si: "m³/s",
    domaine: "Débit",
    statut: "Validé"
});

// ==========================================
// MÉTHODES D'ANALYSE SANDRE
// ==========================================

CREATE (m1:SandreMethodes {
    code: "METH001",
    libelle: "Chromatographie ionique",
    description: "Analyse des anions par chromatographie ionique",
    norme: "NF EN ISO 10304-1",
    principe: "Séparation ionique sur résine échangeuse",
    domaine_application: "Anions majeurs (Cl-, SO4--, NO3-)",
    limite_detection: "0.1 mg/L",
    limite_quantification: "0.5 mg/L",
    incertitude_pct: 10,
    statut: "Validé"
});

CREATE (m2:SandreMethodes {
    code: "METH002",
    libelle: "GC-MS",
    description: "Chromatographie gazeuse couplée à la spectrométrie de masse",
    norme: "NF EN ISO 6468",
    principe: "Séparation chromatographique et identification par spectrométrie de masse",
    domaine_application: "Pesticides organiques",
    limite_detection: "0.01 µg/L",
    limite_quantification: "0.05 µg/L",
    incertitude_pct: 20,
    statut: "Validé"
});

CREATE (m3:SandreMethodes {
    code: "METH003",
    libelle: "Thermométrie",
    description: "Mesure directe de la température par sonde",
    norme: "NF EN 27888",
    principe: "Mesure par thermistance ou thermocouple",
    domaine_application: "Température de l'eau",
    limite_detection: "0.1 °C",
    limite_quantification: "0.1 °C",
    incertitude_pct: 2,
    statut: "Validé"
});

CREATE (m4:SandreMethodes {
    code: "METH004",
    libelle: "pH-métrie",
    description: "Mesure électrométrique du pH",
    norme: "NF EN ISO 10523",
    principe: "Mesure potentiométrique avec électrode de verre",
    domaine_application: "pH de l'eau",
    limite_detection: "0.01 unité pH",
    limite_quantification: "0.01 unité pH",
    incertitude_pct: 1,
    statut: "Validé"
});

// ==========================================
// SUPPORTS ET FRACTIONS
// ==========================================

CREATE (s1:SandreSupports {
    code: "3",
    libelle: "Eau",
    description: "Matrice eau brute ou traitée",
    type_matrice: "Liquide",
    conservation: "4°C, obscurité, acidification si nécessaire",
    duree_conservation: "24h à 7 jours selon paramètre",
    statut: "Validé"
});

CREATE (f1:SandreFractions {
    code: "23",
    libelle: "Dissoute",
    description: "Fraction passant au travers d'un filtre de 0,45 µm",
    taille_filtre: "0.45 µm",
    traitement: "Filtration sur membrane",
    statut: "Validé"
});

CREATE (f2:SandreFractions {
    code: "2",
    libelle: "Totale",
    description: "Échantillon non filtré, matières en suspension incluses",
    traitement: "Aucun filtrage",
    statut: "Validé"
});

CREATE (f3:SandreFractions {
    code: "1",
    libelle: "Non applicable",
    description: "Fraction non pertinente pour ce type de paramètre",
    traitement: "Sans objet",
    statut: "Validé"
});

RETURN "Données Sandre de base créées" as status;
