# 📊 Fréquences de Mise à Jour des APIs Hub'Eau

## 🎯 Résumé Exécutif

Ce document compile les **fréquences réelles de mise à jour** des données pour chaque API Hub'Eau, basé sur :
- 📚 Documentation officielle Hub'Eau
- 🧪 Tests empiriques de l'API (septembre 2025)
- 📖 Bibliothèque de référence [cl-hubeau](https://tgrandje.github.io/cl-hubeau/)
- 🌐 [data.gouv.fr - Hub'Eau](https://www.data.gouv.fr/dataservices/)

---

## 🌊 API Hydrométrie

**URL**: https://hubeau.eaufrance.fr/api/v2/hydrometrie

### Endpoints et Fréquences

| Endpoint | Type | Fréquence Mise à Jour | Accès Temporel | Source |
|----------|------|----------------------|----------------|---------|
| `/referentiel/stations` | Référentiel | Mensuelle | Tout l'historique | Hub'Eau |
| `/observations_tr` | Temps réel | **2 minutes** | **30 derniers jours** ⚠️ | [cl-hubeau](https://tgrandje.github.io/cl-hubeau/hydrometry/) |
| `/obs_elab` | Élaborées | Quotidienne | 30 derniers jours ⚠️ | Hub'Eau |

**⚠️ RESTRICTION CRITIQUE** :
> **"Seules les données du dernier mois glissant sont rendues accessibles"** - cl-hubeau
> 
> Erreur API : `"date can't be < 1 month from now"` (testé empiriquement)

**Partitions recommandées** : **30 derniers jours seulement**

---

## 🏔️ API Piézométrie

**URL**: https://hubeau.eaufrance.fr/api/v1/niveaux_nappes

### Endpoints et Fréquences

| Endpoint | Type | Fréquence Mise à Jour | Accès Temporel | Source |
|----------|------|----------------------|----------------|---------|
| `/stations` | Référentiel | Mensuelle | Tout l'historique | Hub'Eau |
| `/chroniques_tr` | Temps réel | **Horaire** (télétransmission) | Derniers jours | DATA_SOURCES_COMPLETE.md |
| `/chroniques` | Historique | **Quotidienne** | Depuis ~2020 | Tests empiriques (4.9M mesures) |

**Tests empiriques** :
- ✅ 2024-09-15 : 372 mesures (quotidiennes)
- ✅ Historique disponible depuis 2020

**Partitions recommandées** : **Quotidiennes depuis 2020**

---

## 🌡️ API Température

**URL**: https://hubeau.eaufrance.fr/api/v1/temperature

### Endpoints et Fréquences

| Endpoint | Type | Fréquence Mise à Jour | Accès Temporel | Source |
|----------|------|----------------------|----------------|---------|
| `/station` | Référentiel | Mensuelle | Tout l'historique | Hub'Eau |
| `/chronique` | Mesures | **Sporadique** (horaire quand actif) | Depuis 2000-2008 | Tests empiriques (49M mesures) |

**Tests empiriques** :
- ✅ Année 2024 : 119,479 mesures
- ⚠️ Date 2024-09-15 : 3,717 mesures
- ❌ Date 2025-09-29 : 0 mesures
- 📊 Distribution : **Très sporadique** (pas toutes les stations tous les jours)

**Nature** : Mesures **horaires** mais **ponctuelles** (stations souvent inactives)

**Partitions recommandées** : **Quotidiennes** (accepter 0 pour beaucoup de jours)

---

## 🐟 API Hydrobiologie

**URL**: https://hubeau.eaufrance.fr/api/v1/hydrobio

### Endpoints et Fréquences

| Endpoint | Type | Fréquence Mise à Jour | Accès Temporel | Source |
|----------|------|----------------------|----------------|---------|
| `/stations_hydrobio` | Référentiel | Mensuelle | Tout l'historique | Hub'Eau |
| `/indices` | Indices biologiques | **Saisonnière** (3-4 campagnes/an) | Depuis 1971 | Tests empiriques (1.08M indices) |
| `/taxons` | Taxons identifiés | **Saisonnière** (3-4 campagnes/an) | Depuis 1971 | Tests empiriques |

**Tests empiriques** :
- ✅ 2024-09-15 : 3,895 indices (29% des stations)
- ❌ 2025-09-29 : 0 indices
- 📊 Historique : 629k indices (2010-2020), 317k indices (2020-2025)

**Nature** : **Campagnes saisonnières** (printemps, été, automne)
- Printemps (mars-mai) : Forte activité
- Été (juin-août) : Forte activité  
- Automne (sept-nov) : Activité modérée
- Hiver : Très peu de campagnes

**Partitions recommandées** : **Quotidiennes** (accepter 70% de jours vides)

---

## 🧪 API Qualité des Nappes

**URL**: https://hubeau.eaufrance.fr/api/v1/qualite_nappes

### Endpoints et Fréquences

| Endpoint | Type | Fréquence Mise à Jour | Accès Temporel | Source |
|----------|------|----------------------|----------------|---------|
| `/stations` | Référentiel | Mensuelle | Tout l'historique | Hub'Eau |
| `/analyses` | Analyses physico-chimiques | **Trimestrielle à Semestrielle** | Depuis ~2000 | Tests empiriques |

**Tests empiriques** :
- ✅ Année 2024 : 8,534,797 analyses
- ❌ Date 2024-09-15 : 0 analyses
- ❌ Date 2025-09-29 : 0 analyses
- 📊 Moyenne : 162 analyses/station/an = 0.44/jour/station

**Nature** : **Campagnes réglementaires** (DCE - Directive Cadre sur l'Eau)
- Analyses coûteuses en laboratoire
- Programmation trimestrielle/semestrielle
- Pas de suivi quotidien

**Partitions recommandées** : **Quotidiennes** (la plupart des jours = 0)

---

## 🌊 API Qualité des Cours d'Eau

**URL**: https://hubeau.eaufrance.fr/api/v2/qualite_rivieres

### Endpoints et Fréquences

| Endpoint | Type | Fréquence Mise à Jour | Accès Temporel | Source |
|----------|------|----------------------|----------------|---------|
| `/station_pc` | Référentiel | Mensuelle | Tout l'historique | Hub'Eau |
| `/operation_pc` | Opérations de prélèvement | **Variable** (campagnes) | Depuis ~2000 | data.gouv.fr |
| `/analyse_pc` | Analyses | **Variable** (résultats labo) | Depuis ~2000 | data.gouv.fr |

**Volume total** : >200 millions d'analyses sur 20,000+ stations ([source](https://www.data.gouv.fr/dataservices/hubeau-qualite-des-cours-deau))

**Nature** : Base **Naïades** synchronisée en continu

**Partitions recommandées** : **Quotidiennes**

---

## 🌊 API ONDE (Écoulement)

**URL**: https://hubeau.eaufrance.fr/api/v1/ecoulement

### Endpoints et Fréquences

| Endpoint | Type | Fréquence Mise à Jour | Accès Temporel | Source |
|----------|------|----------------------|----------------|---------|
| `/stations` | Référentiel | Mensuelle | Tout l'historique | Hub'Eau |
| `/campagnes` | Campagnes | **Mensuelle** (mai à septembre) | Depuis ~2012 | Tests empiriques |
| `/observations` | Observations | **Mensuelle** (été) + Exceptionnelle (sécheresse) | Depuis ~2012 | Tests empiriques |

**Tests empiriques** :
- ✅ 2025-09-29 : 1,027,230 records (campagnes + observations)
- 📊 3,548 stations actives

**Nature** : **Opération Nationale Des Étiages**
- Campagnes mensuelles **mai à septembre** (période d'étiage)
- Observations exceptionnelles en cas de sécheresse
- Couverture nationale complète

**Partitions recommandées** : **Quotidiennes** ou **Hebdomadaires**

---

## 💧 API Prélèvements

**URL**: https://hubeau.eaufrance.fr/api/v1/prelevements

### Endpoints et Fréquences

| Endpoint | Type | Fréquence Mise à Jour | Accès Temporel | Source |
|----------|------|----------------------|----------------|---------|
| `/referentiel/points_prelevement` | Référentiel | Mensuelle | Tout l'historique | Hub'Eau |
| `/chroniques` | Volumes prélevés | **ANNUELLE** | Depuis ~2012 | [data.gouv.fr](https://www.data.gouv.fr/dataservices/hubeau-prelevements-en-eau/) |

**Source officielle** :
> **"L'API Prélèvements en eau de Hub'Eau fournit des informations sur les volumes annuels d'eau prélevés"** - data.gouv.fr

**Nature** : **BNPE** (Banque Nationale des Prélèvements quantitatifs en Eau)
- Déclarations administratives **annuelles**
- Volumes totaux prélevés sur l'année civile
- Publication avec délai (année N publiée en N+1)

**Partitions recommandées** : **ANNUELLES** (2020, 2021, 2022, etc.)

---

## 📋 Tableau Récapitulatif

| API | Fréquence Réelle | Restriction Temporelle | Partitions Recommandées | Volume Journalier Typique |
|-----|------------------|------------------------|-------------------------|---------------------------|
| **🌊 Hydrométrie** | 2 minutes (TR) | ⚠️ **30 derniers jours** | **30 jours glissants** | 10k-20k observations |
| **🏔️ Piézométrie** | Horaire (TR) / Quotidienne | Depuis 2020 | **Quotidiennes** | 300-500 mesures |
| **🌡️ Température** | Sporadique (horaire) | Depuis 2000-2008 | **Quotidiennes** | 0-4k mesures (variable) |
| **🧪 Qualité Nappes** | Trimestrielle/Semestrielle | Depuis ~2000 | **Quotidiennes** | 0 (campagnes) |
| **🧪 Qualité Cours d'Eau** | Continue (Naïades) | Depuis ~2000 | **Quotidiennes** | Variable |
| **🐟 Hydrobiologie** | Saisonnière (3-4/an) | Depuis 1971 | **Quotidiennes** | 0-5k indices (saisonnier) |
| **🌊 ONDE** | Mensuelle (été) | Depuis ~2012 | **Quotidiennes/Hebdo** | Variable (campagnes) |
| **💧 Prélèvements** | **ANNUELLE** | Depuis ~2012 | **ANNUELLES** | N/A |

---

## 🔍 Méthode d'Investigation

### Tests Empiriques Réalisés

```python
# Hydrométrie : Restriction 30 jours confirmée
date_debut_obs < 30 jours → Erreur 400
"date can't be < 1 month from now"

# Piézométrie : Historique disponible
2020-2025 : 4,868,509 mesures ✅

# Température : Historique ancien disponible
2000-2010 : 10,590,725 mesures ✅
2010-2020 : 33,573,202 mesures ✅

# Hydrobiologie : Très ancien historique
1971-1980 : 662 indices ✅
2010-2020 : 629,452 indices ✅

# Qualité Nappes : Volume massif
2024 : 8,534,797 analyses ✅

# Prélèvements : Volumes annuels
Source officielle confirme : "volumes annuels"
```

---

## 📚 Sources

1. **Hub'Eau Officiel** : https://hubeau.eaufrance.fr/page/apis
2. **cl-hubeau (référence Python)** : https://tgrandje.github.io/cl-hubeau/
3. **data.gouv.fr - Hub'Eau** : https://www.data.gouv.fr/dataservices/
4. **Tests empiriques** : API Hub'Eau (septembre 2025)
5. **Documentation projet** : `docs/DATA_SOURCES_COMPLETE.md`

---

## ⚠️ Recommandations d'Ingestion

### Par Fréquence de Données

**Temps Réel / Quotidien** :
```yaml
APIs:
  - Hydrométrie (observations_tr) : 2 min, 30 jours max
  - Piézométrie (chroniques_tr) : Horaire, récent
  - Piézométrie (chroniques) : Quotidien, historique ✅
  
Partitions: Quotidiennes (30 jours pour Hydrométrie)
Schedule: Quotidien (1x/jour)
```

**Campagnes Périodiques** :
```yaml
APIs:
  - Hydrobiologie : Saisonnière (printemps/été/automne)
  - ONDE : Mensuelle (mai-septembre)
  - Qualité Nappes : Trimestrielle/Semestrielle
  - Température : Sporadique
  
Partitions: Quotidiennes (accepter beaucoup de 0)
Schedule: Quotidien (détection automatique nouvelles données)
```

**Déclarations Annuelles** :
```yaml
APIs:
  - Prélèvements : ANNUELLE (BNPE)
  
Partitions: Annuelles (2020, 2021, 2022, ...)
Schedule: Annuel ou Manuel
```

---

## 🎯 Configuration Actuelle du Pipeline

```python
# Hydrométrie : 30 jours glissants
HYDROMETRY_RECENT_PARTITIONS = DailyPartitionsDefinition(
    start_date=(datetime.now() - timedelta(days=29))
)

# Autres APIs : 3 ans d'historique
DAILY_PARTITIONS = DailyPartitionsDefinition(
    start_date="2022-01-01"
)

# Prélèvements : Partitions annuelles
YEARLY_PARTITIONS = StaticPartitionsDefinition(
    ["2020", "2021", "2022", "2023", "2024", "2025"]
)
```

---

## 📝 Notes sur les Données "Vides"

### Comportement Normal (0 résultats)

**APIs avec données sporadiques** :
- 🌡️ Température : 50-80% des jours peuvent être vides
- 🐟 Hydrobiologie : 70-90% des jours vides (campagnes saisonnières)
- 🧪 Qualité Nappes : 95%+ des jours vides (campagnes trimestrielles)
- 🌊 ONDE : Hors période mai-septembre = peu de données

**Ce n'est PAS un problème** :
- ✅ Les stations sont récupérées
- ✅ Les données arrivent lors des campagnes
- ✅ Agrégations mensuelles/annuelles en Silver/Gold

---

## 🔄 Mise à Jour du Document

**Dernière mise à jour** : 30 septembre 2025  
**Méthode** : Tests empiriques + Documentation officielle  
**Statut** : ✅ Validé en production

Pour toute question ou mise à jour, consulter :
- Documentation Hub'Eau : https://hubeau.eaufrance.fr/page/apis
- Issues GitHub cl-hubeau : https://github.com/tgrandje/cl-hubeau
