# Intégration des données hydrométriques en Gold (réflexion)

Note de réflexion sur l’intégration des données **hydrométriques** (débits) aux données **météo (ERA5)** et/ou **piézométriques** en gold, pour l’analyse. **Aucune implémentation** : uniquement options de design et recommandations.

---

## Contexte actuel

| Domaine   | Silver / Gold actuel | Intégration en gold |
|-----------|----------------------|----------------------|
| **Piézo** | `stg_piezo_chroniques`, `int_daily_measurements` | **Oui** : `hubeau_daily_chroniques` = piézo + ERA5 + TME (station × date). |
| **Hydro** | `stg_hydrometry_obs_elab`, `stg_hydrometry_sites/stations`, `dim_hydro_stations` | **Non** : pas de mart qui joint hydro avec météo ou piézo. |
| **Météo** | `stg_era5_timeseries`, `int_era5_for_stations`, mapping spatial piézo → grille ERA5 | Utilisé uniquement avec la piézo aujourd’hui. |

L’hydrométrie reste donc “à part” en gold : utile pour des analyses débit seules, mais pas alignée avec le même pattern “chroniques × météo” que la piézo.

---

## Faut-il intégrer l’hydro aux données météo et/ou piézo ?

### Hydro + météo (ERA5)

**Oui, c’est pertinent.**

- Le **débit** des cours d’eau est fortement lié aux **précipitations** et à l’**évapotranspiration** (et donc à la température / évaporation ERA5).
- Une table gold du type **“chroniques hydro quotidiennes avec météo”** (site × date × débit + temp, précip, évapo ERA5) permettrait :
  - corrélations débit–précipitations, débit–évaporation ;
  - analyse de la réponse des cours d’eau aux épisodes pluvieux / sécheresse ;
  - comparaison débits observés vs “naturels” (en lien avec le climat) ;
  - indicateurs sécheresse multi-sources (débit + météo).

C’est l’**analogue** de ce qu’on a fait pour la piézo avec `hubeau_daily_chroniques`, appliqué aux sites hydro.

### Hydro + piézo

**Oui, mais plus délicat.**

- **Nappes** (piézo) et **cours d’eau** (hydro) sont liés : exutoires de nappe, recharge par les rivières, délais nappe–rivière, etc.
- En revanche :
  - **Granularité différente** : une station piézo = un point (code_bss) ; un site hydro = un point sur un cours d’eau (code_site). Il n’y a pas de clé naturelle commune.
  - Pour les joindre, il faut un **lien spatial ou hydrogéologique** : même bassin versant, proximité, même masse d’eau, etc.
- Une table “site hydro × date × débit + indicateur piézo (bassin / proche)” serait utile pour :
  - bilan nappe–rivière ;
  - analyse des délais et des corrélations débit–niveau de nappe ;
  - tableaux de bord “eau souterraine + eau de surface”.

Mais le **design spatial** (bassin, proximité, représentativité) est plus lourd que pour “hydro + météo” et peut dépendre d’un **référentiel externe** (bassins versants, zones d’influence).

### Hydro + météo + piézo (les trois)

**Possible**, mais à traiter comme une **évolution** après hydro+ERA5 (et éventuellement hydro+piézo).

- Un mart “multisource” par **date** et par **entité spatiale** (ex. bassin, département, région) : agrégats piézo + agrégats hydro + météo agrégée.
- Très utile pour **indicateurs globaux** (sécheresse multi-indicateurs, bilan eau à l’échelle région/bassin), moins pour l’analyse site par site.

---

## Comment faire en gold ? (options de design)

### 1. Hydro + ERA5 (recommandé en premier)

**Principe** : reproduire le pattern de `hubeau_daily_chroniques` pour l’hydrométrie.

1. **Mapping spatial sites hydro → grille ERA5**  
   - Créer un équivalent de `int_station_era5_mapping` pour les **sites hydro** : pour chaque site (latitude/longitude dans `stg_hydrometry_sites` ou stations), trouver le **point grille ERA5 le plus proche** (KNN PostGIS, comme pour la piézo).
   - Output : `code_site` (ou `code_station`), coordonnées site, `era5_latitude`, `era5_longitude`, métadonnées site (département, cours d’eau, etc.).

2. **Mart “chroniques hydro + météo”**  
   - **Granularité** : `(code_site, date)` (ou `(code_station, date)` selon le choix métier).
   - **Colonnes** :  
     - débit (ex. `resultat_obs_elab` pour QmnJ = débit moyen journalier) ;  
     - `temperature_2m`, `total_precipitation`, `potential_evaporation` (ERA5 au point grille du site) ;  
     - métadonnées site (département, libellé cours d’eau, etc.).
   - **Jointures** :  
     - hydro quotidien (agrégation / déduplication depuis `stg_hydrometry_obs_elab`) ;  
     - mapping site → grille ERA5 ;  
     - ERA5 quotidien (équivalent de `int_era5_for_stations` mais pour les points grille utilisés par les **sites hydro**).
   - **Contraintes** : filtre qualité (débit non nul, météo non nulle), types explicites, PK `(code_site, date)`, hypertable si besoin.

**Avantages** : même logique que piézo+ERA5, pas de référentiel supplémentaire, forte valeur pour l’analyse débit–climat. **Effort** : un mapping + un mart, réutilisation des briques ERA5 existantes.

---

### 2. Hydro + piézo

Plusieurs niveaux possibles, du plus simple au plus “propre” hydrologiquement.

**Option A – Jointure temporelle seule (agrégats)**  
- Pas de lien spatial site-by-site.  
- Mart **par date** (ou par date × département / région) :  
  - agrégats hydro (ex. débit moyen / médiane par département ou France) ;  
  - agrégats piézo (ex. niveau moyen par département, comme dans les marts existants).  
- Utile pour : **tableaux de bord nationaux/régionaux**, indicateurs de sécheresse “débit + nappe” à l’échelle agrégée.  
- **Limite** : pas d’analyse au niveau site (quel débit avec quelle nappe).

**Option B – Lien par proximité géographique**  
- Pour chaque **site hydro**, définir N **stations piézo les plus proches** (rayon fixe ou KNN PostGIS).  
- Mart **site hydro × date** : débit + indicateur piézo “proche” (ex. moyenne des niveaux des K stations les plus proches).  
- **Avantage** : pas de référentiel bassin ; **limite** : la “proximité” n’est pas toujours pertinente hydrologiquement (pas le même bassin, nappe différente).

**Option C – Lien par bassin versant (idéal mais dépendant d’un référentiel)**  
- Référentiel **bassins versants** (ou sous-bassins) avec :  
  - lien site hydro → bassin ;  
  - lien station piézo → bassin (ou zone d’influence).  
- Mart **bassin × date** (ou **site hydro × date** avec colonnes piézo “du bassin”) :  
  - débits des sites hydro du bassin ;  
  - niveaux piézo des stations du bassin (moyenne, min, max, etc.) ;  
  - optionnel : météo agrégée sur le bassin.  
- **Avantage** : cohérent pour l’analyse nappe–rivière ; **inconvénient** : besoin d’un référentiel externe (ex. Sandre, Aïga) et de règles d’affectation.

**Recommandation** : ne pas bloquer le reste. Commencer par **Option A** (agrégats par date/département) pour avoir tout de suite des indicateurs “hydro + piézo” ; ensuite, si besoin métier fort, envisager **Option B** (proximité) ou **Option C** (bassin) quand le référentiel ou la règle est claire.

---

### 3. Mart “multi-sources” (date ou date × région)

- **Granularité** : **date** seule, ou **date × département / région**.  
- **Colonnes** : agrégats **piézo** (nb stations, niveau moyen, etc.) + agrégats **hydro** (nb sites, débit moyen, etc.) + **météo** agrégée (moyenne France ou par région).  
- **Usage** : dashboards nationaux/régionaux, indicateurs de sécheresse “tout-en-un”, pas de détail par site.  
- Peut être alimenté à partir de `hubeau_daily_chroniques` (déjà agrégé piézo+ERA5) + un mart hydro+ERA5 agrégé + éventuellement hydro+piézo agrégé (Option A ci-dessus).

---

## Ordre recommandé (sans implémentation)

1. **Hydro + ERA5** en gold (mapping site hydro → grille ERA5 + mart chroniques hydro × date avec débit + météo).  
   - Aligne l’hydro sur le même pattern que la piézo pour l’analyse climat.  
   - Peu de dépendances, forte valeur pour débit–précipitations–sécheresse.

2. **Hydro + piézo** : commencer par une **jointure temporelle agrégée** (par date ou par département) pour ne pas dépendre tout de suite d’un référentiel spatial ; ensuite, si besoin, ajouter proximité ou bassin.

3. **Mart multi-sources** (date ou date × région) une fois qu’on a au moins piézo+ERA5 et hydro+ERA5 (et optionnellement hydro+piézo agrégé).

4. **Hydro + piézo spatial** (proximité ou bassin) : quand la règle métier et le référentiel sont définis.

---

## Résumé

| Question | Réponse |
|----------|---------|
| Intégrer l’hydro aux données météo en gold ? | **Oui** : mart “chroniques hydro + ERA5” par site × date, sur le même principe que `hubeau_daily_chroniques`. |
| Intégrer l’hydro aux données piézo en gold ? | **Oui**, mais en plusieurs étapes : d’abord agrégats temporels (date/département), puis éventuellement lien spatial (proximité ou bassin) si référentiel disponible. |
| Comment ? | Hydro+ERA5 : mapping spatial sites hydro → grille ERA5 + mart site × date (débit + météo). Hydro+piézo : agrégats par date/département, puis optionnellement proximité ou bassin. |

Ce document reste une **réflexion** : aucune modification du code ou des modèles dbt n’est implémentée ; il peut servir de base pour un prochain design détaillé (schémas, noms de tables, dépendances dbt) quand tu décideras de passer à l’implémentation.
