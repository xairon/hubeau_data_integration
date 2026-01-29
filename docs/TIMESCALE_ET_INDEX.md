# TimescaleDB, hypertables, compression et index (PostgreSQL)

Référence pour comprendre le fonctionnement de TimescaleDB et des types d’index utilisés dans le projet.

---

## 1. TimescaleDB et hypertables

### Qu’est-ce qu’une hypertable ?

Une **hypertable** est une table “logique” qui, en réalité, est découpée en **chunks** (morceaux) selon une colonne de temps. Chaque chunk est une table PostgreSQL classique.

```
Hypertable "stg_piezo_chroniques" (vue logique)
    │
    ├── Chunk 1 : date_mesure ∈ [2020-01-01, 2021-01-01)   → table physique _hyper_1_1_chunk
    ├── Chunk 2 : date_mesure ∈ [2021-01-01, 2022-01-01)   → table physique _hyper_1_2_chunk
    ├── Chunk 3 : date_mesure ∈ [2022-01-01, 2023-01-01)   → table physique _hyper_1_3_chunk
    └── ...
```

- **Colonne de temps** : celle utilisée pour le découpage (ex. `date_mesure`, `time`, `date`).
- **Intervalle de chunk** (`chunk_time_interval`) : chaque chunk couvre une plage de temps (ex. 1 an, 1 mois). Plus l’intervalle est petit, plus il y a de chunks et plus le *pruning* (élimination de chunks) est fin ; plus il est grand, moins il y a de chunks à gérer.

### À quoi ça sert ?

1. **Requêtes par plage de temps** : le planificateur PostgreSQL peut ignorer les chunks qui ne chevauchent pas la plage demandée (*time-based chunk pruning*). Ex. `WHERE date_mesure BETWEEN '2023-01-01' AND '2023-06-01'` → seuls les chunks concernés sont lus.
2. **Compression** : on peut compresser les **anciens** chunks (voir plus bas).
3. **Rétention** : on peut supprimer automatiquement les chunks trop vieux (*retention policy*).
4. **Parallélisme** : les chunks peuvent être lus en parallèle.

### Contrainte importante

La **clé primaire** de l’hypertable doit **inclure la colonne de temps** (ex. `PRIMARY KEY (code_bss, date_mesure)` avec `date_mesure` = colonne de temps). C’est pour que le routage des lignes vers le bon chunk soit cohérent.

---

## 2. Compression : que se passe-t-il quand on requête au-delà ?

### Comment la compression est configurée

Dans le projet, on active la compression avec par exemple :

- `timescaledb.compress` : activer la compression.
- `compress_segmentby` : colonnes pour “grouper” les lignes avant compression (ex. `latitude, longitude` ou `code_bss, code_departement`).
- `compress_orderby` : ordre à l’intérieur d’un segment (ex. `time DESC`).
- **Politique de compression** : “compresser les chunks dont la plage de temps est **entièrement** plus vieille que X” (ex. `compress_after = 90 days` → les données de plus de 90 jours sont compressées).

Résultat : les **anciens** chunks sont convertis en **chunks compressés** (stockage par segment, souvent en colonnes, très compact). Les **récents** restent “chauds” (non compressés) pour les inserts et les requêtes récentes.

### Requêter des données déjà compressées : que se passe-t-il ?

Quand tu fais une requête qui touche à des plages de temps **au-delà** de la limite de compression (donc des chunks compressés) :

1. **TimescaleDB décompresse à la volée** : il n’y a rien de spécial à faire dans ta requête. Tu écris du SQL normal sur l’hypertable.
2. Le moteur :
   - sait quels chunks sont compressés ;
   - décompresse uniquement les **chunks (ou segments) concernés** par ta requête ;
   - exécute le filtre / jointure / agrégat sur les lignes décompressées.
3. **Coût** : lire des données compressées est en général **moins coûteux en I/O** que de lire la même quantité de données non compressées ; le coût CPU de la décompression est souvent compensé par la réduction d’I/O. Donc requêter “au-delà” de la compression reste correct, voire plus rapide en pratique qu’une grosse table non compressée.

En résumé : **tu peux requêter toute la plage de temps** ; les données compressées sont décompressées automatiquement pour répondre à la requête. Aucune requête “spéciale” n’est nécessaire.

---

## 3. Les types d’index : BRIN, B-tree, GIST

### B-tree (arbre B)

- **Usage** : index “classique” pour égalité et tri (recherche par valeur, `=`, `IN`, `>`, `<`, `ORDER BY`, jointures).
- **Structure** : arbre équilibré ; les feuilles contiennent des pointeurs vers les lignes.
- **Taille** : peut devenir volumineux sur de grosses tables.
- **Exemples dans le projet** : `code_bss`, `code_site`, `(code_bss, date_mesure)`, etc. C’est l’index par défaut quand on fait `CREATE INDEX ... ON table (colonne)`.

### BRIN (Block Range INdex)

- **Usage** : idéal pour une colonne **triée ou à peu près triée** physiquement sur le disque (ex. colonne de temps dans une hypertable, où les chunks sont déjà ordonnés par temps).
- **Principe** : au lieu d’indexer chaque valeur, le BRIN stocke pour **chaque bloc de N pages** un résumé (min, max, etc.). Pour une requête “où `date_mesure` entre A et B”, le moteur écarte les blocs dont le résumé ne coupe pas [A, B].
- **Taille** : très petit par rapport à un B-tree sur la même colonne.
- **Limite** : moins précis qu’un B-tree ; il peut lire quelques blocs “en trop”. Très bon pour les **gros volumes** et les **requêtes par plage** sur une colonne de temps.
- **Exemples dans le projet** : index BRIN sur `date_mesure`, `date`, `time`, `era5_date`, `mois`, `annee`.

### GIST (Generalized Search Tree)

- **Usage** : index pour des types “complexes” : **géométries** (PostGIS), **recherche plein texte**, **tableaux**, **gammes** (range), etc.
- **Principe** : structure d’arbre qui permet de répondre à des prédicats comme “contient”, “intersecte”, “est à moins de X mètres”, “K plus proches voisins” (KNN).
- **PostGIS** : sur une colonne `geometry`, un index GIST permet d’utiliser `ST_Contains`, `ST_DWithin`, `ST_Intersects`, et l’opérateur **`<->`** (distance, utilisé pour le KNN : “le point le plus proche”).
- **Exemples dans le projet** : index GIST sur toutes les colonnes `geometry` / `geom` pour les requêtes spatiales et le plus proche voisin (ex. `int_station_era5_mapping`).

---

## 4. Récapitulatif

| Concept | Rôle |
|--------|------|
| **Hypertable** | Table “logique” découpée en chunks par plage de temps ; permet pruning et compression par chunk. |
| **Chunk** | Morceau de la hypertable (une plage de temps) ; table PostgreSQL normale (ou compressée). |
| **Requête au-delà de la compression** | Les chunks compressés sont décompressés à la volée ; le SQL reste inchangé. |
| **B-tree** | Index standard pour égalité, inégalité, tri, jointures. |
| **BRIN** | Index léger pour colonne quasi triée (ex. temps) ; excellents pour les plages de dates. |
| **GIST** | Index pour types avancés (géométrie, etc.) ; utilisé pour PostGIS et KNN. |

Pour aller plus loin : [TimescaleDB Docs](https://docs.timescale.com/), [PostgreSQL BRIN](https://www.postgresql.org/docs/current/brin-intro.html), [PostGIS](https://postgis.net/documentation/).
