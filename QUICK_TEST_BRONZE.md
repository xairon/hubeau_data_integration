# Test Rapide - Bronze Layer Asset

## ⚠️ IMPORTANT - Choisis le BON Asset!

### ❌ NE PAS matérialiser:
- `temperature_stations_csv` (Legacy - crée table SANS _raw)
- `temperature_chroniques_csv` (Legacy - crée table SANS _raw)
- Tout asset se terminant par `_csv`

### ✅ À matérialiser:
- `temperature_stations_raw` (Bronze - crée table AVEC _raw)
- `temperature_chroniques_raw` (Bronze - crée table AVEC _raw)
- Tout asset se terminant par `_raw`

---

## Test 1: Asset STATIONS Bronze (Simple - 30 secondes)

### Étapes:

1. **Ouvre Dagster UI**
   ```
   http://localhost:8080
   ```

2. **Navigue vers Assets**
   - Clique sur "Assets" (menu gauche)
   - Dans la barre de recherche en haut, tape: `temperature_stations_raw`
   - ⚠️ VÉRIFIE: L'asset doit se terminer par `_raw`!

3. **Vérifie l'asset**
   - Clique sur `temperature_stations_raw`
   - Vérifie dans les détails:
     - Group: "temperature"
     - Compute kind: "dlt"
     - ⚠️ Nom: doit finir par `_raw`

4. **Matérialise**
   - Clique sur le bouton **"Materialize"** (en haut à droite)
   - Une popup apparaît → Clique **"Launch 1 run"**
   - Observe le run en temps réel

5. **Ce qui doit se passer** (30-60 secondes):
   ```
   ✅ Step 1: Load config from configs/hubeau/temperature_stations.yml
   ✅ Step 2: Create DLT pipeline with postgres destination
   ✅ Step 3: Call hubeau_stations(config)
   ✅ Step 4: Fetch ~850 stations from Hub'Eau API
   ✅ Step 5: DLT creates table hubeau.temperature_stations_raw (AUTO!)
   ✅ Step 6: DLT infers schema from CSV (AUTO!)
   ✅ Step 7: DLT inserts data
   ✅ Step 8: Run completes: SUCCESS
   ```

6. **Vérification dans Dagster UI**
   - Status final: **SUCCESS** (icône verte ✓)
   - Metadata affichée:
     - "rows_loaded": ~850
     - "duration_seconds": 30-60
   - Aucune erreur dans les logs

7. **Vérification dans la Base de Données**

   **Via Adminer** (http://localhost:8081):
   ```
   Server: postgres
   Username: postgres
   Password: BrgmPostgres2024!
   Database: postgres
   ```

   **SQL à exécuter:**
   ```sql
   -- 1. Vérifier que la table existe
   SELECT EXISTS (
       SELECT FROM information_schema.tables
       WHERE table_schema = 'hubeau'
       AND table_name = 'temperature_stations_raw'
   );
   -- Expected: true

   -- 2. Compter les records
   SELECT COUNT(*) FROM hubeau.temperature_stations_raw;
   -- Expected: ~850

   -- 3. Voir un échantillon
   SELECT code_station, libelle_station, latitude, longitude
   FROM hubeau.temperature_stations_raw
   LIMIT 5;

   -- 4. VÉRIFIER: PAS de PRIMARY KEY (Bronze = raw data!)
   SELECT
       constraint_name,
       constraint_type
   FROM information_schema.table_constraints
   WHERE table_schema = 'hubeau'
   AND table_name = 'temperature_stations_raw';
   -- Expected: Aucun résultat (pas de contraintes!)

   -- 5. Voir la structure de la table
   \d hubeau.temperature_stations_raw
   ```

---

## Critères de Succès

### ✅ Checklist
- [ ] Asset matérialisé: `temperature_stations_raw` (avec _raw!)
- [ ] Run Dagster: SUCCESS
- [ ] Table créée: `hubeau.temperature_stations_raw` (avec _raw!)
- [ ] Records insérés: ~850
- [ ] Aucune contrainte PRIMARY KEY
- [ ] Aucune contrainte FOREIGN KEY
- [ ] Types inférés automatiquement par DLT

### ❌ Si tu vois:
- Table `temperature_stations` (SANS _raw) → Tu as matérialisé le mauvais asset!
- PRIMARY KEY constraint → C'est un legacy asset, pas Bronze!
- Erreur "table already exists" → Supprime la table legacy d'abord

---

## Test 2: Asset CHRONIQUES Bronze avec Partition (Optionnel - 10 min)

### Seulement si Test 1 réussit!

1. **Cherche:** `temperature_chroniques_raw`
2. **Clique** sur l'asset
3. **Onglet "Partitions"** (en haut)
4. **Sélectionne** la partition **"2024"**
5. **Matérialise** la partition sélectionnée
6. **Attends** 5-10 minutes (~1.5M records)

**Ce qui se passe:**
```
✅ delete_year_data("temperature_chroniques_raw", "2024", "date_mesure_temp")
✅ Fetch all 2024 data from API
✅ Create table hubeau.temperature_chroniques_raw (AUTO!)
✅ Insert ~1.5M records
```

**Vérification:**
```sql
-- Compter les records 2024
SELECT COUNT(*)
FROM hubeau.temperature_chroniques_raw
WHERE EXTRACT(YEAR FROM date_mesure_temp) = 2024;
-- Expected: ~1.5M

-- Vérifier que c'est une table _raw (pas de PK!)
SELECT constraint_name
FROM information_schema.table_constraints
WHERE table_schema = 'hubeau'
AND table_name = 'temperature_chroniques_raw';
-- Expected: Aucun résultat
```

---

## Différences Legacy vs Bronze

| Aspect | Legacy (`*_csv`) | Bronze (`*_raw`) |
|--------|------------------|------------------|
| **Asset name** | `temperature_stations_csv` | `temperature_stations_raw` |
| **Table name** | `temperature_stations` | `temperature_stations_raw` |
| **Primary Key** | ✅ YES | ❌ NO |
| **Foreign Keys** | ✅ YES | ❌ NO |
| **Schema** | Manual (SQL files) | Auto (DLT infers) |
| **Destination** | Custom (optimized_v2) | Standard (DLT postgres) |
| **Philosophy** | Clean, normalized | Raw, duplicates OK |
| **Maintenance** | High | Zero |

---

## Si Erreur

### "Table temperature_stations already exists"
```sql
-- Supprimer la table legacy
DROP TABLE IF EXISTS hubeau.temperature_stations CASCADE;
```

### "Cannot import name..."
```bash
# Redémarrer le worker
docker compose restart dlt_worker
# Attendre 10 secondes
sleep 10
```

### "Run failed"
```bash
# Voir les logs
docker compose logs dlt_worker --tail=50
```

---

## 🎯 ACTION IMMÉDIATE

1. Va sur http://localhost:8080
2. Cherche `temperature_stations_raw` (AVEC _raw!)
3. Matérialise
4. Vérifie que la table `hubeau.temperature_stations_raw` est créée
5. ✅ Succès = Bronze Layer fonctionne!
