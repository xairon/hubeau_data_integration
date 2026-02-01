# 🚀 Hub'Eau Pipeline - Démarrage Rapide

## ⚠️ ÉTAPE 1: CRÉER LES VOLUMES (OBLIGATOIRE)

**AVANT le premier `docker compose up`, vous DEVEZ créer les volumes Docker externes.**

Ceci protège vos données contre suppression accidentelle. Les volumes externes ne sont **JAMAIS** supprimés par `docker compose down -v`.

### Linux/Mac:
```bash
bash scripts/init_volumes.sh
```

### Windows:
```cmd
scripts\init_volumes.bat
```

### OU manuellement:
```bash
docker volume create brgm_postgres_data
docker volume create brgm_dagster_pg_data
docker volume create brgm_cloudbeaver_data
```

Vérifier que les volumes existent:
```bash
docker volume ls | grep brgm_
```

---

## ÉTAPE 2: Configuration (Optionnel)

Créer un fichier `.env` à la racine (copier depuis `.env.example` si disponible):
```env
# Base de données principale
PG_PASSWORD=votre_mot_de_passe_securisé
PG_DB=postgres
PG_USER=postgres

# Métadonnées Dagster
DAGSTER_PG_PASSWORD=dagster_mot_de_passe

# Superset
SUPERSET_SECRET_KEY=votre_cle_secrete_longue_et_complexe
SUPERSET_ADMIN_PASSWORD=admin_password

# Activation des schedules/sensors (production)
DAGSTER_ENABLE_SCHEDULES=false
DAGSTER_ENABLE_SENSORS=false
```

**Note**: Si `.env` n'existe pas, les valeurs par défaut seront utilisées (développement local).

---

## ÉTAPE 3: Démarrer les services

```bash
# Build et démarrage
docker compose up -d --build

# Vérifier que tous les services sont "healthy"
docker compose ps

# Suivre les logs (Ctrl+C pour quitter)
docker compose logs -f dlt_worker dagster_webserver
```

**Services disponibles**:
- Dagster UI: http://localhost:49500
- Adminer (DB): http://localhost:49501
- PostgreSQL: localhost:49502
- CloudBeaver: http://localhost:49503
- Superset: http://localhost:49504

---

## ÉTAPE 4: Chargement initial des données

### Option A: Bootstrap complet (recommandé)
Lance le job `full_bootstrap_job` depuis le Dagster UI (http://localhost:49500):
1. Ouvrir Dagster UI
2. Aller dans "Jobs"
3. Cliquer sur `full_bootstrap_job`
4. Cliquer sur "Launchpad" → "Launch Run"

**Attention**: Ce job prend plusieurs heures (voire jours) - il charge toutes les données depuis 1990.

### Option B: Chargement progressif
1. **Données de référence**: Lancer `reference_data_bronze_job` (TME uniquement)
2. **Stations**: Lancer `all_stations_job` (métadonnées)
3. **Chroniques**: Lancer `all_chroniques_job` avec partitions spécifiques (ex: 2023, 2024)
4. **ERA5**: Lancer `era5_meteo_job` avec partitions
5. **Transformations**: Lancer `dbt_silver_gold_pipeline_job`

---

## ÉTAPE 5: Vérification

### Vérifier les données dans PostgreSQL
```bash
docker exec -it brgm-postgres psql -U postgres -d postgres

# Compter les lignes par schéma
SELECT schemaname, tablename, n_live_tup AS rows
FROM pg_stat_user_tables
WHERE schemaname IN ('bronze', 'silver', 'gold')
ORDER BY schemaname, n_live_tup DESC;
```

### Lancer les tests dbt
```bash
docker exec brgm-dlt-worker dbt test
```

### Générer la documentation dbt
```bash
docker exec brgm-dlt-worker dbt docs generate
```

---

## 🛡️ Protection des données

### ✅ SAFE - Ne supprime PAS les volumes:
```bash
docker compose down
docker compose restart
docker compose stop
```

### ⚠️ DANGEREUX - Supprime les volumes:
```bash
# Ne JAMAIS utiliser en production:
docker compose down -v
docker system prune --volumes

# Pour supprimer manuellement les volumes:
docker volume rm brgm_postgres_data brgm_dagster_pg_data brgm_cloudbeaver_data
```

### 💾 Backup recommandé

```bash
# Backup complet
docker exec brgm-postgres pg_dump -U postgres postgres | gzip > backup_$(date +%Y%m%d).sql.gz

# Restore
gunzip -c backup_20260201.sql.gz | docker exec -i brgm-postgres psql -U postgres postgres
```

---

## 📚 Documentation complète

Voir [CLAUDE.md](CLAUDE.md) pour:
- Commandes Docker avancées
- Opérations dbt détaillées
- Architecture du projet
- Troubleshooting

---

## 🐛 Problèmes courants

### "Error: volume brgm_postgres_data not found"
→ Lancer `scripts/init_volumes.sh` (ou `.bat` sur Windows)

### "Port 49502 already in use"
→ Modifier le port dans `docker-compose.yml` ou arrêter le service qui utilise ce port

### "dbt KeyError: test not found in manifest"
→ Relancer `docker compose build --no-cache` (le manifest dbt est généré au build)

### Services en "unhealthy"
→ Vérifier les logs: `docker compose logs [service_name]`

---

## 📞 Support

Pour les instances futures de Claude Code, voir [CLAUDE.md](CLAUDE.md) pour le contexte complet du projet.
