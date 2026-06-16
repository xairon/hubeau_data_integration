# Déploiement Sandbox (POC) — Hub'Eau Data Pipeline

Déploiement de l'entrepôt sur le serveur **sandbox recherche** via **Portainer + GitOps**,
avec rechargement complet des données (pas de migration : `full_bootstrap_job`).

| | |
|---|---|
| **Cible** | `gpu2.recherche.sandbox.univ-tours.fr` (`10.108.44.30`) — Docker standalone, agent Portainer |
| **Compose** | [`docker-compose.sandbox.yml`](../docker-compose.sandbox.yml) |
| **Env** | [`.env.sandbox.example`](../.env.sandbox.example) |
| **Données** | bootstrap from scratch (Hub'Eau + ERA5) |
| **CI/CD** | GitOps Portainer (auto-redeploy sur push `main`) |

> Différences avec la prod : volumes nommés (pas d'`init_volumes.sh`), pas de stack
> monitoring (supprime la contrainte `XDG_RUNTIME_DIR`), pas de réservation GPU,
> `mem_limit` réduits car serveur mutualisé.

---

## Pré-requis à vérifier côté serveur

1. **Accès réseau sortant** vers les API externes — *bloquant pour le bootstrap* :
   - `hubeau.eaufrance.fr` (HTTPS)
   - `cds.climate.copernicus.eu` (HTTPS, ERA5)
   Un sandbox universitaire est souvent derrière un proxy/firewall. Tester depuis le serveur :
   `curl -sI https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/stations | head -1`
2. **Ports 495xx libres** (`49500-49505`) sur `10.108.44.30` — exposés sur l'IP du sandbox.
3. **Clé Copernicus CDS** valide (compte perso).

---

## 1. Créer le stack dans Portainer (depuis Git)

Portainer → *Stacks* → **Add stack** → méthode **Repository** :

| Champ | Valeur |
|---|---|
| Name | `hubeau-sandbox` |
| Repository URL | URL GitLab du repo |
| Repository reference | `refs/heads/main` |
| Compose path | `docker-compose.sandbox.yml` |
| Authentication | token GitLab si repo privé |

Dans **Environment variables**, renseigner les clés de `.env.sandbox.example`
(au minimum `DAGSTER_PG_PASSWORD`, `PG_PASSWORD`, `POSTGIS_PASSWORD`,
`COPERNICUS_API_KEY`, `SUPERSET_SECRET_KEY`, `SUPERSET_ADMIN_PASSWORD`).

> Portainer **build** les images `hubeau-worker` et `hubeau-orchestrator` directement
> sur le serveur depuis le contexte Git — aucun registry requis.
> Les 3 volumes (`postgres_data`, `dagster_pg_data`, `cloudbeaver_data`) sont créés
> automatiquement, préfixés par le nom du stack.

Cliquer **Deploy the stack**. Premier build ≈ quelques minutes (image worker ~2 GB).

---

## 2. Activer le CI/CD (GitOps)

Deux options Portainer Business, au choix :

### Option A — Polling (le plus simple)
Dans le stack → **GitOps updates** → *Enable* → mécanisme **Polling**, intervalle ex. `5m`.
Portainer re-tire `main` et redéploie (rebuild des images si Dockerfile change) à chaque
changement détecté. Zéro config côté GitLab.

### Option B — Webhook (déploiement à la demande / piloté par la CI)
1. Stack → **GitOps updates** → *Enable* → mécanisme **Webhook** → copier l'URL générée.
2. Ajouter un job de déploiement dans `.gitlab-ci.yml` (déclenché sur `main`) :

```yaml
deploy_sandbox:
  stage: deploy
  image: curlimages/curl:latest
  script:
    - curl -fsS -X POST "$PORTAINER_WEBHOOK_URL"
  rules:
    - if: '$CI_COMMIT_BRANCH == "main"'
```

3. Dans GitLab → *Settings → CI/CD → Variables*, créer `PORTAINER_WEBHOOK_URL`
   (masquée/protégée) = l'URL du webhook.

> Le job `pages` existant (génération dbt docs) reste inchangé ; `deploy_sandbox`
> s'ajoute à côté dans le stage `deploy`.

À chaque push sur `main` : GitLab appelle le webhook → Portainer re-tire le repo,
rebuild et redéploie le stack. **Le code applicatif (`src/`, `configs/`, `dagster_home/`)
est monté en volume** → un simple redéploiement suffit ; pensez juste à recharger la
*code location* Dagster (étape 4) après un changement de code Python.

---

## 3. Lancer le bootstrap initial

Une fois le stack *healthy* :

1. Dagster UI : `http://10.108.44.30:49500`
2. Vérifier que la code location `hubeau_pipeline` est chargée (sinon *Reload*).
3. *Jobs* → **`full_bootstrap_job`** → **Launchpad** → *Launch Run*.

Le sous-ensemble chargé est piloté par `BOOTSTRAP_PARTITIONS` (cf. `.env.sandbox.example`).
**Pour un POC, limiter à 2-3 années récentes** — sinon le rechargement ERA5 complet
(1990→présent) prend des heures/jours et sature le quota CDS.

Restartable : l'état est persisté dans `ops.bootstrap_state`. En cas d'échec partiel
(API 503), relancer le job reprend là où il s'est arrêté (`BOOTSTRAP_CONTINUE_ON_ERROR=true`).

---

## 4. Après un changement de code (rappel)

Le worker hot-reload le code monté en volume, mais Dagster ne recharge pas les
définitions tout seul :

1. Redéploiement Portainer (auto via GitOps) → conteneurs recréés avec le repo à jour.
2. Dagster UI → **Reload definitions** sur la code location `hubeau_pipeline`
   (ou GraphQL `reloadRepositoryLocation`).

---

## Points d'attention sandbox

- **Serveur mutualisé** : les ports 495xx et la BI Superset sont exposés sur l'IP du
  sandbox. Ne pas y mettre de données sensibles ; changer les mots de passe par défaut.
- **Persistance** : volumes Portainer non-externes → un *remove stack* avec l'option
  "remove volumes" efface la base. Acceptable pour un POC (re-bootstrap possible).
- **GPU** : non utilisé par ce stack (réservation retirée). Le serveur GPU n'apporte
  rien de spécifique ici hormis le CPU/RAM.
