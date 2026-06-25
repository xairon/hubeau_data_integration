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

> Le Portainer mutualisé interdit **les bind mounts ET le `build:` dans un stack**
> ("forbidden properties"). Ce déploiement n'utilise donc ni l'un ni l'autre :
> les 4 images custom sont **pré-buildées par la CI GitLab** et poussées sur le
> **GitLab Container Registry** ; le compose ne fait que les référencer par tag.
>
> Autres différences avec la prod : volumes nommés (pas d'`init_volumes.sh`), pas
> de GPU, `postgres_tuning` retiré, `mem_limit` réduits.
>
> ⚠️ Dans Portainer : **ne PAS activer « relative path volumes »** (aucun bind mount).

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

## 1. Builder & pousser les images (CI GitLab → Container Registry)

Les images sont buildées par la CI, pas par Portainer.

1. **Activer le registry** : GitLab → *Settings → General → Visibility* → s'assurer
   que **Container Registry** est activé. Le chemin s'affiche dans *Deploy → Container Registry*
   (= la variable `CI_REGISTRY_IMAGE`), ex. `registry.scm.univ-tours.fr/ringuet/hubeau_data_integration`.
   **Note ce chemin** : c'est le `REGISTRY_PREFIX` de l'étape 3.
2. **Lancer le pipeline** : un push sur `main` déclenche le
   stage `build` (`.gitlab-ci.yml`) qui build et pousse 3 images via kaniko :
   - `hubeau-worker:sandbox`
   - `hubeau-orchestrator:sandbox`
   - `hubeau-postgres-sandbox:sandbox` (TimescaleDB + `init.sql` intégré)

   Suivre dans GitLab → *Build → Pipelines*. Le job `build_worker` est le plus long (~2 GB, GDAL + dbt).
3. **Vérifier** : GitLab → *Deploy → Container Registry* doit lister les 3 images taguées `sandbox`.

## 2. Déclarer le registry dans Portainer (pour pull le privé)

Le registry est privé → Portainer a besoin d'un identifiant pour le pull :

1. GitLab → *Settings → Repository → Deploy tokens* → créer un token avec le scope
   **`read_registry`** (note le username + token générés).
2. Portainer → *Registries* → **Add registry** → type **Custom registry** :
   - URL : le **host** du registry (ex. `registry.scm.univ-tours.fr`)
   - Username / Password : le deploy token de l'étape 1.

## 3. Créer le stack dans Portainer (compose depuis Git)

Portainer → *Stacks* → **Add stack** → méthode **Repository** :

| Champ | Valeur |
|---|---|
| Name | `hubeau-sandbox` |
| Repository URL | URL GitLab du repo |
| Repository reference | `refs/heads/main` |
| Compose path | `docker-compose.sandbox.yml` |
| Authentication | token GitLab si repo privé |
| Relative path volumes | **décoché** (aucun bind mount) |

Dans **Environment variables** (Advanced mode → coller le `.env.sandbox`), au minimum :
`REGISTRY_PREFIX` (le chemin de l'étape 1), `DAGSTER_PG_PASSWORD`, `PG_PASSWORD`,
`POSTGIS_PASSWORD`, `COPERNICUS_API_KEY`.

> Le compose ne contient **ni `build:` ni bind mount** → uniquement des `image:`
> (3 custom depuis le registry + postgres/adminer publics) et 2 volumes nommés.

Cliquer **Deploy the stack**. Portainer pull les images (pas de build) → démarrage en 1-2 min.

---

## 4. CI/CD (auto-rebuild + redeploy)

Le flux complet à chaque push : **CI rebuild les images** (stage `build`, tag `sandbox`
écrasé) → **Portainer re-pull et redéploie**. Deux façons de déclencher le redeploy :

### Option A — Polling (le plus simple)
Dans le stack → **GitOps updates** → *Enable* → **Polling** `5m` + cocher
**Re-pull image** (sinon Portainer garde l'ancienne image au même tag). Portainer
détecte le nouveau commit, re-pull les images `:sandbox` fraîches et redéploie.

### Option B — Webhook (piloté par la CI, après le build)
1. Stack → **GitOps updates** → *Enable* → **Webhook** → copier l'URL.
2. Ajouter un job dans `.gitlab-ci.yml` (stage `deploy`, donc **après** le build) :

```yaml
deploy_sandbox:
  stage: deploy
  image: curlimages/curl:latest
  script:
    - curl -fsS -X POST "$PORTAINER_WEBHOOK_URL"
  rules:
    - if: '$CI_COMMIT_BRANCH == "main"'
```

3. GitLab → *Settings → CI/CD → Variables* → `PORTAINER_WEBHOOK_URL` (masquée).

> Comme le code est **intégré dans les images** (plus de bind mount), un changement
> de code Python n'est pris en compte qu'après **rebuild de l'image worker** par la CI.
> Après le redeploy, pense à **Reload definitions** dans Dagster (étape 6).

---

## 5. Lancer le bootstrap initial

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

## 6. Après un changement de code (rappel)

Le code est **intégré dans l'image worker** (plus de hot reload par volume) :

1. Push → la CI **rebuild** `hubeau-worker:sandbox`.
2. Portainer **re-pull + redéploie** (GitOps, étape 4).
3. Dagster UI → **Reload definitions** sur la code location `hubeau_pipeline`
   (ou GraphQL `reloadRepositoryLocation`).

---

## Points d'attention sandbox

- **Serveur mutualisé** : les ports 495xx sont exposés sur l'IP du
  sandbox. Ne pas y mettre de données sensibles ; changer les mots de passe par défaut.
- **Persistance** : volumes Portainer non-externes → un *remove stack* avec l'option
  "remove volumes" efface la base. Acceptable pour un POC (re-bootstrap possible).
- **GPU** : non utilisé par ce stack (réservation retirée). Le serveur GPU n'apporte
  rien de spécifique ici hormis le CPU/RAM.
