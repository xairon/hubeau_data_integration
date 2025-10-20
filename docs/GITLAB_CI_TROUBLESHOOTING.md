# GitLab CI/CD - Résolution des Problèmes

## 🚨 Solution Rapide pour Erreur 503 Persistante

Si l'erreur 503 persiste malgré les configurations, exécuter ce script automatisé :

```bash
# SSH sur le serveur
ssh root@srv991054.hstgr.cloud

# Télécharger et exécuter le script de fix
curl -fsSL https://raw.githubusercontent.com/YOUR_REPO/main/scripts/fix-docker-pull.sh | bash

# OU si le repo est cloné localement :
cd /srv/brgm
bash scripts/fix-docker-pull.sh

# Redémarrer le runner
systemctl restart gitlab-runner

# Relancer le pipeline dans GitLab UI
```

**Ce script va automatiquement :**
1. ✅ Pré-puller toutes les images Docker nécessaires
2. ✅ Configurer le GitLab Runner pour préférer le cache local
3. ✅ Créer un wrapper avec retry automatique
4. ✅ Tester la connexion Docker Hub et afficher le rate limit

---

## Erreur: "503 Service Unavailable" lors du pull Docker Hub

### Symptôme
```
ERROR: Job failed: failed to pull image "docker:24-cli" with specified policies [always]:
Error response from daemon: Head "https://registry-1.docker.io/v2/library/docker/manifests/24-cli":
received unexpected HTTP status: 503 Service Unavailable
```

### Causes Possibles
1. **Docker Hub temporairement indisponible** (problème serveur)
2. **Rate limiting Docker Hub** (limite de pull atteinte)
3. **Problème réseau du GitLab Runner**
4. **DNS/proxy issues sur le serveur**

### Solutions

#### Solution 1: Retry Automatique (✅ IMPLÉMENTÉ V2)

La configuration `.gitlab-ci.yml` inclut maintenant une stratégie de retry améliorée :

```yaml
variables:
  FF_GITLAB_REGISTRY_HELPER_IMAGE: 1
  DOCKER_PULL_POLICY: "if-not-present"

build:image:
  image:
    name: docker:24-cli
    pull_policy: ["if-not-present", "always"]
  retry:
    max: 2  # Maximum autorisé par GitLab
    when:
      - runner_system_failure
      - stuck_or_timeout_failure
      - api_failure
```

**Améliorations V2**:
- Pull policy "if-not-present" pour utiliser cache local en priorité
- Retry maximum de 2 tentatives (limite GitLab)
- Gestion des erreurs API (503, timeouts)
- Fallback automatique si docker CLI manquant

#### Solution 2: Utiliser une Image Locale Pré-pullée

Si les erreurs persistent, pré-charger l'image sur le serveur :

```bash
# SSH sur le serveur Hostinger
ssh root@srv991054.hstgr.cloud

# Puller l'image manuellement
docker pull docker:24-cli

# Vérifier que l'image est présente
docker images | grep docker
```

Ensuite modifier `.gitlab-ci.yml` pour utiliser `pull_policy: if-not-present` :

```yaml
build:image:
  image: docker:24-cli
  variables:
    DOCKER_PULL_POLICY: if-not-present
```

#### Solution 3: Utiliser un Registry Alternatif

Utiliser un miroir Docker ou un registry interne :

```yaml
# Option A: Quay.io mirror
build:image:
  image: quay.io/docker/docker:24-cli

# Option B: GitLab Container Registry (si disponible)
build:image:
  image: registry.gitlab.com/your-namespace/docker:24-cli
```

#### Solution 4: Désactiver l'Image Helper GitLab

Modifier la configuration du Runner pour réduire les pulls :

```bash
# Sur le serveur, éditer /etc/gitlab-runner/config.toml
nano /etc/gitlab-runner/config.toml

# Ajouter dans la section [[runners]]
[runners.docker]
  pull_policy = ["if-not-present", "always"]
  helper_image = "registry.gitlab.com/gitlab-org/gitlab-runner/gitlab-runner-helper:x86_64-v18.4.0"
```

Puis redémarrer le runner :
```bash
systemctl restart gitlab-runner
```

#### Solution 5: Rate Limiting Docker Hub

Si vous dépassez la limite Docker Hub (100 pulls/6h pour IP non authentifiée) :

**Option A: S'authentifier à Docker Hub**

```yaml
build:image:
  before_script:
    - echo "$DOCKERHUB_PASSWORD" | docker login -u "$DOCKERHUB_USERNAME" --password-stdin
```

Définir les variables dans GitLab CI/CD Variables :
- `DOCKERHUB_USERNAME`: votre username Docker Hub
- `DOCKERHUB_PASSWORD`: votre token (Settings → Security → New Access Token)

**Option B: Vérifier votre quota**

```bash
# Vérifier les limites actuelles
TOKEN=$(curl -s "https://auth.docker.io/token?service=registry.docker.io&scope=repository:ratelimitpreview/test:pull" | jq -r .token)
curl -s -H "Authorization: Bearer $TOKEN" https://registry-1.docker.io/v2/ratelimitpreview/test/manifests/latest -I | grep -i ratelimit
```

Output attendu :
```
ratelimit-limit: 100;w=21600       # 100 pulls par 6 heures
ratelimit-remaining: 95;w=21600    # 95 pulls restants
```

---

## Autres Problèmes CI/CD Courants

### Erreur: "Volume PostgreSQL Already Exists"

Si vous voulez réinitialiser complètement PostgreSQL :

1. Définir la variable GitLab CI/CD : `FORCE_POSTGRES_RESET=true`
2. Push un commit sur `main`
3. Le pipeline supprimera `/srv/brgm-data/dagster_pg` automatiquement
4. **IMPORTANT**: Redéfinir `FORCE_POSTGRES_RESET=false` après pour éviter suppressions accidentelles

### Erreur: "rsync: Failed to Copy Files"

Si rsync échoue lors du build :

```bash
# SSH sur le serveur
ssh root@srv991054.hstgr.cloud

# Vérifier les permissions
ls -la /srv/brgm
chown -R gitlab-runner:gitlab-runner /srv/brgm

# Créer le répertoire si manquant
mkdir -p /srv/brgm
chmod 755 /srv/brgm
```

### Erreur: "Docker Compose Command Not Found"

Installer `docker-compose` sur le runner :

```yaml
before_script:
  - apk add --no-cache bash docker-compose
```

Ou utiliser `docker compose` (plugin V2 intégré) :

```yaml
script:
  - docker compose -f docker-compose.production.yml up -d
```

---

## Vérification du Runner GitLab

### Status du Runner

```bash
# Sur le serveur
systemctl status gitlab-runner

# Logs en temps réel
journalctl -u gitlab-runner -f
```

### Vérifier la Configuration

```bash
cat /etc/gitlab-runner/config.toml

# Doit contenir le tag "hubeau"
[[runners]]
  name = "hubeau"
  tags = ["hubeau"]
```

### Test Manuel d'un Job

Vous pouvez tester localement sans GitLab :

```bash
cd /srv/brgm

# Simuler le build
docker build -f docker/orchestrator/Dockerfile -t hubeau-orchestrator:test .

# Simuler le déploiement
export DAGSTER_PG_PASSWORD="your_password"
export MINIO_PASS="your_password"
export PG_PASSWORD="your_password"
docker compose -f docker-compose.production.yml up -d
```

---

## Monitoring GitLab CI/CD

### Variables d'Environnement Obligatoires

Vérifier dans **Settings → CI/CD → Variables** que ces variables sont définies :

| Variable | Description | Masked | Protected |
|----------|-------------|--------|-----------|
| `DAGSTER_PG_PASSWORD` | Password PostgreSQL Dagster | ✅ | ✅ |
| `MINIO_PASS` | Password MinIO | ✅ | ✅ |
| `PG_PASSWORD` | Password PostgreSQL Hub'Eau | ✅ | ✅ |
| `MINIO_USER` | Username MinIO (optional) | ❌ | ✅ |
| `FORCE_POSTGRES_RESET` | Reset PostgreSQL (optional) | ❌ | ❌ |

### Logs Utiles Après Déploiement

```bash
# Logs Dagster Webserver
docker logs brgm-dagster-webserver --tail 100 -f

# Logs DLT Worker
docker logs brgm-dlt-worker --tail 100 -f

# Logs PostgreSQL Dagster
docker logs brgm-dagster-postgres --tail 50

# Logs MinIO (si utilisé)
docker logs brgm-minio --tail 50

# Status de tous les conteneurs
docker compose -f /srv/brgm/docker-compose.production.yml ps
```

---

## Résolution Rapide: Checklist

Si le pipeline échoue, vérifier dans l'ordre :

1. ✅ **Erreur 503 Docker Hub** → Relancer le pipeline (retry automatique activé)
2. ✅ **Variables manquantes** → Vérifier GitLab CI/CD Variables
3. ✅ **Runner offline** → `systemctl status gitlab-runner`
4. ✅ **Permissions /srv/brgm** → `chown -R gitlab-runner:gitlab-runner /srv/brgm`
5. ✅ **Espace disque** → `df -h /srv`
6. ✅ **Network issues** → `ping registry-1.docker.io`
7. ✅ **Docker daemon** → `systemctl status docker`

---

## Contact Support

Si le problème persiste après avoir testé ces solutions :

1. **Logs GitLab Runner**: `journalctl -u gitlab-runner --since "1 hour ago"`
2. **Screenshot de l'erreur** dans GitLab CI/CD
3. **Output de**: `docker info` et `docker version`
4. **Configuration Runner**: `cat /etc/gitlab-runner/config.toml`

Ces informations permettront d'identifier rapidement la cause du problème.
