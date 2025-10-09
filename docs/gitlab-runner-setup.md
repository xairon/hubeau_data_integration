# Configuration GitLab Runner sur VPS Hostinger

## 1. Installation du GitLab Runner sur le VPS

```bash
# Se connecter au VPS
ssh root@srv991054.hstgr.cloud

# Installer GitLab Runner
curl -L "https://packages.gitlab.com/install/repositories/runner/gitlab-runner/script.deb.sh" | sudo bash
sudo apt-get install gitlab-runner

# Vérifier l'installation
gitlab-runner --version
```

## 2. Enregistrer le Runner avec votre projet GitLab

```bash
# Obtenir le token depuis GitLab:
# Settings > CI/CD > Runners > New project runner

# Enregistrer le runner
sudo gitlab-runner register \
  --url https://gitlab.com/ \
  --registration-token VOTRE_TOKEN_ICI \
  --executor docker \
  --docker-image docker:24-dind \
  --docker-privileged \
  --docker-volumes /var/run/docker.sock:/var/run/docker.sock \
  --docker-volumes /srv/brgm-data:/srv/brgm-data \
  --description "Hostinger VPS Runner" \
  --tag-list "vps,production,auto-deploy"

# Démarrer le runner
sudo gitlab-runner start
```

## 3. Configuration du Runner

Éditer `/etc/gitlab-runner/config.toml`:

```toml
concurrent = 1  # Limiter à 1 job à la fois (VPS 1 vCPU)

[[runners]]
  name = "Hostinger VPS Runner"
  url = "https://gitlab.com/"
  token = "VOTRE_TOKEN"
  executor = "docker"
  [runners.docker]
    tls_verify = false
    image = "docker:24-dind"
    privileged = true
    disable_cache = false
    volumes = [
      "/var/run/docker.sock:/var/run/docker.sock",
      "/srv/brgm-data:/srv/brgm-data"
    ]
    shm_size = 0
  [runners.cache]
    [runners.cache.s3]
    [runners.cache.gcs]
```

## 4. Redémarrer le runner

```bash
sudo gitlab-runner restart
sudo gitlab-runner verify
```

## 5. Vérification

Dans GitLab: Settings > CI/CD > Runners
Vous devriez voir votre runner avec un point vert.
