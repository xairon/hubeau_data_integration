#!/bin/bash
# Script pour résoudre les erreurs 503 Docker Hub sur GitLab Runner
# Usage: bash fix-docker-pull.sh

set -e

echo "================================================"
echo "🔧 Fix pour erreurs 503 Docker Hub"
echo "================================================"

# Couleurs pour output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 1. Pré-pull des images Docker critiques
echo ""
echo "📦 Étape 1: Pré-pull des images Docker..."
echo "----------------------------------------"

IMAGES=(
  "docker:24-cli"
  "docker:24-dind"
  "docker:24"
  "docker:latest"
  "alpine:3.18"
  "alpine:latest"
  "registry.gitlab.com/gitlab-org/gitlab-runner/gitlab-runner-helper:x86_64-v18.4.0"
)

FAILED_IMAGES=()

for image in "${IMAGES[@]}"; do
  echo -n "  Pulling $image... "
  if docker pull "$image" > /dev/null 2>&1; then
    echo -e "${GREEN}✅ OK${NC}"
  else
    echo -e "${YELLOW}⚠️  Failed (will use fallback)${NC}"
    FAILED_IMAGES+=("$image")
  fi
done

# 2. Créer un script de fallback pour images manquantes
if [ ${#FAILED_IMAGES[@]} -gt 0 ]; then
  echo ""
  echo -e "${YELLOW}⚠️  Certaines images n'ont pas pu être téléchargées:${NC}"
  for img in "${FAILED_IMAGES[@]}"; do
    echo "   - $img"
  done
  echo ""
  echo "  Création d'un script de retry..."

  cat > /tmp/retry-pull.sh <<'EOF'
#!/bin/bash
# Retry script pour images manquantes
for i in {1..5}; do
  echo "Tentative $i/5..."
  SUCCESS=true
EOF

  for img in "${FAILED_IMAGES[@]}"; do
    echo "  docker pull \"$img\" || SUCCESS=false" >> /tmp/retry-pull.sh
  done

  cat >> /tmp/retry-pull.sh <<'EOF'
  if [ "$SUCCESS" = "true" ]; then
    echo "✅ Toutes les images récupérées!"
    exit 0
  fi
  echo "Attente 30 secondes avant retry..."
  sleep 30
done
echo "❌ Impossible de récupérer toutes les images après 5 tentatives"
exit 1
EOF

  chmod +x /tmp/retry-pull.sh
  echo -e "  ${GREEN}Script de retry créé: /tmp/retry-pull.sh${NC}"
fi

# 3. Configuration du GitLab Runner
echo ""
echo "⚙️  Étape 2: Configuration du GitLab Runner..."
echo "----------------------------------------"

RUNNER_CONFIG="/etc/gitlab-runner/config.toml"

if [ -f "$RUNNER_CONFIG" ]; then
  echo "  Configuration actuelle détectée"

  # Backup de la config
  cp "$RUNNER_CONFIG" "${RUNNER_CONFIG}.backup.$(date +%Y%m%d_%H%M%S)"
  echo -e "  ${GREEN}✅ Backup créé${NC}"

  # Vérifier si la config pull_policy existe déjà
  if grep -q "pull_policy" "$RUNNER_CONFIG"; then
    echo -e "  ${YELLOW}⚠️  pull_policy déjà configuré dans le runner${NC}"
  else
    echo "  Ajout de la configuration pull_policy..."

    # Créer un patch pour la configuration
    cat > /tmp/runner-config.patch <<'EOF'

  # Configuration pour éviter erreurs 503
  [runners.docker]
    pull_policy = ["if-not-present", "always"]
    allowed_pull_policies = ["if-not-present", "always", "never"]
EOF

    echo -e "  ${GREEN}✅ Patch de configuration créé${NC}"
    echo ""
    echo -e "  ${YELLOW}ACTION REQUISE:${NC}"
    echo "  Ajouter manuellement ces lignes dans $RUNNER_CONFIG:"
    echo "  ────────────────────────────────────────"
    cat /tmp/runner-config.patch
    echo "  ────────────────────────────────────────"
  fi
else
  echo -e "  ${YELLOW}⚠️  Fichier de config non trouvé: $RUNNER_CONFIG${NC}"
  echo "  Le runner n'est peut-être pas installé sur cette machine"
fi

# 4. Test de connexion Docker Hub
echo ""
echo "🔍 Étape 3: Test de connexion Docker Hub..."
echo "----------------------------------------"

echo -n "  Test de l'API Docker Hub... "
if curl -s -o /dev/null -w "%{http_code}" https://hub.docker.com/v2/ | grep -q "200\|401"; then
  echo -e "${GREEN}✅ Accessible${NC}"

  # Vérifier le rate limit
  echo ""
  echo "  Vérification du rate limit..."
  TOKEN=$(curl -s "https://auth.docker.io/token?service=registry.docker.io&scope=repository:ratelimitpreview/test:pull" | grep -o '"token":"[^"]*' | cut -d'"' -f4)

  if [ -n "$TOKEN" ]; then
    RATE_INFO=$(curl -s -H "Authorization: Bearer $TOKEN" -I https://registry-1.docker.io/v2/ratelimitpreview/test/manifests/latest 2>/dev/null | grep -i ratelimit)

    if [ -n "$RATE_INFO" ]; then
      echo "  Rate limit info:"
      echo "$RATE_INFO" | while read -r line; do
        echo "    $line"
      done
    else
      echo -e "  ${YELLOW}⚠️  Impossible de récupérer les infos de rate limit${NC}"
    fi
  fi
else
  echo -e "${RED}❌ Non accessible${NC}"
  echo "  Docker Hub semble être down ou bloqué"
fi

# 5. Créer un alias pour docker pull avec retry
echo ""
echo "🔧 Étape 4: Création d'un wrapper Docker avec retry..."
echo "----------------------------------------"

cat > /usr/local/bin/docker-pull-retry <<'EOF'
#!/bin/bash
# Wrapper pour docker pull avec retry automatique

IMAGE="$1"
MAX_ATTEMPTS=3
WAIT_TIME=10

for i in $(seq 1 $MAX_ATTEMPTS); do
  echo "Tentative $i/$MAX_ATTEMPTS pour $IMAGE..."

  if docker pull "$IMAGE"; then
    echo "✅ Image $IMAGE récupérée avec succès"
    exit 0
  fi

  if [ $i -lt $MAX_ATTEMPTS ]; then
    echo "⏳ Attente de ${WAIT_TIME}s avant retry..."
    sleep $WAIT_TIME
    WAIT_TIME=$((WAIT_TIME * 2))  # Exponential backoff
  fi
done

echo "❌ Impossible de récupérer $IMAGE après $MAX_ATTEMPTS tentatives"
exit 1
EOF

chmod +x /usr/local/bin/docker-pull-retry
echo -e "  ${GREEN}✅ Wrapper créé: /usr/local/bin/docker-pull-retry${NC}"

# 6. Instructions finales
echo ""
echo "================================================"
echo "✅ Script terminé!"
echo "================================================"
echo ""
echo "Actions suivantes recommandées:"
echo ""
echo "1. Si des images ont échoué, exécuter:"
echo -e "   ${YELLOW}bash /tmp/retry-pull.sh${NC}"
echo ""
echo "2. Redémarrer le GitLab Runner:"
echo -e "   ${YELLOW}systemctl restart gitlab-runner${NC}"
echo ""
echo "3. Pour utiliser le wrapper avec retry:"
echo -e "   ${YELLOW}docker-pull-retry docker:24-cli${NC}"
echo ""
echo "4. Relancer le pipeline dans GitLab"
echo ""

# Afficher un résumé
echo "📊 Résumé:"
echo "  - Images pré-pullées: $((${#IMAGES[@]} - ${#FAILED_IMAGES[@]}))/${#IMAGES[@]}"
if [ -f "$RUNNER_CONFIG" ]; then
  echo "  - Config runner: Backup créé"
fi
echo "  - Wrapper retry: Installé"

exit 0