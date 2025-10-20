#!/bin/bash
# Script d'installation de Portainer sur le serveur
# Usage: bash setup-portainer.sh

set -e

echo "================================================"
echo "🐳 Installation de Portainer CE"
echo "================================================"
echo ""

# Couleurs
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Vérifier si Portainer est déjà installé
if docker ps -a | grep -q portainer; then
    echo -e "${YELLOW}⚠️  Portainer est déjà installé${NC}"
    echo ""
    read -p "Voulez-vous le réinstaller ? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Installation annulée"
        exit 0
    fi

    echo "🛑 Arrêt et suppression de l'ancien Portainer..."
    docker stop portainer || true
    docker rm portainer || true
fi

# Vérifier si le fichier portainer-compose.yml existe
if [ ! -f "portainer-compose.yml" ]; then
    echo -e "${RED}❌ Fichier portainer-compose.yml non trouvé${NC}"
    echo "Assurez-vous d'être dans le répertoire /srv/brgm"
    exit 1
fi

echo "📦 Pulling de l'image Portainer..."
docker pull portainer/portainer-ce:2.19.4-alpine || {
    echo -e "${YELLOW}⚠️  Pull depuis Docker Hub échoué, tentative avec cache local...${NC}"
}

echo ""
echo "🚀 Démarrage de Portainer..."
docker compose -f portainer-compose.yml up -d

echo ""
echo "⏳ Attente du démarrage (15 secondes)..."
sleep 15

echo ""
echo "🔍 Vérification du statut..."
docker ps | grep portainer

echo ""
echo "================================================"
echo -e "${GREEN}✅ Portainer installé avec succès !${NC}"
echo "================================================"
echo ""
echo "📍 Accès à l'interface:"
echo "  → URL: https://srv991054.hstgr.cloud:9443"
echo "  → Alternative: https://$(hostname -I | awk '{print $1}'):9443"
echo ""
echo "🔐 Première connexion:"
echo "  1. Ouvrez https://srv991054.hstgr.cloud:9443 dans votre navigateur"
echo "  2. Acceptez le certificat auto-signé (normal)"
echo "  3. Créez un compte admin (username + password)"
echo "  4. Sélectionnez 'Get Started' pour gérer l'environnement local"
echo ""
echo "💡 Conseils:"
echo "  - Le mot de passe admin doit faire au moins 12 caractères"
echo "  - Notez bien vos identifiants, vous ne pourrez pas les récupérer"
echo "  - Portainer redémarre automatiquement avec le serveur"
echo ""
echo "📚 Documentation:"
echo "  → https://docs.portainer.io/"
echo ""

# Test de santé
echo "🏥 Test de santé du conteneur..."
sleep 5
HEALTH=$(docker inspect portainer --format='{{.State.Health.Status}}' 2>/dev/null || echo "no healthcheck")

if [ "$HEALTH" = "healthy" ]; then
    echo -e "${GREEN}✅ Portainer est healthy${NC}"
elif [ "$HEALTH" = "starting" ]; then
    echo -e "${YELLOW}⏳ Portainer est en cours de démarrage (status: starting)${NC}"
    echo "   Attendez quelques secondes et vérifiez manuellement"
else
    echo -e "${YELLOW}⚠️  Health check: $HEALTH${NC}"
    echo "   Vérifiez les logs: docker logs portainer"
fi

echo ""
echo "🔧 Commandes utiles:"
echo "  - Voir les logs:     docker logs portainer -f"
echo "  - Arrêter:           docker stop portainer"
echo "  - Redémarrer:        docker restart portainer"
echo "  - Supprimer:         docker stop portainer && docker rm portainer"
echo ""