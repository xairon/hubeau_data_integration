#!/bin/bash
# Script pour ajouter le volume /srv/brgm au GitLab Runner
# À exécuter sur le serveur VPS

set -e

echo "🔧 Configuration du GitLab Runner pour accès à /srv/brgm"

CONFIG_FILE="/etc/gitlab-runner/config.toml"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "❌ Fichier de config runner non trouvé: $CONFIG_FILE"
    exit 1
fi

echo "📝 Backup de la configuration actuelle..."
cp $CONFIG_FILE ${CONFIG_FILE}.backup.$(date +%Y%m%d_%H%M%S)

echo "🔍 Vérification des volumes actuels..."
grep -A 10 "\[runners.docker\]" $CONFIG_FILE || true

echo ""
echo "⚠️  Ajoutez manuellement ce volume dans $CONFIG_FILE :"
echo ""
echo '    volumes = ['
echo '      "/var/run/docker.sock:/var/run/docker.sock",'
echo '      "/srv/brgm:/srv/brgm",'
echo '      "/srv/brgm-data:/srv/brgm-data"'
echo '    ]'
echo ""
echo "Puis redémarrez le runner avec:"
echo "  sudo gitlab-runner restart"
echo "  sudo gitlab-runner verify"

