#!/bin/bash
# Script d'URGENCE pour corriger le GitLab Runner qui force pull_policy=always

echo "🚨 SCRIPT D'URGENCE - Fix GitLab Runner Pull Policy"
echo "===================================================="
echo ""
echo "Ce script va forcer le GitLab Runner à utiliser les images locales"
echo ""

# 1. Pré-puller les images critiques
echo "📦 Étape 1: Pré-pull des images essentielles..."
docker pull alpine:3.18 || echo "⚠️  alpine:3.18 pull failed"
docker pull alpine:latest || echo "⚠️  alpine:latest pull failed"

# 2. Configurer le runner
echo ""
echo "⚙️  Étape 2: Configuration du GitLab Runner..."

RUNNER_CONFIG="/etc/gitlab-runner/config.toml"

if [ -f "$RUNNER_CONFIG" ]; then
    # Backup
    cp "$RUNNER_CONFIG" "${RUNNER_CONFIG}.backup.$(date +%Y%m%d_%H%M%S)"

    # Modifier la configuration pour forcer if-not-present
    echo ""
    echo "📝 Modification de la configuration..."

    # Vérifier si pull_policy existe
    if grep -q "pull_policy" "$RUNNER_CONFIG"; then
        # Remplacer pull_policy existant
        sed -i 's/pull_policy = .*/pull_policy = ["if-not-present", "never"]/' "$RUNNER_CONFIG"
        echo "✅ pull_policy modifié"
    else
        # Ajouter pull_policy dans la section [runners.docker]
        if grep -q "\[runners.docker\]" "$RUNNER_CONFIG"; then
            sed -i '/\[runners.docker\]/a\    pull_policy = ["if-not-present", "never"]' "$RUNNER_CONFIG"
            echo "✅ pull_policy ajouté"
        else
            echo "❌ Section [runners.docker] non trouvée"
            echo ""
            echo "Ajouter manuellement dans $RUNNER_CONFIG :"
            echo ""
            echo "[[runners]]"
            echo "  [runners.docker]"
            echo '    pull_policy = ["if-not-present", "never"]'
            echo '    allowed_pull_policies = ["if-not-present", "never", "always"]'
        fi
    fi
else
    echo "❌ Fichier $RUNNER_CONFIG non trouvé"
fi

# 3. Redémarrer le runner
echo ""
echo "🔄 Étape 3: Redémarrage du GitLab Runner..."
systemctl restart gitlab-runner
sleep 2
systemctl status gitlab-runner --no-pager | head -10

echo ""
echo "===================================================="
echo "✅ Script terminé!"
echo ""
echo "Le pipeline utilise maintenant alpine:3.18 au lieu de docker:24-cli"
echo "Cela devrait contourner le problème de Docker Hub 503"
echo ""
echo "Relancez le pipeline dans GitLab pour tester"
echo "===================================================="