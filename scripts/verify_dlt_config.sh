#!/bin/bash
# Vérifie que la configuration DLT est bien chargée dans les conteneurs

echo "🔍 Vérification de la configuration DLT..."

echo ""
echo "📁 Vérification du fichier config.toml local:"
if [ -f ".dlt/config.toml" ]; then
    echo "✅ Fichier .dlt/config.toml existe"
    echo "📊 Contenu du fichier:"
    cat .dlt/config.toml | head -20
else
    echo "❌ Fichier .dlt/config.toml INTROUVABLE"
fi

echo ""
echo "🐳 Vérification dans le conteneur Dagster daemon:"
if docker exec brgm-dagster-daemon test -f /app/.dlt/config.toml; then
    echo "✅ Fichier /app/.dlt/config.toml existe dans le conteneur"
    echo ""
    echo "📊 Configuration workers:"
    docker exec brgm-dagster-daemon cat /app/.dlt/config.toml | grep -A 2 "\[normalize\]"
    docker exec brgm-dagster-daemon cat /app/.dlt/config.toml | grep -A 2 "\[load\]"
    echo ""
    echo "📊 Configuration buffering:"
    docker exec brgm-dagster-daemon cat /app/.dlt/config.toml | grep -A 4 "\[data_writer\]"
else
    echo "❌ Fichier /app/.dlt/config.toml INTROUVABLE dans le conteneur"
    echo "⚠️  Le volume .dlt n'est probablement pas monté correctement"
fi

echo ""
echo "✅ Vérification terminée"

