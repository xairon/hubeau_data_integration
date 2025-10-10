#!/bin/bash

# Script de test pour les optimisations mémoire
# Auteur: Assistant AI
# Date: 2025-10-10

set -e  # Arrêter en cas d'erreur

echo "======================================"
echo "🧪 Test des Optimisations Mémoire"
echo "======================================"
echo ""

# Couleurs pour l'affichage
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Fonction pour afficher les logs avec couleur
log_info() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Vérifier que Dagster est accessible
echo "1️⃣  Vérification de l'accès à Dagster..."
if ! command -v dagster &> /dev/null; then
    log_error "Dagster n'est pas installé ou pas dans le PATH"
    exit 1
fi
log_info "Dagster est accessible"
echo ""

# Tester quality_rivers_stations (celui qui crashait)
echo "2️⃣  Test du job quality_rivers_stations_reference..."
echo "   Ce job crashait au slice 22/107 avant les optimisations"
echo ""

# Lancer le job en arrière-plan et capturer le PID
log_info "Lancement du job..."
dagster job execute \
  -m src.hubeau_pipeline.definitions \
  -j hubeau_quality_rivers_job \
  --config '{"ops": {"quality_rivers_stations_reference": {"config": {}}}}' \
  > /tmp/dagster_quality_rivers.log 2>&1 &

JOB_PID=$!

# Surveiller la mémoire pendant l'exécution
echo ""
log_info "Surveillance de la mémoire (PID: $JOB_PID)..."
echo ""

# Fonction pour obtenir la mémoire du processus
get_memory() {
    ps -p $1 -o rss= 2>/dev/null || echo "0"
}

# Surveiller pendant 5 minutes maximum
MAX_ITERATIONS=300
ITERATION=0
MAX_MEMORY=0
PREV_MEMORY=0

while kill -0 $JOB_PID 2>/dev/null; do
    MEMORY=$(get_memory $JOB_PID)
    MEMORY_MB=$((MEMORY / 1024))
    
    if [ $MEMORY_MB -gt $MAX_MEMORY ]; then
        MAX_MEMORY=$MEMORY_MB
    fi
    
    # Afficher seulement les changements significatifs
    if [ $((MEMORY_MB - PREV_MEMORY)) -gt 50 ] || [ $((PREV_MEMORY - MEMORY_MB)) -gt 50 ]; then
        echo "   Mémoire actuelle: ${MEMORY_MB} MB (pic: ${MAX_MEMORY} MB)"
        PREV_MEMORY=$MEMORY_MB
    fi
    
    sleep 1
    ITERATION=$((ITERATION + 1))
    
    if [ $ITERATION -gt $MAX_ITERATIONS ]; then
        log_warning "Timeout après 5 minutes"
        kill $JOB_PID 2>/dev/null || true
        break
    fi
done

wait $JOB_PID
EXIT_CODE=$?

echo ""
echo "======================================"
echo "📊 Résultats du Test"
echo "======================================"
echo ""

# Analyser les logs
if grep -q "SIGKILL\|signal 9" /tmp/dagster_quality_rivers.log; then
    log_error "Le processus a été tué (SIGKILL) - Problème mémoire persistant"
    EXIT_CODE=1
elif grep -q "Slice 107/107\|terminé.*107 slices" /tmp/dagster_quality_rivers.log; then
    log_info "Tous les slices (107/107) ont été traités avec succès !"
    EXIT_CODE=0
else
    log_warning "Le job s'est terminé mais impossible de vérifier le traitement complet"
fi

# Afficher les statistiques
echo ""
echo "Statistiques mémoire:"
echo "  • Pic de mémoire: ${MAX_MEMORY} MB"

if [ $MAX_MEMORY -lt 2000 ]; then
    log_info "Consommation mémoire normale (< 2 GB)"
elif [ $MAX_MEMORY -lt 4000 ]; then
    log_warning "Consommation mémoire élevée (2-4 GB)"
else
    log_error "Consommation mémoire excessive (> 4 GB)"
fi

echo ""
echo "Dernières lignes du log Dagster:"
tail -n 20 /tmp/dagster_quality_rivers.log

echo ""
echo "======================================"

if [ $EXIT_CODE -eq 0 ]; then
    log_info "Test réussi ! Les optimisations fonctionnent correctement."
    echo ""
    log_info "Prochaines étapes recommandées:"
    echo "  1. Tester les autres jobs (sync_all_stations, etc.)"
    echo "  2. Surveiller la mémoire en production"
    echo "  3. Ajuster les paramètres si nécessaire (voir docs/OPTIMISATION_MEMOIRE.md)"
else
    log_error "Test échoué. Consultez les logs pour plus de détails."
    echo ""
    log_warning "Actions possibles:"
    echo "  1. Réduire la taille des batches (configs/hubeau/quality_rivers_stations.yml)"
    echo "  2. Augmenter la mémoire disponible (docker-compose.yml)"
    echo "  3. Consulter docs/OPTIMISATION_MEMOIRE.md pour plus d'options"
fi

echo ""
echo "Log complet disponible: /tmp/dagster_quality_rivers.log"
echo "======================================"

exit $EXIT_CODE

