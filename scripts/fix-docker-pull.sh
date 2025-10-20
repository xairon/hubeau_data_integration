#!/bin/bash
# Script de fallback pour résoudre les erreurs 503 Docker Hub
# Usage: ./scripts/fix-docker-pull.sh [image_name]

set -e

IMAGE_NAME=${1:-"alpine:3.19"}
MAX_RETRIES=5
RETRY_DELAY=30

echo "🔧 Tentative de résolution des erreurs Docker Hub pour $IMAGE_NAME"

# Fonction pour tester la connectivité Docker Hub
test_docker_hub() {
    echo "🌐 Test de connectivité Docker Hub..."
    if curl -s --max-time 10 https://registry-1.docker.io/v2/ > /dev/null; then
        echo "✅ Docker Hub accessible"
        return 0
    else
        echo "❌ Docker Hub non accessible"
        return 1
    fi
}

# Fonction pour nettoyer le cache Docker
clean_docker_cache() {
    echo "🧹 Nettoyage du cache Docker..."
    docker system prune -f || true
    docker builder prune -f || true
}

# Fonction pour essayer différents registres
try_alternative_registries() {
    local image=$1
    local base_image=$(echo $image | cut -d: -f1)
    local tag=$(echo $image | cut -d: -f2)
    
    echo "🔄 Tentative avec des registres alternatifs..."
    
    # Essayer GitHub Container Registry
    if docker pull ghcr.io/library/$base_image:$tag 2>/dev/null; then
        echo "✅ Image trouvée sur GitHub Container Registry"
        docker tag ghcr.io/library/$base_image:$tag $image
        return 0
    fi
    
    # Essayer Quay.io
    if docker pull quay.io/$base_image:$tag 2>/dev/null; then
        echo "✅ Image trouvée sur Quay.io"
        docker tag quay.io/$base_image:$tag $image
        return 0
    fi
    
    return 1
}

# Fonction principale de retry
pull_with_retry() {
    local image=$1
    local attempt=1
    
    while [ $attempt -le $MAX_RETRIES ]; do
        echo "🔄 Tentative $attempt/$MAX_RETRIES pour $image..."
        
        if docker pull $image; then
            echo "✅ Image $image téléchargée avec succès"
            return 0
        else
            echo "❌ Échec de la tentative $attempt"
            
            if [ $attempt -lt $MAX_RETRIES ]; then
                echo "⏳ Attente de $RETRY_DELAY secondes avant la prochaine tentative..."
                sleep $RETRY_DELAY
                
                # Nettoyer le cache toutes les 2 tentatives
                if [ $((attempt % 2)) -eq 0 ]; then
                    clean_docker_cache
                fi
            fi
            
            attempt=$((attempt + 1))
        fi
    done
    
    echo "❌ Échec après $MAX_RETRIES tentatives"
    return 1
}

# Exécution principale
main() {
    echo "🚀 Début du script de résolution Docker Hub"
    
    # Test de connectivité
    if ! test_docker_hub; then
        echo "⚠️  Docker Hub non accessible, tentative de registres alternatifs..."
        if try_alternative_registries $IMAGE_NAME; then
            echo "✅ Image obtenue depuis un registre alternatif"
            exit 0
        else
            echo "❌ Aucun registre alternatif disponible"
            exit 1
        fi
    fi
    
    # Tentative de pull avec retry
    if pull_with_retry $IMAGE_NAME; then
        echo "✅ Script terminé avec succès"
        exit 0
    else
        echo "❌ Script échoué après toutes les tentatives"
        exit 1
    fi
}

# Exécuter le script principal
main "$@"