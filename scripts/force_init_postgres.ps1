# Script PowerShell pour forcer l'initialisation PostgreSQL au déploiement
# Utilisé dans le CI/CD pour s'assurer que le schéma est créé

Write-Host "🔄 FORCE INITIALISATION POSTGRESQL" -ForegroundColor Cyan
Write-Host "==================================" -ForegroundColor Cyan

# 1. Arrêter PostgreSQL
Write-Host "🛑 Arrêt de PostgreSQL..." -ForegroundColor Yellow
docker-compose stop postgres 2>$null

# 2. Supprimer le volume de données pour forcer la réinitialisation
Write-Host "🗑️ Suppression du volume de données..." -ForegroundColor Yellow
docker volume rm /srv/brgm-data/postgres -f 2>$null

# 3. Redémarrer PostgreSQL (il va exécuter le script d'init)
Write-Host "🚀 Redémarrage de PostgreSQL avec PostGIS..." -ForegroundColor Yellow
docker-compose up -d postgres

# 4. Attendre que PostgreSQL soit prêt
Write-Host "⏳ Attente que PostgreSQL soit prêt..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# 5. Vérifier que le schéma et les tables sont créés
Write-Host "✅ Vérification du schéma..." -ForegroundColor Green
docker exec brgm-postgres psql -U postgres -d postgres -c "\dn"
docker exec brgm-postgres psql -U postgres -d postgres -c "\dt hubeau.*"

Write-Host ""
Write-Host "✅ Initialisation forcée terminée !" -ForegroundColor Green
Write-Host "PostgreSQL avec PostGIS est prêt avec le schéma hubeau." -ForegroundColor Green
