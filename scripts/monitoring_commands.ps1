# Script PowerShell pour les commandes de monitoring Hub'Eau
# Usage: .\scripts\monitoring_commands.ps1

Write-Host "🎯 HUB'EAU MONITORING COMMANDS" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan

# Fonction pour vérifier si Docker est démarré
function Test-DockerRunning {
    try {
        docker ps | Out-Null
        return $true
    }
    catch {
        return $false
    }
}

# Fonction pour exécuter des commandes Dagster
function Invoke-DagsterCommand {
    param(
        [string]$Command,
        [string]$Description
    )
    
    Write-Host "`n📋 $Description" -ForegroundColor Yellow
    Write-Host "Commande: $Command" -ForegroundColor Gray
    
    if (Test-DockerRunning) {
        try {
            Invoke-Expression $Command
            Write-Host "✅ Commande exécutée avec succès" -ForegroundColor Green
        }
        catch {
            Write-Host "❌ Erreur lors de l'exécution: $($_.Exception.Message)" -ForegroundColor Red
        }
    }
    else {
        Write-Host "❌ Docker n'est pas démarré. Veuillez démarrer Docker Desktop." -ForegroundColor Red
    }
}

# Menu principal
function Show-Menu {
    Write-Host "`n🔧 OPTIONS DISPONIBLES:" -ForegroundColor White
    Write-Host "1. Monitoring rapide (métriques essentielles)" -ForegroundColor Green
    Write-Host "2. Monitoring qualité des données complet" -ForegroundColor Green
    Write-Host "3. Monitoring performance système" -ForegroundColor Green
    Write-Host "4. Dashboard complet de monitoring" -ForegroundColor Green
    Write-Host "5. Rapport exécutif" -ForegroundColor Green
    Write-Host "6. Vérifier l'état des services" -ForegroundColor Blue
    Write-Host "7. Nettoyer les tables parasites" -ForegroundColor Orange
    Write-Host "8. Redémarrer les services" -ForegroundColor Orange
    Write-Host "9. Voir les logs Dagster" -ForegroundColor Blue
    Write-Host "0. Quitter" -ForegroundColor Red
    Write-Host ""
}

# Vérifier l'état des services
function Test-ServicesStatus {
    Write-Host "`n🔍 VÉRIFICATION DE L'ÉTAT DES SERVICES" -ForegroundColor Cyan
    
    if (Test-DockerRunning) {
        Write-Host "✅ Docker est démarré" -ForegroundColor Green
        
        # Vérifier les conteneurs
        $containers = docker ps --format "table {{.Names}}\t{{.Status}}"
        Write-Host "`n📦 Conteneurs actifs:" -ForegroundColor Yellow
        Write-Host $containers
        
        # Vérifier PostgreSQL
        try {
            $pgStatus = docker exec postgres pg_isready -U postgres
            Write-Host "✅ PostgreSQL: $pgStatus" -ForegroundColor Green
        }
        catch {
            Write-Host "❌ PostgreSQL: Non accessible" -ForegroundColor Red
        }
        
        # Vérifier Dagster
        try {
            $dagsterStatus = docker exec dagster dagster-daemon status
            Write-Host "✅ Dagster: Accessible" -ForegroundColor Green
        }
        catch {
            Write-Host "❌ Dagster: Non accessible" -ForegroundColor Red
        }
    }
    else {
        Write-Host "❌ Docker n'est pas démarré" -ForegroundColor Red
        Write-Host "Veuillez démarrer Docker Desktop et relancer ce script." -ForegroundColor Yellow
    }
}

# Nettoyer les tables parasites
function Clear-ParasiteTables {
    Write-Host "`n🧹 NETTOYAGE DES TABLES PARASITES" -ForegroundColor Cyan
    
    if (Test-DockerRunning) {
        try {
            Write-Host "Exécution du script de nettoyage..." -ForegroundColor Yellow
            
            # Exécuter le script de nettoyage via Docker
            docker exec postgres psql -U postgres -d postgres -c "
            DO \$\$ 
            DECLARE
                r RECORD;
            BEGIN
                FOR r IN (
                    SELECT schemaname, tablename 
                    FROM pg_tables 
                    WHERE tablename LIKE '%__geometry__coordinates%'
                    AND schemaname = 'hubeau'
                ) LOOP
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.schemaname) || '.' || quote_ident(r.tablename) || ' CASCADE';
                    RAISE NOTICE 'Supprimé: %.%', r.schemaname, r.tablename;
                END LOOP;
            END \$\$;"
            
            Write-Host "✅ Tables parasites nettoyées" -ForegroundColor Green
        }
        catch {
            Write-Host "❌ Erreur lors du nettoyage: $($_.Exception.Message)" -ForegroundColor Red
        }
    }
    else {
        Write-Host "❌ Docker n'est pas démarré" -ForegroundColor Red
    }
}

# Redémarrer les services
function Restart-Services {
    Write-Host "`n🔄 REDÉMARRAGE DES SERVICES" -ForegroundColor Cyan
    
    if (Test-DockerRunning) {
        try {
            Write-Host "Arrêt des services..." -ForegroundColor Yellow
            docker-compose down
            
            Write-Host "Démarrage des services..." -ForegroundColor Yellow
            docker-compose up -d
            
            Write-Host "✅ Services redémarrés" -ForegroundColor Green
            Write-Host "Attendez quelques secondes que les services soient prêts..." -ForegroundColor Yellow
            Start-Sleep -Seconds 10
        }
        catch {
            Write-Host "❌ Erreur lors du redémarrage: $($_.Exception.Message)" -ForegroundColor Red
        }
    }
    else {
        Write-Host "❌ Docker n'est pas démarré" -ForegroundColor Red
    }
}

# Voir les logs Dagster
function Show-DagsterLogs {
    Write-Host "`n📋 LOGS DAGSTER" -ForegroundColor Cyan
    
    if (Test-DockerRunning) {
        try {
            Write-Host "Derniers logs Dagster (50 lignes):" -ForegroundColor Yellow
            docker logs dagster --tail 50
        }
        catch {
            Write-Host "❌ Impossible de récupérer les logs: $($_.Exception.Message)" -ForegroundColor Red
        }
    }
    else {
        Write-Host "❌ Docker n'est pas démarré" -ForegroundColor Red
    }
}

# Boucle principale
do {
    Show-Menu
    $choice = Read-Host "Choisissez une option (0-9)"
    
    switch ($choice) {
        "1" { 
            Invoke-DagsterCommand "docker exec dagster dagster asset materialize --select quick_monitoring" "Monitoring rapide"
        }
        "2" { 
            Invoke-DagsterCommand "docker exec dagster dagster asset materialize --select data_quality_monitoring" "Monitoring qualité des données"
        }
        "3" { 
            Invoke-DagsterCommand "docker exec dagster dagster asset materialize --select performance_monitoring" "Monitoring performance"
        }
        "4" { 
            Invoke-DagsterCommand "docker exec dagster dagster asset materialize --select hubeau_monitoring_dashboard" "Dashboard complet"
        }
        "5" { 
            Invoke-DagsterCommand "docker exec dagster dagster asset materialize --select executive_summary_report" "Rapport exécutif"
        }
        "6" { 
            Test-ServicesStatus
        }
        "7" { 
            Clear-ParasiteTables
        }
        "8" { 
            Restart-Services
        }
        "9" { 
            Show-DagsterLogs
        }
        "0" { 
            Write-Host "`n👋 Au revoir !" -ForegroundColor Cyan
            break
        }
        default { 
            Write-Host "❌ Option invalide. Veuillez choisir entre 0 et 9." -ForegroundColor Red
        }
    }
    
    if ($choice -ne "0") {
        Write-Host "`nAppuyez sur Entrée pour continuer..." -ForegroundColor Gray
        Read-Host
    }
    
} while ($choice -ne "0")

Write-Host "`n🎯 Script de monitoring terminé." -ForegroundColor Cyan
