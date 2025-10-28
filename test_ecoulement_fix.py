#!/usr/bin/env python3
"""
Script de test pour vérifier la correction du problème de format Hub'Eau
Test de l'ingestion ecoulement_stations_csv avec la nouvelle logique robuste
"""

import requests
import time
import json
from datetime import datetime

def test_api_response_format():
    """Teste le format de réponse de l'API Hub'Eau"""
    print("🔍 Test du format de réponse de l'API Hub'Eau...")
    
    try:
        # Test endpoint CSV
        csv_url = "https://hubeau.eaufrance.fr/api/v1/ecoulement/stations.csv"
        response = requests.get(csv_url, params={'page': 1}, timeout=10)
        
        if response.status_code == 200:
            content = response.text[:200]  # Premiers 200 caractères
            print(f"✅ API CSV accessible")
            print(f"📄 Format détecté: {'CSV' if ';' in content else 'Autre'}")
            print(f"📝 Début du contenu: {content[:100]}...")
            
            # Vérifier si c'est bien du CSV avec des colonnes attendues
            lines = response.text.split('\n')[:3]
            if len(lines) > 0:
                headers = lines[0].split(';')
                print(f"📋 Colonnes détectées ({len(headers)}): {headers[:5]}...")
                
                # Vérifier colonnes clés
                expected_cols = ['code_station', 'libelle_station', 'latitude', 'longitude']
                found_cols = [col for col in expected_cols if col in headers]
                print(f"✅ Colonnes clés trouvées: {found_cols}")
                
        else:
            print(f"❌ Erreur API: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Erreur test API: {e}")

def launch_dagster_job():
    """Lance le job ecoulement via l'API Dagster"""
    print("\n🚀 Lancement du job ecoulement_stations_csv via Dagster...")
    
    try:
        # Vérifier si Dagster est accessible
        health_response = requests.get("http://localhost:8080/graphql", timeout=5)
        if health_response.status_code != 200:
            print("❌ Dagster non accessible sur localhost:8080")
            return None
            
    except Exception as e:
        print(f"❌ Impossible de joindre Dagster: {e}")
        return None
    
    # Mutation GraphQL pour lancer le job
    mutation = """
    mutation LaunchEcoulementStations {
        launchRun(executionParams: {
            selector: { assetKeys: [{ path: ["ecoulement_stations_csv"] }] }
            runConfigData: {}
        }) {
            __typename
            ... on LaunchRunSuccess {
                run {
                    runId
                    status
                }
            }
            ... on RunConfigValidationInvalid {
                errors {
                    message
                }
            }
            ... on PythonError {
                message
                stack
            }
        }
    }
    """
    
    try:
        response = requests.post(
            "http://localhost:8080/graphql",
            json={"query": mutation},
            timeout=10
        )
        
        if response.status_code != 200:
            print(f"❌ Erreur HTTP: {response.status_code}")
            return None
            
        result = response.json()
        
        if "errors" in result:
            print(f"❌ Erreurs GraphQL: {result['errors']}")
            return None
            
        launch_result = result["data"]["launchRun"]
        
        if launch_result["__typename"] == "LaunchRunSuccess":
            run_id = launch_result["run"]["runId"]
            print(f"✅ Job lancé avec succès! Run ID: {run_id}")
            return run_id
        else:
            print(f"❌ Échec du lancement: {launch_result}")
            return None
            
    except Exception as e:
        print(f"❌ Erreur lancement job: {e}")
        return None

def monitor_job_progress(run_id, max_wait_minutes=10):
    """Monitore le progrès du job"""
    print(f"\n📊 Surveillance du job {run_id}...")
    
    start_time = time.time()
    max_wait_seconds = max_wait_minutes * 60
    
    query = f"""
    query RunStatus {{
        runOrError(runId: "{run_id}") {{
            ... on Run {{
                status
                stats {{
                    stepsSucceeded
                    stepsFailed
                }}
                logs(limit: 50) {{
                    nodes {{
                        message
                        level
                        timestamp
                    }}
                }}
            }}
        }}
    }}
    """
    
    while time.time() - start_time < max_wait_seconds:
        try:
            response = requests.post(
                "http://localhost:8080/graphql",
                json={"query": query},
                timeout=10
            )
            
            if response.status_code != 200:
                print(f"❌ Erreur monitoring: {response.status_code}")
                break
                
            result = response.json()
            run_data = result["data"]["runOrError"]
            
            status = run_data["status"]
            print(f"📈 Statut: {status}")
            
            # Analyser les logs récents
            logs = run_data.get("logs", {}).get("nodes", [])
            recent_logs = logs[-10:]  # 10 derniers logs
            
            # Vérifier les patterns de succès/erreur
            format_errors = 0
            success_indicators = 0
            pages_processed = 0
            
            for log in recent_logs:
                msg = log["message"]
                
                if "Format invalide" in msg:
                    format_errors += 1
                    print(f"⚠️  ERREUR FORMAT: {msg}")
                    
                elif "Page" in msg and "records" in msg and "→" in msg:
                    pages_processed += 1
                    print(f"📥 {msg}")
                    
                elif "Direct iterator" in msg or "standard pagination" in msg:
                    success_indicators += 1
                    print(f"✅ NOUVEAU CODE: {msg}")
                    
                elif "ERROR" in log["level"]:
                    print(f"❌ ERREUR: {msg}")
            
            # Statut de fin
            if status in ["SUCCESS", "FAILURE"]:
                print(f"\n🏁 Job terminé avec statut: {status}")
                
                if format_errors > 0:
                    print(f"❌ PROBLÈME: {format_errors} erreurs de format détectées")
                    print("   → La correction n'est pas encore active")
                elif success_indicators > 0 and pages_processed > 0:
                    print(f"✅ SUCCÈS: Nouveau code actif, {pages_processed} pages traitées")
                    print("   → La correction fonctionne correctement")
                elif pages_processed > 0:
                    print(f"✅ SUCCÈS PARTIEL: {pages_processed} pages traitées")
                    print("   → Aucune erreur de format, mais logs limités")
                else:
                    print("⚠️  INCERTAIN: Pas assez d'informations dans les logs")
                    
                break
                
            # Attendre avant prochain check
            time.sleep(5)
            
        except Exception as e:
            print(f"❌ Erreur monitoring: {e}")
            break
    
    else:
        print(f"⏰ Timeout après {max_wait_minutes} minutes")

def main():
    """Fonction principale de test"""
    print("🧪 Test de correction Hub'Eau - Problème format dict/list")
    print("=" * 60)
    
    # Test 1: Format API
    test_api_response_format()
    
    # Test 2: Lancement job
    run_id = launch_dagster_job()
    
    if run_id:
        # Test 3: Monitoring
        monitor_job_progress(run_id)
    else:
        print("\n💡 Alternative: Lancer manuellement le job depuis Dagster UI:")
        print("   1. Ouvrir http://localhost:8080")
        print("   2. Aller dans Assets")
        print("   3. Chercher 'ecoulement_stations_csv'")
        print("   4. Cliquer sur 'Materialize'")
        print("   5. Surveiller les logs pour les messages d'erreur/succès")
    
    print("\n📋 Résumé des corrections appliquées:")
    print("✅ Validation robuste des réponses CSV (détection JSON parasite)")
    print("✅ Gestion des erreurs de parsing (pages corrompues ignorées)")
    print("✅ Validation du format de données (list/dict)")
    print("✅ Type mappings Hub'Eau appliqués lors de la création de table")
    print("✅ Fallback gracieux pour continuer l'ingestion malgré les erreurs")

if __name__ == "__main__":
    main()
