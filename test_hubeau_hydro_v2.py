#!/usr/bin/env python3
"""
Script de test pour valider les correctifs Hub'Eau Hydrométrie v2
Teste les URLs avec les bons paramètres
"""

import requests
import json
from datetime import datetime, timedelta

def test_obs_elab_v2():
    """Test de l'endpoint obs_elab avec les bons paramètres v2"""
    print("🧪 Test obs_elab v2 avec paramètres corrigés")
    
    # Paramètres temporels : 30 jours
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    params = {
        "format": "json",
        "pretty": "true",
        # ✅ v2: bons noms de bornes temporelles
        "date_debut_obs_elab": start_date.strftime("%Y-%m-%d"),
        "date_fin_obs_elab": end_date.strftime("%Y-%m-%d"),
        # ✅ au moins une grandeur élaborée (débit moyen journalier)
        "grandeur_hydro_elab": "QmnJ",
        "size": 5,
        "page": 1,
    }
    
    url = "https://hubeau.eaufrance.fr/api/v2/hydrometrie/obs_elab"
    
    print(f"🔍 URL: {url}")
    print(f"📋 Paramètres: {params}")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        print(f"📊 Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            records = data.get("data", [])
            count = data.get("count", 0)
            
            print(f"✅ Succès: {count} records total, {len(records)} dans cette page")
            
            if records:
                print(f"📝 Échantillon record:")
                sample = records[0]
                print(f"   - code_site: {sample.get('code_site')}")
                print(f"   - code_station: {sample.get('code_station')}")
                print(f"   - date_obs_elab: {sample.get('date_obs_elab')}")
                print(f"   - grandeur_hydro_elab: {sample.get('grandeur_hydro_elab')}")
                print(f"   - resultat_obs_elab: {sample.get('resultat_obs_elab')}")
                
                # Vérifier les champs requis
                required_fields = {"date_obs_elab", "resultat_obs_elab"}
                sample_fields = set(sample.keys())
                missing_fields = required_fields - sample_fields
                
                if missing_fields:
                    print(f"⚠️ Champs manquants: {missing_fields}")
                else:
                    print(f"✅ Champs requis présents: {required_fields}")
            else:
                print("⚠️ Aucun record dans cette page")
                
        else:
            print(f"❌ Erreur HTTP: {response.status_code}")
            print(f"📝 Réponse: {response.text[:500]}")
            
    except Exception as e:
        print(f"❌ Erreur: {e}")

def test_observations_tr_v2():
    """Test de l'endpoint observations_tr (qui fonctionne déjà)"""
    print("\n🧪 Test observations_tr v2 (référence)")
    
    # Paramètres temporels : 24h
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    params = {
        "format": "json",
        "pretty": "true",
        # ✅ v1: bons noms de bornes temporelles
        "date_debut_obs": start_date.strftime("%Y-%m-%d"),
        "date_fin_obs": end_date.strftime("%Y-%m-%d"),
        "size": 5,
    }
    
    url = "https://hubeau.eaufrance.fr/api/v2/hydrometrie/observations_tr"
    
    print(f"🔍 URL: {url}")
    print(f"📋 Paramètres: {params}")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        print(f"📊 Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            records = data.get("data", [])
            count = data.get("count", 0)
            
            print(f"✅ Succès: {count} records total, {len(records)} dans cette page")
            
            if records:
                print(f"📝 Échantillon record:")
                sample = records[0]
                print(f"   - code_station: {sample.get('code_station')}")
                print(f"   - date_obs: {sample.get('date_obs')}")
                print(f"   - resultat: {sample.get('resultat')}")
                
        else:
            print(f"❌ Erreur HTTP: {response.status_code}")
            print(f"📝 Réponse: {response.text[:500]}")
            
    except Exception as e:
        print(f"❌ Erreur: {e}")

def test_multiple_grandeurs():
    """Test avec plusieurs grandeurs élaborées"""
    print("\n🧪 Test obs_elab avec plusieurs grandeurs")
    
    grandeurs = ["QmnJ", "QmM", "HIXnJ", "HIXM"]
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)  # 7 jours pour éviter trop de données
    
    for grandeur in grandeurs:
        print(f"\n🔍 Test grandeur: {grandeur}")
        
        params = {
            "format": "json",
            "date_debut_obs_elab": start_date.strftime("%Y-%m-%d"),
            "date_fin_obs_elab": end_date.strftime("%Y-%m-%d"),
            "grandeur_hydro_elab": grandeur,
            "size": 3,
            "page": 1,
        }
        
        url = "https://hubeau.eaufrance.fr/api/v2/hydrometrie/obs_elab"
        
        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                count = data.get("count", 0)
                records = data.get("data", [])
                print(f"✅ {grandeur}: {count} records total, {len(records)} dans cette page")
            else:
                print(f"❌ {grandeur}: HTTP {response.status_code}")
                
        except Exception as e:
            print(f"❌ {grandeur}: {e}")

if __name__ == "__main__":
    print("🎯 === TEST CORRECTIFS HUB'EAU HYDROMÉTRIE V2 ===")
    
    # Test 1: obs_elab avec paramètres corrigés
    test_obs_elab_v2()
    
    # Test 2: observations_tr (référence qui fonctionne)
    test_observations_tr_v2()
    
    # Test 3: Plusieurs grandeurs
    test_multiple_grandeurs()
    
    print("\n🎉 Tests terminés !")
