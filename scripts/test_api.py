#!/usr/bin/env python
"""Test Hub'Eau API connectivity"""
import requests

# Test piezometry chroniques API 
url = 'https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/chroniques'
params = {
    'date_debut_mesure': '2004-01-01',
    'date_fin_mesure': '2004-12-31',
    'code_bss': '07548X0009/F',
    'size': 20
}

print(f"Testing: {url}")
print(f"Params: {params}")

try:
    r = requests.get(url, params=params, timeout=60)
    print(f"Status: {r.status_code}")
    
    if r.status_code == 200:
        data = r.json()
        count = data.get('count', 0)
        print(f"Count: {count}")
        if data.get('data'):
            print(f"First record keys: {list(data['data'][0].keys())}")
    else:
        print(f"Error: {r.text[:500]}")
except Exception as e:
    print(f"Exception: {e}")
