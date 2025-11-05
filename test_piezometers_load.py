#!/usr/bin/env python3
"""
Script de test pour vérifier le chargement des données de piézomètres.
"""

import sys
from pathlib import Path
from sqlalchemy import create_engine, text
import pandas as pd
import os

# Ajouter le chemin src au sys.path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from hubeau_pipeline.utils.database import get_postgres_connection_string


def test_csv_file():
    """Teste l'existence et la lisibilité du fichier CSV."""
    print("\n🔍 Test du fichier CSV...")
    csv_file = Path("piezometers_merged_clean.csv")

    if not csv_file.exists():
        print(f"❌ Fichier introuvable : {csv_file}")
        return False

    try:
        df = pd.read_csv(csv_file, nrows=5)
        print(f"✅ Fichier CSV lisible : {len(df)} lignes d'exemple lues")
        print(f"   Colonnes : {', '.join(df.columns)}")
        return True
    except Exception as e:
        print(f"❌ Erreur lors de la lecture du CSV : {e}")
        return False


def test_database_connection():
    """Teste la connexion à PostgreSQL."""
    print("\n🔍 Test de la connexion PostgreSQL...")

    try:
        # Tester avec les variables d'environnement directement
        from dotenv import load_dotenv
        load_dotenv()

        connection_string = get_postgres_connection_string()
        engine = create_engine(connection_string)

        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()")).fetchone()
            print(f"✅ Connexion réussie à PostgreSQL")
            print(f"   Version : {result[0][:50]}...")

            # Vérifier les schémas disponibles
            schemas = conn.execute(text("SELECT schema_name FROM information_schema.schemata")).fetchall()
            print(f"   Schémas disponibles : {', '.join([s[0] for s in schemas[:5]])}")

        engine.dispose()
        return True

    except Exception as e:
        print(f"❌ Erreur de connexion : {e}")
        return False


def test_table_creation():
    """Teste la création de la table piezometers."""
    print("\n🔍 Test de création de table...")

    try:
        from hubeau_pipeline.assets.piezometers_csv import create_piezometers_table_schema

        connection_string = get_postgres_connection_string()
        engine = create_engine(connection_string)

        with engine.begin() as conn:
            # Supprimer la table si elle existe
            conn.execute(text("DROP TABLE IF EXISTS piezometers CASCADE"))
            print("   Table supprimée si elle existait")

            # Créer la table
            conn.execute(text(create_piezometers_table_schema()))
            print("✅ Table créée avec succès")

            # Vérifier la structure
            columns = conn.execute(text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'piezometers'
                ORDER BY ordinal_position
                LIMIT 10
            """)).fetchall()

            print(f"   Premières colonnes créées :")
            for col_name, col_type in columns:
                print(f"     - {col_name}: {col_type}")

        engine.dispose()
        return True

    except Exception as e:
        print(f"❌ Erreur lors de la création de table : {e}")
        return False


def test_sample_load():
    """Teste le chargement d'un échantillon de données."""
    print("\n🔍 Test de chargement d'échantillon...")

    try:
        csv_file = Path("piezometers_merged_clean.csv")
        df = pd.read_csv(csv_file, nrows=100, parse_dates=['time', 'date_de_début', 'date_de_fin'])

        # Renommer les colonnes
        column_mapping = {
            'date_de_début': 'date_de_debut',
            'date_de_fin': 'date_de_fin',
            'durée': 'duree',
            'cyclicité_ig': 'cyclicite_ig',
            'cyclicité_piézomètre': 'cyclicite_piezometre',
            'modèle_métier_existant': 'modele_metier_existant',
            'logiciel_du_modèle_métier': 'logiciel_du_modele_metier',
            'météeau_nappes': 'meteeau_nappes',
            'données_de_pompage_disponibles': 'donnees_de_pompage_disponibles',
            'caractéristique_principale': 'caracteristique_principale'
        }
        df = df.rename(columns=column_mapping)
        df = df.where(pd.notnull(df), None)

        connection_string = get_postgres_connection_string()
        engine = create_engine(connection_string)

        # Charger l'échantillon
        df.to_sql('piezometers', engine, if_exists='append', index=False, method='multi')

        # Vérifier le chargement
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM piezometers")).scalar()
            print(f"✅ Échantillon chargé : {count} lignes dans la table")

            # Afficher quelques statistiques
            stats = conn.execute(text("""
                SELECT
                    COUNT(DISTINCT pz) as nb_piezometers,
                    MIN(time) as date_min,
                    MAX(time) as date_max
                FROM piezometers
            """)).fetchone()

            print(f"   Piézomètres uniques : {stats[0]}")
            print(f"   Période : {stats[1]} à {stats[2]}")

        engine.dispose()
        return True

    except Exception as e:
        print(f"❌ Erreur lors du chargement : {e}")
        return False


def main():
    """Exécute tous les tests."""
    print("=" * 60)
    print("TEST DU PIPELINE DE CHARGEMENT DES PIÉZOMÈTRES")
    print("=" * 60)

    tests = [
        ("Fichier CSV", test_csv_file),
        ("Connexion DB", test_database_connection),
        ("Création table", test_table_creation),
        ("Chargement échantillon", test_sample_load),
    ]

    results = []
    for test_name, test_func in tests:
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            print(f"❌ Erreur inattendue dans {test_name}: {e}")
            results.append((test_name, False))

    print("\n" + "=" * 60)
    print("RÉSUMÉ DES TESTS")
    print("=" * 60)

    for test_name, success in results:
        status = "✅ SUCCÈS" if success else "❌ ÉCHEC"
        print(f"{test_name:.<30} {status}")

    total_success = sum(1 for _, s in results if s)
    print(f"\nTotal : {total_success}/{len(results)} tests réussis")

    if total_success == len(results):
        print("\n🎉 Tous les tests sont passés !")
        print("Vous pouvez maintenant lancer le job 'load_piezometers_csv' dans Dagster UI")
        print("URL : http://localhost:8080")
    else:
        print("\n⚠️ Certains tests ont échoué. Veuillez corriger les erreurs avant de lancer le job.")

    return total_success == len(results)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)