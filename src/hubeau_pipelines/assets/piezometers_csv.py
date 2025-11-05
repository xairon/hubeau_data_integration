"""
Asset pour charger les données de piézomètres depuis un fichier CSV dans PostgreSQL.
"""

import pandas as pd
import os
from pathlib import Path
from dagster import asset, Output, AssetExecutionContext, MetadataValue
from sqlalchemy import create_engine, text
from hubeau_pipelines.utils.database import get_postgres_connection_string
from typing import Dict, Any


def create_piezometers_table_schema() -> str:
    """
    Retourne le schéma SQL pour créer la table des piézomètres.

    Analyse du fichier CSV :
    - 120826 lignes de données
    - 22 colonnes avec différents types de données
    - Données de surveillance des nappes phréatiques
    """
    return """
    CREATE TABLE IF NOT EXISTS piezometers (
        -- Identifiants et mesures principales
        pz VARCHAR(50) NOT NULL,                       -- Identifiant du piézomètre
        time DATE NOT NULL,                            -- Date de la mesure
        niveau_nappe_eau DOUBLE PRECISION,             -- Niveau de la nappe d'eau
        tp_m DOUBLE PRECISION,                         -- Température prévue (m)
        pev_m DOUBLE PRECISION,                        -- Évapotranspiration potentielle (m)

        -- Informations géographiques et administratives
        bassin VARCHAR(100),                           -- Nom du bassin
        ig DOUBLE PRECISION,                           -- ID de la nappe
        ig_nom TEXT,                                    -- Nom de la nappe
        cyclicite_ig VARCHAR(100),                     -- Cyclicité de la nappe
        xlamb_93 DOUBLE PRECISION,                     -- Coordonnée X Lambert 93
        ylamb_93 DOUBLE PRECISION,                     -- Coordonnée Y Lambert 93

        -- Données temporelles
        date_de_debut DATE,                            -- Date de début des mesures
        date_de_fin DATE,                              -- Date de fin des mesures
        duree DOUBLE PRECISION,                        -- Durée en années

        -- Caractéristiques du piézomètre
        cyclicite_piezometre VARCHAR(100),             -- Cyclicité du piézomètre
        influence_anthropique VARCHAR(100),            -- Influence anthropique probable
        modele_metier_existant VARCHAR(50),            -- Existence d'un modèle métier
        logiciel_du_modele_metier VARCHAR(100),        -- Logiciel utilisé pour le modèle
        meteeau_nappes VARCHAR(50),                    -- Lien avec Météau nappes
        donnees_de_pompage_disponibles VARCHAR(50),    -- Disponibilité des données de pompage
        commentaire TEXT,                              -- Commentaires
        caracteristique_principale VARCHAR(200),       -- Caractéristique principale

        -- Métadonnées
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        -- Contraintes
        PRIMARY KEY (pz, time)                         -- Clé composite sur piézomètre et temps
    );

    -- Index pour améliorer les performances
    CREATE INDEX IF NOT EXISTS idx_piezometers_pz ON piezometers(pz);
    CREATE INDEX IF NOT EXISTS idx_piezometers_time ON piezometers(time);
    CREATE INDEX IF NOT EXISTS idx_piezometers_bassin ON piezometers(bassin);
    CREATE INDEX IF NOT EXISTS idx_piezometers_ig ON piezometers(ig);
    CREATE INDEX IF NOT EXISTS idx_piezometers_coords ON piezometers(xlamb_93, ylamb_93);
    """


@asset(
    description="Charge les données de piézomètres depuis un fichier CSV dans PostgreSQL",
    compute_kind="python",
    metadata={
        "source": "CSV file",
        "destination": "PostgreSQL",
        "table": "piezometers"
    }
)
def piezometers_csv_data(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Charge les données de piézomètres depuis le fichier CSV dans PostgreSQL.

    Le fichier CSV contient des données de surveillance des nappes phréatiques
    avec des mesures temporelles, géographiques et hydrologiques.

    Returns:
        Dict contenant les statistiques de chargement
    """

    # Configuration
    csv_file = Path("/home/ringuet/Documents/brgm/piezometers_merged_clean.csv")
    table_name = "piezometers"
    schema_name = "public"  # Ou utiliser HUBEAU_SCHEMA si préféré

    # Vérifier que le fichier existe
    if not csv_file.exists():
        raise FileNotFoundError(f"Le fichier CSV n'existe pas : {csv_file}")

    context.log.info(f"Chargement du fichier CSV : {csv_file}")

    # Connexion à la base de données
    connection_string = get_postgres_connection_string()
    engine = create_engine(connection_string)

    try:
        # Créer le schéma et la table si nécessaire
        with engine.begin() as conn:
            context.log.info("Création de la table si elle n'existe pas...")
            conn.execute(text(create_piezometers_table_schema()))

            # Optionnel : Vider la table avant le chargement (à commenter si on veut un chargement incrémental)
            context.log.info(f"Suppression des données existantes dans {schema_name}.{table_name}...")
            conn.execute(text(f"TRUNCATE TABLE {schema_name}.{table_name} CASCADE;"))

        # Charger le CSV
        context.log.info("Lecture du fichier CSV...")
        df = pd.read_csv(csv_file, parse_dates=['time', 'date_de_début', 'date_de_fin'])

        # Renommer les colonnes pour correspondre au schéma SQL
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

        # Remplacer les valeurs vides par None pour PostgreSQL
        df = df.where(pd.notnull(df), None)

        # Statistiques avant chargement
        total_rows = len(df)
        unique_piezometers = df['pz'].nunique()
        date_range = f"{df['time'].min()} à {df['time'].max()}"

        context.log.info(f"Nombre total de lignes : {total_rows}")
        context.log.info(f"Nombre de piézomètres uniques : {unique_piezometers}")
        context.log.info(f"Plage de dates : {date_range}")

        # Charger les données par batch pour éviter les problèmes de mémoire
        batch_size = 5000
        total_loaded = 0

        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i+batch_size]
            batch_df.to_sql(
                name=table_name,
                con=engine,
                schema=schema_name,
                if_exists='append',
                index=False,
                method='multi'
            )
            total_loaded += len(batch_df)
            context.log.info(f"Chargé {total_loaded}/{total_rows} lignes...")

        # Vérifier le chargement
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")).scalar()
            context.log.info(f"Nombre de lignes dans la table après chargement : {result}")

            # Statistiques supplémentaires
            stats_query = f"""
            SELECT
                COUNT(DISTINCT pz) as nb_piezometers,
                MIN(time) as date_min,
                MAX(time) as date_max,
                COUNT(*) as nb_total_mesures,
                COUNT(DISTINCT bassin) as nb_bassins,
                AVG(niveau_nappe_eau) as niveau_moyen,
                MIN(niveau_nappe_eau) as niveau_min,
                MAX(niveau_nappe_eau) as niveau_max
            FROM {schema_name}.{table_name}
            """
            stats = conn.execute(text(stats_query)).fetchone()

        # Préparer les métadonnées de sortie
        output_metadata = {
            "nb_lignes_chargees": total_loaded,
            "nb_piezometers": stats[0],
            "date_min": str(stats[1]),
            "date_max": str(stats[2]),
            "nb_bassins": stats[4],
            "niveau_nappe_moyen": float(stats[5]) if stats[5] else None,
            "niveau_nappe_min": float(stats[6]) if stats[6] else None,
            "niveau_nappe_max": float(stats[7]) if stats[7] else None,
        }

        context.log.info("Chargement terminé avec succès!")

        return Output(
            value=output_metadata,
            metadata={
                "lignes_chargees": MetadataValue.int(total_loaded),
                "piezometers_uniques": MetadataValue.int(stats[0]),
                "plage_dates": MetadataValue.text(f"{stats[1]} à {stats[2]}"),
                "bassins": MetadataValue.int(stats[4]),
                "niveau_moyen": MetadataValue.float(float(stats[5])) if stats[5] else None,
            }
        )

    except Exception as e:
        context.log.error(f"Erreur lors du chargement des données : {str(e)}")
        raise
    finally:
        engine.dispose()