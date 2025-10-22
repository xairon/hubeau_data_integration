"""
Gestion des contraintes de clés étrangères pour Hub'Eau
Certains endpoints retournent des références qui n'existent pas dans les tables parentes
"""

import logging
import psycopg2
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class ForeignKeyHandler:
    """
    Gère les violations de clés étrangères en:
    1. Vérifiant les références manquantes
    2. Créant des enregistrements stub si nécessaire
    3. Filtrant les données invalides
    """

    @staticmethod
    def check_and_fix_foreign_keys(
        data: List[Dict[str, Any]],
        table_name: str,
        conn_params: dict
    ) -> List[Dict[str, Any]]:
        """
        Vérifie et corrige les violations de FK avant insertion

        Pour hydrometry_stations -> hydrometry_sites:
        - Vérifie que tous les code_site existent
        - Crée des sites stub si manquants
        - Ou filtre les stations invalides
        """

        if table_name == "hydrometry_stations":
            return ForeignKeyHandler._handle_hydrometry_stations(data, conn_params)

        # Pas de traitement spécial pour les autres tables
        return data

    @staticmethod
    def _handle_hydrometry_stations(
        data: List[Dict[str, Any]],
        conn_params: dict
    ) -> List[Dict[str, Any]]:
        """
        Gère les références manquantes pour hydrometry_stations -> hydrometry_sites
        """
        if not data:
            return data

        # Extraire tous les code_site uniques
        code_sites = set()
        for record in data:
            if record.get("code_site"):
                code_sites.add(record["code_site"])

        if not code_sites:
            return data

        conn = psycopg2.connect(**conn_params)
        try:
            with conn.cursor() as cursor:
                # Vérifier quels code_site existent déjà
                cursor.execute("""
                    SELECT code_site
                    FROM hubeau.hydrometry_sites
                    WHERE code_site = ANY(%s)
                """, (list(code_sites),))

                existing_sites = {row[0] for row in cursor.fetchall()}
                missing_sites = code_sites - existing_sites

                if missing_sites:
                    logger.warning(f"⚠️ {len(missing_sites)} code_site manquants dans hydrometry_sites")
                    logger.warning(f"   Sites manquants: {list(missing_sites)[:10]}...")

                    # Option 1: Créer des sites stub (enregistrements minimaux)
                    for code_site in missing_sites:
                        try:
                            cursor.execute("""
                                INSERT INTO hubeau.hydrometry_sites (code_site, libelle_site)
                                VALUES (%s, %s)
                                ON CONFLICT (code_site) DO NOTHING
                            """, (code_site, f"Site stub - {code_site}"))
                            logger.info(f"✅ Créé site stub pour {code_site}")
                        except Exception as e:
                            logger.error(f"❌ Erreur création site stub {code_site}: {e}")

                    conn.commit()

                    # Option 2 (alternative): Filtrer les stations avec sites manquants
                    # filtered_data = [r for r in data if r.get("code_site") in existing_sites]
                    # logger.warning(f"⚠️ Filtré {len(data) - len(filtered_data)} stations avec sites manquants")
                    # return filtered_data

        except Exception as e:
            logger.error(f"❌ Erreur vérification FK: {e}")
            conn.rollback()
        finally:
            conn.close()

        return data