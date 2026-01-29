# Tables de rejet (exception)

**Bonnes pratiques** : les lignes filtrées en silver (valeurs nulles, clés manquantes, etc.) ne sont pas supprimées sans trace. Elles sont redirigées vers des **tables de rejet** dans le schéma `silver_rejects`, avec une colonne `rejection_reason` pour l’audit et la qualité.

- **Audit / conformité** : savoir quelles lignes ont été exclues et pourquoi.
- **Debugging** : analyser les rejets (volume, motifs) et remonter aux sources.
- **Reprocessing** : corriger des règles puis réinjecter les rejets si besoin.

Chaque modèle `*_rejected` correspond à un staging qui filtre des lignes : il sélectionne **exactement les lignes exclues** par ce staging, avec un code de rejet.

## Modèles

| Modèle | Source bronze | Lignes rejetées (exemples) |
|--------|----------------|----------------------------|
| `stg_piezo_chroniques_rejected` | `piezometry_chroniques_raw` | `date_mesure` ou `code_bss` ou `niveau_nappe_eau` ou `profondeur_nappe` nul/invalide |
| `stg_hydrometry_stations_rejected` | `hydrometry_stations_raw` | `code_site` absent de `stg_hydrometry_sites`, ou `code_station`/coords nulles |
| `stg_hydrometry_obs_elab_rejected` | `hydrometry_obs_elab_raw` | `date_obs_elab` ou `code_site` ou `grandeur_hydro_elab` ou `resultat_obs_elab` nul/invalide, ou `code_site` absent des sites |

## Colonnes ajoutées

- **rejection_reason** : code du premier motif de rejet (ex. `NIVEAU_NAPPE_NULL`, `RESULTAT_OBS_NULL`).
- **rejected_at** : date/heure du run dbt (optionnel, si besoin d’horodatage précis).

## Requêtes utiles

```sql
-- Volume de rejets par motif (piézo)
SELECT rejection_reason, COUNT(*) 
FROM silver_rejects.stg_piezo_chroniques_rejected 
GROUP BY rejection_reason;

-- Derniers rejets hydrométrie
SELECT * FROM silver_rejects.stg_hydrometry_obs_elab_rejected 
ORDER BY date_obs_elab DESC NULLS LAST LIMIT 100;
```
