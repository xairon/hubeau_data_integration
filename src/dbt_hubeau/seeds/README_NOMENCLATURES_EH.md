# Nomenclatures entités hydrogéologiques (Sandre / BDLISA)

## Où sont les nomenclatures sur BDLISA ?

Le site [BDLISA](https://bdlisa.eaufrance.fr/) ne publie **pas** les listes de codes/libellés comme pages ou fichiers séparés. Les nomenclatures utilisées par la BDLISA sont celles du **Sandre** (dictionnaire PRL, nomenclatures SAQ 2002-1). Les services WFS/WMS BDLISA renvoient des **codes** (ex. `code_milieu`, `code_theme`) ; les **libellés** officiels sont ceux du Sandre, intégrés dans les seeds `ref_*_eh.csv` ci-dessous.

## Source officielle (Sandre)

Les libellés **Nature**, **État** et **Thème** proviennent du **dictionnaire Sandre PRL 1.0** (Prélèvements des ressources en eau), nomenclatures **SAQ 2002-1** :

- **ref_nature_eh** : Nature de l'entité hydrogéologique (NatureEntiteHydroGeol) — codes 1 à 7 + 12 (GSM). Code 0 = Inconnue (données BDLISA).
- **ref_etat_eh** : État de l'entité hydrogéologique (EtatEntiteHydroGeol) — libellés officiels pour les codes 1 à 5. Codes 0 et 6 ajoutés pour couvrir les valeurs présentes dans le jeu (0 = non renseigné, 6 = hors nomenclature).
- **ref_theme_eh** : Thème de l'entité hydrogéologique (ThemeEntiteHydroGeol) — 1 Alluvial, 2 sédimentaire, 3 Socle, 4 Intensément plissés de montagne, 5 Volcanisme. Code 0 = Inconnu.

Référence API : `https://api.sandre.eaufrance.fr/definitions/v1/dictionnaire/PRL/1.0`

## Niveau (ref_niveau_eh)

Convention BDLISA / Sandre : 1 = niveau national, 2 = niveau régional, 3 = niveau local. Libellés cohérents avec la documentation BDLISA (pas de nomenclature PRL dédiée trouvée).

## À vérifier en cas de mise à jour

- **ref_milieu_eh** (type de milieu / porosité) : non présent dans le dictionnaire PRL 1.0. Libellés déduits de la doc BDLISA (poreux, fissuré, karstique, etc.). À confirmer avec le référentiel Sandre ou la doc BDLISA si une nomenclature officielle existe.
- **ref_origine_eh** (potentialités aquifères) : non présent dans le dictionnaire PRL 1.0. Libellés interprétés (forte / moyenne / faible / nulle). À confirmer avec Sandre ou BDLISA.

En cas de découverte d’une nomenclature officielle pour Milieu ou Origine, mettre à jour les CSV puis relancer `dbt seed`.
