# Hub'Eau CLI - Guide de démarrage rapide

## Installation

```bash
# Installer les dépendances
pip install -r requirements.txt

# Ajouter src/ au PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"

# Ou sur Windows PowerShell
$env:PYTHONPATH = "$env:PYTHONPATH;$(pwd)\src"
```

## Utilisation du CLI

### Méthode 1: Module Python

```bash
# Lancer le CLI comme module Python
python -m hubeau.cli --help

# Exemples d'utilisation
python -m hubeau.cli info apis
python -m hubeau.cli info endpoints hydrometry
python -m hubeau.cli load hydrometry stations
```

### Méthode 2: Script Direct

```bash
# Depuis le répertoire src/
cd src
python -m hubeau.cli info apis

# Ou directement
python src/hubeau/cli.py info apis
```

### Méthode 3: Alias (Recommandé)

Ajoutez cet alias à votre `.bashrc` ou `.zshrc`:

```bash
alias hubeau='python -m hubeau.cli'
```

Puis utilisez simplement:

```bash
hubeau info apis
hubeau load hydrometry stations
hubeau export temperature chroniques output.parquet
```

## Commandes Principales

```bash
# Informations
hubeau info apis                     # Liste les APIs
hubeau info endpoints hydrometry     # Liste les endpoints d'une API
hubeau info stats                     # Statistiques

# Chargement de données
hubeau load hydrometry stations                  # Vers PostgreSQL
hubeau load temperature chroniques -d filesystem # Vers filesystem
hubeau load piezometry stations --dry-run        # Test sans charger

# Export
hubeau export hydrometry stations output.parquet
hubeau export temperature chroniques data.csv --format csv

# État et gestion
hubeau state                          # État global
hubeau state hydrometry               # État d'une API
hubeau reset hydrometry               # Reset état

# Validation
hubeau validate configs/hubeau/hydrometry_stations.yml
```

## Dépannage

Si vous avez une erreur d'import:

```bash
# Vérifier que PYTHONPATH contient src/
echo $PYTHONPATH

# Ou lancer depuis le répertoire racine avec
PYTHONPATH=src python -m hubeau.cli info apis
```