# 🔐 Configuration des Secrets GitLab CI/CD

Guide pour configurer les secrets dans GitLab afin que le déploiement soit **100% automatique**.

## 🎯 Objectif

**ZÉRO action manuelle sur le serveur** - Tous les secrets sont gérés dans GitLab, le pipeline génère automatiquement le fichier `.env.production` à chaque déploiement.

## 📍 Où configurer les secrets

1. **Aller sur votre projet GitLab** :
   ```
   https://scm.univ-tours.fr/ringuet/hubeau_data_integration
   ```

2. **Menu latéral** : `Settings` > `CI/CD`

3. **Section "Variables"** : Cliquer sur `Expand`

4. **Ajouter les variables** (bouton `Add variable`)

## 🔑 Variables à configurer

### 1. DAGSTER_PG_PASSWORD

- **Key**: `DAGSTER_PG_PASSWORD`
- **Value**: Votre mot de passe PostgreSQL pour Dagster (ex: `SuperSecure2024!`)
- **Type**: `Variable`
- **Flags**:
  - ✅ `Protect variable` (accessible seulement sur branche `main`)
  - ✅ `Mask variable` (masqué dans les logs)
  - ❌ `Expand variable reference` (décoché)

### 2. MINIO_USER

- **Key**: `MINIO_USER`
- **Value**: `admin` (ou votre nom d'utilisateur MinIO)
- **Type**: `Variable`
- **Flags**:
  - ✅ `Protect variable`
  - ✅ `Mask variable`
  - ❌ `Expand variable reference`

### 3. MINIO_PASS

- **Key**: `MINIO_PASS`
- **Value**: Votre mot de passe MinIO (ex: `MinIOSecure2024!`)
- **Type**: `Variable`
- **Flags**:
  - ✅ `Protect variable`
  - ✅ `Mask variable`
  - ❌ `Expand variable reference`

## 📋 Checklist Visuelle

Dans GitLab > Settings > CI/CD > Variables, vous devez voir :

```
┌─────────────────────────────────────────────────────────────┐
│ Key                    │ Value              │ Flags          │
├─────────────────────────────────────────────────────────────┤
│ DAGSTER_PG_PASSWORD    │ ****************   │ Protected, Masked │
│ MINIO_USER             │ ********           │ Protected, Masked │
│ MINIO_PASS             │ ****************   │ Protected, Masked │
└─────────────────────────────────────────────────────────────┘
```

## 🔒 Bonnes Pratiques Sécurité

### Mots de passe forts

```bash
# Générer un mot de passe fort (sur votre PC)
openssl rand -base64 32

# Exemples de mots de passe forts :
# DAGSTER_PG_PASSWORD: "K8mP#xQz2!vR9wN@pL4tY6bH"
# MINIO_PASS: "Zy3@hN8pW!qR5mT#vK9xL2bC"
```

### Protection des secrets

- ✅ Toujours cocher `Protect variable` (seulement accessible sur `main`)
- ✅ Toujours cocher `Mask variable` (caché dans les logs)
- ✅ Ne **JAMAIS** mettre de secrets dans le code
- ✅ Ne **JAMAIS** commiter `.env.production`
- ✅ Changer les mots de passe régulièrement (tous les 3-6 mois)

## 🚀 Test du Workflow

### 1. Configurer les variables dans GitLab

Suivre les étapes ci-dessus pour ajouter les 3 variables.

### 2. Pusher un commit

```bash
# Sur votre PC
git add .
git commit -m "test: workflow automatique"
git push origin main
```

### 3. Vérifier le pipeline

Le pipeline va automatiquement :

1. ✅ Clone le code
2. ✅ Build l'image Docker
3. ✅ **Génère `.env.production` depuis les secrets GitLab**
4. ✅ Déploie les services
5. ✅ Vérifie que tout fonctionne

### 4. Vérifier les logs du pipeline

Dans GitLab > CI/CD > Pipelines > Votre pipeline > Job `deploy:production`

Vous devriez voir :

```
🔐 Génération du fichier .env.production depuis les secrets GitLab...
✅ Fichier .env.production créé
🛑 Arrêt des anciens conteneurs...
🚀 Démarrage avec la nouvelle image...
```

**Les valeurs des secrets seront masquées** : `***` dans les logs.

## ✅ Vérification Post-Déploiement

### Sur le serveur (optionnel, pour vérifier)

```bash
ssh root@srv991054.hstgr.cloud

# Vérifier que .env.production a été créé automatiquement
cat /srv/brgm/.env.production

# Devrait contenir :
# DAGSTER_PG_PASSWORD=VotreMotDePasse
# MINIO_USER=admin
# MINIO_PASS=VotreMotDePasse
```

### Vérifier que Dagster fonctionne

```bash
# Vérifier les variables d'environnement chargées
docker exec brgm-dagster-webserver env | grep DAGSTER_PG_PASSWORD

# Devrait afficher votre mot de passe (pas vide !)

# Vérifier les logs
docker logs brgm-dagster-webserver --tail 30

# Devrait voir : "Serving dagster-webserver on..."
```

### Accéder à l'UI

- **Dagster** : http://srv991054.hstgr.cloud:8080
- **MinIO** : http://srv991054.hstgr.cloud:9001

## 🔄 Modifier un secret

### Pour changer un mot de passe :

1. **GitLab** : Settings > CI/CD > Variables
2. **Trouver la variable** (ex: `MINIO_PASS`)
3. **Cliquer sur "Edit"**
4. **Changer la valeur**
5. **Save**
6. **Déclencher un nouveau déploiement** :
   ```bash
   git commit --allow-empty -m "chore: update secrets"
   git push origin main
   ```

Le pipeline va automatiquement régénérer `.env.production` avec le nouveau mot de passe.

## 🆘 Dépannage

### Les variables ne sont pas chargées

**Symptôme** : Les conteneurs crashent, logs montrent "no password supplied"

**Solution** :
1. Vérifier que les variables existent dans GitLab (Settings > CI/CD > Variables)
2. Vérifier que `Protect variable` est coché
3. Vérifier que vous déployez depuis la branche `main` (pas `develop`)

### Les secrets apparaissent dans les logs

**Problème** : Vous voyez les mots de passe en clair dans les logs du pipeline

**Solution** :
1. Cocher `Mask variable` pour chaque variable
2. Relancer le pipeline

### Pipeline échoue avec "variable not set"

**Symptôme** :
```
ERROR: DAGSTER_PG_PASSWORD: variable not set
```

**Solution** :
1. Aller dans GitLab > Settings > CI/CD > Variables
2. Vérifier que la variable `DAGSTER_PG_PASSWORD` existe
3. Vérifier qu'elle n'est pas vide
4. Relancer le pipeline

## 📚 Résumé

### ✅ Avantages de cette approche

- ✅ **Zéro intervention manuelle** - Tout est automatique
- ✅ **Sécurisé** - Secrets jamais dans le code
- ✅ **Versionné** - Historique des déploiements dans GitLab
- ✅ **Auditable** - Traçabilité complète
- ✅ **Reproductible** - Même déploiement à chaque fois
- ✅ **Scalable** - Facile d'ajouter des environments (dev/staging/prod)

### 🔄 Workflow Final

```
PC Local
   ↓ git push
GitLab
   ↓ Pipeline CI/CD
   ├─ Build image Docker
   ├─ Génère .env.production (depuis secrets GitLab)
   └─ Déploie sur VPS
      ↓
Serveur Production (srv991054)
   ├─ /srv/brgm/.env.production (généré automatiquement)
   ├─ Docker Compose up
   └─ Services démarrés
         ├─ Dagster UI :8080
         └─ MinIO :9001
```

**Vous ne touchez JAMAIS au serveur directement !** 🎉

---

**Important** : Sauvegardez vos mots de passe dans un gestionnaire de mots de passe sécurisé (LastPass, 1Password, Bitwarden, etc.)

