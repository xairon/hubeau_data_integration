# SSH Port Forwarding Guide

## 📖 Instructions pour l'équipe

### Prérequis
1. ✅ Accès VPN configuré
2. ✅ Compte SSH sur le serveur `dib-2019006065`
3. ✅ Connexion SSH fonctionnelle

### Étapes rapides

#### 1. Connexion VPN
Connectez-vous au VPN de l'entreprise (si pas déjà fait).

#### 2. Créer le tunnel SSH

**Option simple (Adminer seulement)** :
```bash
ssh -L 18081:localhost:8081 VOTRE_USERNAME@dib-2019006065
```

**Option complète (tous les services)** :
```bash
ssh -L 18080:localhost:8080 \
    -L 18081:localhost:8081 \
    -L 15432:localhost:5432 \
    VOTRE_USERNAME@dib-2019006065
```

⚠️ Remplacez `VOTRE_USERNAME` par votre identifiant SSH !

#### 3. Accéder à Adminer

Une fois le tunnel SSH ouvert, allez sur : http://localhost:18081

**Credentials** :
- Serveur : `postgres`
- Utilisateur : `readonly`
- Mot de passe : `readonly_2024_secure`
- Base : `postgres`

---

## Convention des Ports

**Règle simple** : Ports locaux = Ports serveur avec préfixe `1`

| Service | Port Serveur | Port Local | URL Locale |
|---------|--------------|------------|------------|
| **Dagster UI** | 8080 | **18080** | http://localhost:18080 |
| **Adminer** | 8081 | **18081** | http://localhost:18081 |
| **PostgreSQL** | 5432 | **15432** | localhost:15432 |

## Utilisation

### Linux / macOS

```bash
# Rendre le script exécutable
chmod +x scripts/ssh_forward.sh

# Lancer le forwarding
./scripts/ssh_forward.sh
```

### Windows

```cmd
# Lancer le forwarding
scripts\ssh_forward.bat
```

### Commande manuelle

Si vous préférez lancer manuellement :

```bash
ssh -L 18080:localhost:8080 \
    -L 18081:localhost:8081 \
    -L 15432:localhost:5432 \
    VOTRE_USERNAME@dib-2019006065
```

⚠️ **IMPORTANT** : Remplacez `VOTRE_USERNAME` par votre identifiant SSH sur le serveur (ex: `ringuet`, `ben`, etc.)

## Accès aux Interfaces

Une fois le tunnel SSH établi, ouvrez dans votre navigateur :

### 🎯 Dagster UI (Principal)
**URL** : http://localhost:18080

Interface principale pour :
- Lancer les jobs (ex: `era5_meteo_bronze`)
- Visualiser les assets
- Monitorer les runs

### 🗄️ Adminer (PostgreSQL Web)
**URL** : http://localhost:18081

**Credentials (Read-Only - Recommandé)** :
- **System** : PostgreSQL
- **Server** : `postgres`
- **Username** : `readonly`
- **Password** : `readonly_2024_secure`
- **Database** : `postgres`

**Credentials (Admin - Full Access)** :
- **System** : PostgreSQL
- **Server** : `postgres`
- **Username** : `postgres`
- **Password** : `REDACTED`
- **Database** : `postgres`

💡 **Recommandation** : Utilisez le user `readonly` pour la consultation. Le user `postgres` est réservé pour les opérations de maintenance.

### 🔌 PostgreSQL Direct

Pour DBeaver, pgAdmin, ou tout client PostgreSQL :

**Connexion** :
- **Host** : `localhost`
- **Port** : `15432`
- **User** : `postgres`
- **Password** : `REDACTED`
- **Database** : `postgres`

## Troubleshooting

### Port déjà utilisé

Si un port local est occupé :

```bash
# Identifier le processus (Linux/macOS)
lsof -i :18080

# Identifier le processus (Windows PowerShell)
Get-NetTCPConnection -LocalPort 18080 | Select-Object -Property OwningProcess
```

Puis tuer le processus ou utiliser un autre port.

### Connexion SSH perdue

Le tunnel se ferme si la connexion SSH est interrompue. Relancez simplement le script.

### Timeout

Si la connexion SSH timeout, ajoutez ces options :

```bash
ssh -L 18080:localhost:8080 \
    -L 18081:localhost:8081 \
    -L 15432:localhost:5432 \
    -o ServerAliveInterval=60 \
    -o ServerAliveCountMax=3 \
    VOTRE_USERNAME@dib-2019006065
```

## Background Mode (Optionnel)

Pour laisser le tunnel en arrière-plan :

```bash
# Linux/macOS
ssh -f -N \
    -L 18080:localhost:8080 \
    -L 18081:localhost:8081 \
    -L 15432:localhost:5432 \
    VOTRE_USERNAME@dib-2019006065

# Pour tuer le tunnel
pkill -f "ssh.*18080"
```

**Attention** : En mode background, vous devez manuellement fermer le tunnel.

## Configuration Tabby (RECOMMANDÉ)

Tabby permet de configurer un profil SSH avec port forwarding automatique - c'est la méthode la plus simple !

### Étape 1 : Ouvrir les paramètres Tabby

1. Ouvrir Tabby
2. Cliquer sur ⚙️ **Settings** (en bas à gauche)
3. Aller dans **Profiles & connections**

### Étape 2 : Créer un nouveau profil SSH

1. Cliquer sur **New profile** → **SSH connection**
2. Configurer le profil comme suit :

**Onglet "General" :**
- **Name:** `BRGM Hub'Eau (avec port forwarding)`
- **Group:** `BRGM` (ou laissez vide)
- **Icon:** 🌊 (optionnel)

**Onglet "Connection" :**
- **Host:** `dib-2019006065`
- **Port:** `22`
- **Username:** `VOTRE_USERNAME` (ex: ringuet, ben, etc.)
- **Authentication:** Password / Key (selon votre config)

⚠️ **IMPORTANT** : Utilisez **votre propre username SSH**, pas celui de quelqu'un d'autre !

**Onglet "Port forwarding" :**

Cliquer sur **Add** pour chaque redirection :

| Type | Description | Listen Address | Listen Port | Target Address | Target Port |
|------|-------------|----------------|-------------|----------------|-------------|
| Local | Dagster UI | `localhost` | `18080` | `localhost` | `8080` |
| Local | Adminer | `localhost` | `18081` | `localhost` | `8081` |
| Local | PostgreSQL | `localhost` | `15432` | `localhost` | `5432` |

**Onglet "Advanced" (optionnel) :**
- **Keep alive interval:** `60` secondes
- Cocher ✅ **Reconnect automatically**

### Étape 3 : Sauvegarder et utiliser

1. Cliquer sur **Save**
2. Le profil apparaît maintenant dans votre liste de connexions
3. Double-cliquer dessus pour se connecter → **Les ports sont automatiquement redirigés !**

### Avantages de Tabby

✅ **Pas besoin de script** : Tout est configuré dans l'interface
✅ **Reconnexion auto** : Si la connexion se perd, Tabby reconnecte et rétablit les tunnels
✅ **Profil réutilisable** : Un clic pour se connecter avec tous les forwards
✅ **Visual** : Voir l'état des port forwards dans l'interface
✅ **Multi-session** : Peut ouvrir plusieurs onglets sur le même serveur

### Export/Import du profil (optionnel)

Pour partager la config avec l'équipe, vous pouvez exporter le profil :

1. **Settings** → **Profiles & connections**
2. Clic droit sur le profil → **Export**
3. Partager le fichier JSON avec l'équipe

**Exemple de configuration JSON :**
```json
{
  "type": "ssh",
  "name": "BRGM Hub'Eau (avec port forwarding)",
  "group": "BRGM",
  "options": {
    "host": "dib-2019006065",
    "port": 22,
    "user": "ringuet",
    "forwardedPorts": [
      {
        "type": "Local",
        "description": "Dagster UI",
        "host": "localhost",
        "port": 18080,
        "targetAddress": "localhost",
        "targetPort": 8080
      },
      {
        "type": "Local",
        "description": "Adminer",
        "host": "localhost",
        "port": 18081,
        "targetAddress": "localhost",
        "targetPort": 8081
      },
      {
        "type": "Local",
        "description": "PostgreSQL",
        "host": "localhost",
        "port": 15432,
        "targetAddress": "localhost",
        "targetPort": 5432
      }
    ],
    "keepaliveInterval": 60000,
    "keepaliveCountMax": 3
  }
}
```
