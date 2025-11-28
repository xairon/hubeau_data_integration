# SSH Port Forwarding Guide

## Convention des Ports

**Règle simple** : Ports locaux = Ports serveur avec préfixe `1`

| Service | Port Serveur | Port Local | URL Locale |
|---------|--------------|------------|------------|
| **Dagster UI** | 8080 | **18080** | http://localhost:18080 |
| **Adminer** | 8081 | **18081** | http://localhost:18081 |
| **Portainer** | 9000 | **19000** | http://localhost:19000 |
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
    -L 19000:localhost:9000 \
    -L 15432:localhost:5432 \
    ringuet@dib-2019006065
```

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

**Credentials** :
- **System** : PostgreSQL
- **Server** : `brgm-postgres` ou `postgres`
- **Username** : `postgres`
- **Password** : `REDACTED`
- **Database** : `postgres`

### 🐳 Portainer (Docker Management)
**URL** : http://localhost:19000

Au premier accès, créer un compte admin.

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
    -L 19000:localhost:9000 \
    -L 15432:localhost:5432 \
    -o ServerAliveInterval=60 \
    -o ServerAliveCountMax=3 \
    ringuet@dib-2019006065
```

## Background Mode (Optionnel)

Pour laisser le tunnel en arrière-plan :

```bash
# Linux/macOS
ssh -f -N \
    -L 18080:localhost:8080 \
    -L 18081:localhost:8081 \
    -L 19000:localhost:9000 \
    -L 15432:localhost:5432 \
    ringuet@dib-2019006065

# Pour tuer le tunnel
pkill -f "ssh.*18080"
```

**Attention** : En mode background, vous devez manuellement fermer le tunnel.
