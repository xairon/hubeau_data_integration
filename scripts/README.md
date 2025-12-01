# Scripts Hub'Eau Pipeline

Ce dossier contient les scripts utilitaires pour le projet Hub'Eau Data Integration.

## SSH Port Forwarding

### Méthode 1 : Tabby (RECOMMANDÉ) 🌟

**Avantages :**
- ✅ Configuration graphique simple
- ✅ Reconnexion automatique
- ✅ Pas besoin de terminal séparé
- ✅ Visual feedback sur l'état des tunnels

**Installation :**

1. **Importer le profil prêt à l'emploi :**
   - Ouvrir Tabby
   - Settings → Profiles & connections
   - New profile → Import from file
   - Sélectionner `scripts/tabby-profile-hubeau.json`
   - Save

2. **Ou configurer manuellement :**
   - Settings → Profiles & connections
   - New profile → SSH connection
   - Suivre les instructions dans `docs/PORT_FORWARDING.md`

3. **Utilisation :**
   - Double-cliquer sur le profil "BRGM Hub'Eau"
   - Les 4 ports sont automatiquement redirigés !
   - Accès : http://localhost:18080 (Dagster), http://localhost:18081 (Adminer), etc.

### Méthode 2 : Scripts shell

**Windows (CMD/PowerShell) :**
```cmd
scripts\ssh_forward.bat
```

**Linux/macOS/Git Bash :**
```bash
chmod +x scripts/ssh_forward.sh
./scripts/ssh_forward.sh
```

**Commande SSH manuelle :**
```bash
ssh -L 18080:localhost:8080 \
    -L 18081:localhost:8081 \
    -L 19000:localhost:9000 \
    -L 15432:localhost:5432 \
    ringuet@dib-2019006065
```

## Fichiers

| Fichier | Description |
|---------|-------------|
| `tabby-profile-hubeau.json` | Profil Tabby prêt à importer (RECOMMANDÉ) |
| `ssh_forward.sh` | Script Bash pour Linux/macOS/Git Bash |
| `ssh_forward.bat` | Script batch pour Windows |

## Convention de ports

**Règle simple :** Ajouter "1" devant le port du serveur

| Service | Port Serveur | Port Local | URL |
|---------|--------------|------------|-----|
| Dagster UI | 8080 | **18080** | http://localhost:18080 |
| Adminer | 8081 | **18081** | http://localhost:18081 |
| Portainer | 9000 | **19000** | http://localhost:19000 |
| PostgreSQL | 5432 | **15432** | localhost:15432 |

## Documentation complète

Voir `docs/PORT_FORWARDING.md` pour :
- Guide détaillé de configuration Tabby
- Troubleshooting
- Mode background
- Configuration SSH avancée
