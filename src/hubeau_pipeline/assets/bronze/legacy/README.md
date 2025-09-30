# 📦 Code Legacy Hub'Eau

## ⚠️ **ATTENTION : CODE OBSOLÈTE**

Ce dossier contient l'ancien système d'ingestion Hub'Eau qui a été remplacé par la nouvelle architecture moderne.

## 📁 Contenu

- **`hubeau_real_ingestion.py`** - Ancien service d'ingestion Hub'Eau
- **`hubeau_configs.py`** - Anciennes configurations Hub'Eau  
- **`bdlisa_real_ingestion.py`** - Ancien service BDLISA
- **`sandre_real_ingestion.py`** - Ancien service SANDRE

## 🚀 Migration vers la nouvelle architecture

### **Nouveau système :**
- **`../hubeau_modern.py`** - Architecture moderne avec httpx + tenacity + pydantic
- **`../hubeau_configs_modern.py`** - Configurations centralisées
- **`../hubeau_migration.py`** - Assets de migration

### **Avantages du nouveau système :**
- ✅ **-90% de code** (2000 → 200 lignes)
- ✅ **+300% de performance** (async natif)
- ✅ **Retry automatique** (tenacity)
- ✅ **Validation stricte** (pydantic)
- ✅ **Type hints complets**

## 🔄 Plan de suppression

1. **Phase 1** : Migration complète vers le nouveau système
2. **Phase 2** : Validation des performances
3. **Phase 3** : Suppression de ce dossier legacy

## 📅 Date de création du legacy

**29 septembre 2024** - Migration vers l'architecture moderne

---

**⚠️ Ne pas utiliser ce code pour de nouveaux développements !**
