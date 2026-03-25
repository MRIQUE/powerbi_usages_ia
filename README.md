# Pipeline Fabric — Données d'utilisation IA

**Organisation** : ADENES — DSI Groupe
**Auteur** : Aymeric Lacroix
**Date** : Mars 2026

Pipeline d'ingestion automatisé des données d'utilisation de l'IA générative vers Microsoft Fabric, consolidant trois sources Microsoft dans un Lakehouse analytique unifié.

---

## Contexte

Ce projet alimente le tableau de bord CODIR IA d'ADENES en agrégeant :

| Source | API | Données |
|--------|-----|---------|
| Microsoft Defender for Cloud Apps | Microsoft Graph Beta (`/security/dataDiscovery`) | Applications IA découvertes, utilisateurs, volumes réseau |
| Microsoft Viva Insights | Copilot Dashboard | Métriques d'adoption Copilot |
| Microsoft Graph Reports API | `/v1.0/reports/` | Usage Copilot par utilisateur |

---

## Structure du dépôt

```
powerbi_usages_ia/
├── Specifications/
│   └── NOTE_TECHNIQUE_PIPELINE_FABRIC_AI_ANALYTICS.md   # Note technique complète
├── docs/superpowers/
│   ├── specs/2026-03-25-fabric-ai-analytics-deploy-design.md
│   └── plans/2026-03-25-fabric-ai-analytics-deployer.md
└── fabric-ai-analytics-deploy/    # Script de déploiement automatisé
    ├── deploy/                    # Modules Python
    ├── tests/                     # Tests unitaires
    ├── .env.example               # Template de configuration
    ├── requirements.txt
    └── README.md
```

---

## Déploiement automatisé (`fabric-ai-analytics-deploy`)

Script Python qui provisionne en une seule commande l'ensemble de l'infrastructure Fabric :

| Étape | Ressource |
|-------|-----------|
| 1 | App Registration Entra ID + Service Principal + permissions Graph |
| 2 | Workspace Fabric `WS_AI_Analytics_ADENES` |
| 3 | Lakehouse `lkh_ai_analytics` |
| 4 | Notebooks d'ingestion (Cloud Discovery + Copilot Usage) |
| 5 | Data Pipeline `pip_daily_ai_ingestion` |
| 6 | Planification quotidienne à 6h00 (Job Scheduler) |
| 7 | Exécution on-demand + vérification tables Delta |

### Prérequis

1. Python 3.10+
2. Compte avec droits **Global Administrator** ou **Privileged Role Administrator** pour l'étape 1 (Entra)
3. Paramètre admin Fabric activé : *Service principals can use Fabric APIs* + *Service principals can create workspaces*
4. Une capacité Fabric F-SKU active

### Installation

```bash
cd fabric-ai-analytics-deploy
pip install -r requirements.txt
cp .env.example .env
# Renseigner les variables dans .env
```

### Configuration (`.env`)

```env
TENANT_ID=your-tenant-id
CLIENT_ID=your-client-id
CLIENT_SECRET=your-client-secret
CAPACITY_ID=your-fabric-capacity-id
KEY_VAULT_URL=https://kv-adenes.vault.azure.net/
KV_SECRET_NAME_CLIENT_ID=fabric-pipeline-client-id
KV_SECRET_NAME_CLIENT_SECRET=fabric-pipeline-client-secret
# Requis uniquement pour l'étape 1 — obtenir via :
# az account get-access-token --resource https://graph.microsoft.com --query accessToken -o tsv
ADMIN_TOKEN=
```

### Utilisation

```bash
# Déploiement complet (étapes 1 à 7)
python -m deploy

# Reprendre depuis une étape spécifique (si une étape précédente a déjà été exécutée)
python -m deploy --from-step 2

# Lister les étapes disponibles
python -m deploy --list-steps
```

L'état de chaque étape est persisté dans `deploy-output.json` pour permettre la reprise en cas d'interruption.

### Tests

```bash
pip install -r requirements-dev.txt
pytest tests/
```

---

## Hors périmètre (configuration manuelle)

Le **Dataflow Gen2 Viva Insights** ne peut pas être créé programmatiquement via l'API REST Fabric. Sa configuration reste manuelle — voir la note technique pour les étapes détaillées.

---

## Documentation

- [Note technique complète](Specifications/NOTE_TECHNIQUE_PIPELINE_FABRIC_AI_ANALYTICS.md)
- [Design spec du déployeur](docs/superpowers/specs/2026-03-25-fabric-ai-analytics-deploy-design.md)
