# Design Spec — Script de déploiement Fabric AI Analytics

**Projet** : Pipeline Fabric — Données d'utilisation Copilot & Cloud Discovery
**Organisation** : ADENES — DSI Groupe
**Date** : 2026-03-25
**Statut** : Approuvé

---

## Contexte

Ce document décrit la conception du script Python d'orchestration automatisé (section 9 de la note technique `NOTE_TECHNIQUE_PIPELINE_FABRIC_AI_ANALYTICS.md`) qui provisionne l'ensemble de l'infrastructure Microsoft Fabric nécessaire au pipeline d'analytics IA d'ADENES.

Le script déploie en une seule exécution :
- L'App Registration Entra ID avec les permissions Graph requises
- Le Workspace Microsoft Fabric
- Le Lakehouse `lkh_ai_analytics`
- Les Notebooks Python d'ingestion (Cloud Discovery + Copilot Usage)
- Le Data Pipeline quotidien avec planification Job Scheduler
- Une exécution de test on-demand avec monitoring

---

## Structure du projet

```
fabric-ai-analytics-deploy/
├── deploy/
│   ├── __main__.py             # Point d'entrée CLI, orchestre les étapes 1→7
│   ├── config.py               # Chargement config (.env → env vars → erreur explicite)
│   ├── lro.py                  # Helper polling Long Running Operations
│   ├── retry.py                # Retry avec backoff exponentiel (throttling 429)
│   ├── logger.py               # Configuration logging (console + fichier /logs/)
│   ├── entra.py                # Étape 1 : App Registration + Service Principal + Secret
│   ├── fabric_workspace.py     # Étape 2 : Workspace Fabric
│   ├── fabric_lakehouse.py     # Étape 3 : Lakehouse (avec LRO)
│   ├── fabric_notebooks.py     # Étape 4 : Notebooks (Cloud Discovery + Copilot Usage)
│   ├── fabric_pipelines.py     # Étapes 5+6+7 : Pipeline + planification + test run
│   ├── output.py               # Génération deploy-output.json
│   └── notebook_content/
│       ├── nb_cloud_discovery.py    # Code notebook ingestion Cloud Discovery
│       └── nb_copilot_usage.py      # Code notebook ingestion Copilot Usage
├── logs/                       # Logs de chaque run (gitignored)
├── .env.example                # Template de configuration
├── .gitignore
├── requirements.txt            # msal, requests, python-dotenv
└── README.md
```

---

## Configuration

### Variables requises

| Variable | Description |
|---|---|
| `TENANT_ID` | Directory (tenant) ID Entra |
| `CLIENT_ID` | Application (client) ID du compte admin de déploiement |
| `CLIENT_SECRET` | Secret du compte admin de déploiement |
| `CAPACITY_ID` | ID de la capacité Fabric (F64 ou trial) |
| `KEY_VAULT_URL` | URL du Key Vault (ex : `https://kv-adenes.vault.azure.net/`) |
| `KV_SECRET_NAME` | Nom du secret dans Key Vault pour le pipeline |

### Stratégie de chargement (`config.py`)

1. Tente de charger `.env` via `python-dotenv` (usage local)
2. Sinon utilise les variables d'environnement du shell / CI (Azure DevOps, GitHub Actions)
3. Si une variable obligatoire est absente : exception explicite avec le nom de la variable manquante

---

## Interface CLI

```bash
# Déploiement complet (étapes 1 → 7)
python -m deploy

# Reprendre depuis une étape spécifique après un échec
python -m deploy --from-step 3

# Lister les étapes disponibles
python -m deploy --list-steps
```

### Étapes disponibles

| # | Étape | Module |
|---|---|---|
| 1 | App Registration Entra (+ Service Principal + Secret) | `entra.py` |
| 2 | Workspace Fabric | `fabric_workspace.py` |
| 3 | Lakehouse `lkh_ai_analytics` | `fabric_lakehouse.py` |
| 4 | Notebooks d'ingestion | `fabric_notebooks.py` |
| 5 | Data Pipeline `pip_daily_ai_ingestion` | `fabric_pipelines.py` |
| 6 | Planification Job Scheduler (quotidien 6h00) | `fabric_pipelines.py` |
| 7 | Exécution on-demand + monitoring + vérification tables | `fabric_pipelines.py` |

---

## Idempotence

Mode **standard** : si une ressource existe, elle est réutilisée et le script continue. Chaque skip est logué `[SKIP]`.

| Ressource | Vérification d'existence |
|---|---|
| App Registration | `GET /v1.0/applications?$filter=displayName eq '{name}'` |
| Workspace Fabric | `GET /v1/workspaces` filtré sur `displayName` |
| Lakehouse | `GET /v1/workspaces/{id}/lakehouses` filtré sur `displayName` |
| Notebook | `GET /v1/workspaces/{id}/notebooks` filtré sur `displayName` |
| Data Pipeline | `GET /v1/workspaces/{id}/items` filtré sur `type=DataPipeline` et `displayName` |
| Schedule | `GET .../jobs/Pipeline/schedules` — skip si schedule actif existant |

---

## Gestion LRO (`lro.py`)

Les opérations Fabric qui retournent `202 Accepted` sont gérées par polling :

```
POST → 202 Accepted + header Location
  └→ GET Location toutes {Retry-After} secondes (défaut : 30s)
       ├→ status "Succeeded"  → retourne l'objet résultat
       ├→ status "Failed"     → lève FabricLROError avec le message d'erreur
       └→ timeout 5 min       → lève TimeoutError
```

Ressources concernées : Lakehouse, Notebooks avec définition.

---

## Retry / Backoff exponentiel (`retry.py`)

Utilisé dans le script de déploiement ET dans les notebooks embarqués.

- Déclenchement : `429 Too Many Requests` ou `5xx`
- Délai initial : valeur du header `Retry-After` si présent, sinon 30s
- Facteur multiplicateur : x2 à chaque tentative
- Maximum : 5 tentatives
- Au-delà : lève l'exception d'origine

---

## Logging (`logger.py`)

Chaque exécution crée deux sorties simultanées :

- **Console** : logs colorés niveau INFO (WARNING/ERROR en rouge)
- **Fichier** : `logs/deploy_YYYYMMDD_HHMMSS.log` — niveau DEBUG, format structuré

```
2026-03-25 10:30:01 | INFO  | [STEP 1] Création App Registration...
2026-03-25 10:30:02 | INFO  | [STEP 1] App Registration créée — appId: abc123
2026-03-25 10:30:05 | INFO  | [STEP 2] Workspace existant détecté — [SKIP] workspaceId: xyz789
2026-03-25 10:30:05 | INFO  | [STEP 3] Création Lakehouse...
2026-03-25 10:30:06 | INFO  | [STEP 3] LRO en cours — polling toutes 30s...
2026-03-25 10:31:10 | INFO  | [STEP 3] Lakehouse créé — lakehouseId: lkh456
```

Le dossier `logs/` est listé dans `.gitignore`.

---

## Contenu des notebooks embarqués

### Structure commune aux deux notebooks

```
1. Authentification MSAL
   └→ secrets via mssparkutils.credentials.getSecret(KEY_VAULT_URL, KV_SECRET_NAME)

2. Appels API Graph avec retry/backoff exponentiel
   └→ pagination automatique sur @odata.nextLink

3. Conversion JSON → DataFrame Spark
   └→ schéma explicite (évite les inférences incorrectes)

4. Écriture Delta dans Lakehouse
   └→ mode "append" + déduplication sur clé naturelle

5. Logging structuré
   └→ timestamp, nb lignes ingérées, durée d'exécution
```

### `nb_cloud_discovery.py`

- Récupère le `streamId` MDE via `uploadedStreams`
- Appelle `aggregatedAppsDetails(period=duration'P90D')` avec filtre `category eq 'Generative AI' or category eq 'AI Model Provider'`
- Pagination sur `/users` pour chaque app
- Destinations Delta : `bronze.cloud_discovery_apps`, `bronze.cloud_discovery_users`

### `nb_copilot_usage.py`

- Endpoints `/v1.0/copilot/reports/` (pas le chemin `/beta/reports/` déprécié)
- `getMicrosoft365CopilotUsageUserDetail(period='D30')`
- `getMicrosoft365CopilotUserCountTrend(period='D30')` avec explosion de `adoptionByDate`
- Destinations Delta : `bronze.copilot_usage_detail`, `bronze.copilot_usage_trend`

---

## Fichier de sortie `deploy-output.json`

```json
{
  "deployedAt": "2026-03-25T10:30:00Z",
  "steps": {
    "1_entra": {
      "status": "created",
      "appId": "...",
      "objectId": "...",
      "portalUrl": "https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationMenuBlade/~/Overview/appId/..."
    },
    "2_workspace": {
      "status": "existing_reused",
      "workspaceId": "...",
      "portalUrl": "https://app.fabric.microsoft.com/groups/..."
    },
    "3_lakehouse": {
      "status": "created",
      "lakehouseId": "...",
      "portalUrl": "https://app.fabric.microsoft.com/groups/{workspaceId}/lakehouses/..."
    },
    "4_notebooks": {
      "status": "created",
      "ids": {
        "nb_cloud_discovery": "...",
        "nb_copilot_usage": "..."
      }
    },
    "5_pipeline": {
      "status": "created",
      "pipelineId": "...",
      "portalUrl": "https://app.fabric.microsoft.com/..."
    },
    "6_schedule": {
      "status": "created",
      "nextRun": "2026-04-01T06:00:00+02:00"
    },
    "7_test_run": {
      "status": "completed",
      "jobId": "...",
      "durationSeconds": 47,
      "tablesCreated": ["bronze.cloud_discovery_apps", "bronze.cloud_discovery_users", "bronze.copilot_usage_detail", "bronze.copilot_usage_trend"]
    }
  }
}
```

---

## Contraintes techniques rappelées (depuis la note technique)

| Point | Détail |
|---|---|
| API Graph Cloud Discovery | Beta — schémas susceptibles d'évoluer. Durées supportées : P7D, P30D, P90D uniquement. |
| Copilot API | Utiliser `/v1.0/copilot/reports/` — le chemin `/beta/reports/` est déprécié. |
| Throttling | Géré par `retry.py` dans le script et dans les notebooks. |
| Secrets | Jamais en clair dans le code. `.env` dans `.gitignore`. Key Vault via `mssparkutils` dans les notebooks. |
| LRO Fabric | Lakehouse et Notebooks avec définition retournent 202 → polling obligatoire. |
| Service Principal Fabric | L'admin tenant doit avoir activé « Service principals can create workspaces » dans le portail admin Fabric. |

---

## Conventions Git

- Repo initialisé à la racine du projet avec `.gitignore` (logs/, .env, __pycache__, *.pyc)
- Commits réguliers après chaque phase de développement :
  - Phase 0 : init repo + structure + config + logger + helpers (lro, retry)
  - Phase 1 : `entra.py`
  - Phase 2 : `fabric_workspace.py` + `fabric_lakehouse.py`
  - Phase 3 : contenu notebooks (`notebook_content/`)
  - Phase 4 : `fabric_notebooks.py`
  - Phase 5 : `fabric_pipelines.py`
  - Phase 6 : `output.py` + `__main__.py` (orchestration complète)
  - Phase 7 : `README.md` + `.env.example`

---

## Dépendances Python

```
msal>=1.28.0
requests>=2.32.0
python-dotenv>=1.0.0
```
