# Design Spec — Script de déploiement Fabric AI Analytics

**Projet** : Pipeline Fabric — Données d'utilisation Copilot & Cloud Discovery
**Organisation** : ADENES — DSI Groupe
**Date** : 2026-03-25
**Statut** : Approuvé

---

## Contexte

Ce document décrit la conception du script Python d'orchestration automatisé (section 9 de la note technique `NOTE_TECHNIQUE_PIPELINE_FABRIC_AI_ANALYTICS.md`) qui provisionne l'ensemble de l'infrastructure Microsoft Fabric nécessaire au pipeline d'analytics IA d'ADENES.

Le script déploie en une seule exécution :
- L'App Registration Entra ID avec les permissions Graph requises (étape manuelle si droits insuffisants — voir section Prérequis)
- Le Workspace Microsoft Fabric
- Le Lakehouse `lkh_ai_analytics`
- Les deux Notebooks Python d'ingestion (Cloud Discovery + Copilot Usage)
- Le Data Pipeline quotidien avec planification Job Scheduler
- Une exécution de test on-demand avec monitoring

**Hors périmètre** : le Dataflow Gen2 Viva Insights (l'API Fabric REST ne supporte pas encore la création de Dataflow Gen2 programmatiquement). La configuration de cette source reste manuelle, documentée dans le README.

---

## Structure du projet

```
fabric-ai-analytics-deploy/
├── deploy/
│   ├── __main__.py             # Point d'entrée CLI, orchestre les étapes 1→7
│   ├── config.py               # Chargement config (.env → env vars → erreur explicite)
│   ├── lro.py                  # Helper polling Long Running Operations (deux phases)
│   ├── retry.py                # Retry avec backoff exponentiel (throttling 429)
│   ├── logger.py               # Configuration logging (console + fichier /logs/)
│   ├── entra.py                # Étape 1 : App Registration + Service Principal + appRoleAssignments
│   ├── fabric_workspace.py     # Étape 2 : Workspace Fabric + roleAssignment SPN
│   ├── fabric_lakehouse.py     # Étape 3 : Lakehouse (avec LRO deux phases)
│   ├── fabric_notebooks.py     # Étape 4 : Notebooks (Cloud Discovery + Copilot Usage)
│   ├── fabric_pipelines.py     # Étapes 5+6+7 : Pipeline + planification + test run
│   ├── output.py               # Génération deploy-output.json
│   └── notebook_content/
│       ├── nb_cloud_discovery.py    # Code notebook ingestion Cloud Discovery
│       └── nb_copilot_usage.py      # Code notebook ingestion Copilot Usage (/v1.0/)
├── logs/                       # Logs de chaque run (gitignored)
├── .env.example                # Template de configuration
├── .gitignore
├── requirements.txt            # msal>=1.28.0, requests>=2.32.0, python-dotenv>=1.0.0
└── README.md
```

---

## Prérequis manuels (avant exécution du script)

Ces actions nécessitent des droits Global Administrator ou Privileged Role Administrator et doivent être réalisées une fois avant le premier run :

1. **Paramètre admin Fabric** : activer « Service principals can use Fabric APIs » ET « Service principals can create workspaces, connections, and deployment pipelines » dans le portail admin Fabric (`app.fabric.microsoft.com/admin-portal`).
2. **Droits Global Admin pour l'étape 1** : l'étape Entra (création App Registration + appRoleAssignments) nécessite un compte ou Service Principal avec `Application.ReadWrite.All` + `AppRoleAssignment.ReadWrite.All`. Si le compte d'exécution ne dispose pas de ces droits, l'étape 1 peut être exécutée séparément par un administrateur et le script repris avec `--from-step 2`.
3. **Token admin pour l'étape 1** : fourni via la variable `ADMIN_TOKEN` (token pré-acquis via device code flow ou Azure CLI : `az account get-access-token --resource https://graph.microsoft.com`). Requis uniquement pour `--from-step 1` ou un run complet.

---

## Configuration

### Variables requises

| Variable | Description | Requis pour |
|---|---|---|
| `TENANT_ID` | Directory (tenant) ID Entra | Toutes étapes |
| `CLIENT_ID` | Application (client) ID du Service Principal de déploiement | Étapes 2–7 |
| `CLIENT_SECRET` | Secret du Service Principal de déploiement | Étapes 2–7 |
| `CAPACITY_ID` | ID de la capacité Fabric (F64 ou trial) | Étape 2 |
| `KEY_VAULT_URL` | URL du Key Vault (ex : `https://kv-adenes.vault.azure.net/`) | Notebooks |
| `KV_SECRET_NAME_CLIENT_ID` | Nom du secret Key Vault contenant le `CLIENT_ID` du SPN pipeline | Notebooks |
| `KV_SECRET_NAME_CLIENT_SECRET` | Nom du secret Key Vault contenant le `CLIENT_SECRET` du SPN pipeline | Notebooks |
| `ADMIN_TOKEN` | Token Graph d'un compte Global Admin (pré-acquis) | Étape 1 uniquement |

### Stratégie de chargement (`config.py`)

1. Tente de charger `.env` via `python-dotenv` (usage local)
2. Sinon utilise les variables d'environnement du shell / CI (Azure DevOps, GitHub Actions)
3. Si une variable obligatoire est absente : exception explicite avec le nom de la variable manquante

`.env` est listé dans `.gitignore` et ne doit jamais être commité.

---

## Interface CLI

```bash
# Déploiement complet (étapes 1 → 7)
python -m deploy

# Reprendre depuis une étape spécifique après un échec (lit deploy-output.json pour les IDs précédents)
python -m deploy --from-step 3

# Lister les étapes disponibles
python -m deploy --list-steps
```

### Étapes disponibles

| # | Étape | Module |
|---|---|---|
| 1 | App Registration Entra + Service Principal + appRoleAssignments (consentement admin) | `entra.py` |
| 2 | Workspace Fabric + assignation rôle SPN (Contributor) | `fabric_workspace.py` |
| 3 | Lakehouse `lkh_ai_analytics` | `fabric_lakehouse.py` |
| 4 | Notebooks d'ingestion (2 notebooks) | `fabric_notebooks.py` |
| 5 | Data Pipeline `pip_daily_ai_ingestion` (Notebook Activities) | `fabric_pipelines.py` |
| 6 | Planification Job Scheduler (quotidien 6h00, fuseau `Romance Standard Time`, jobType=`DefaultJob`) | `fabric_pipelines.py` |
| 7 | Exécution on-demand + polling job instance + vérification tables Delta via Lakehouse API | `fabric_pipelines.py` |

### Mécanisme `--from-step N`

Quand `--from-step N` est passé :
1. Le script charge `deploy-output.json` (s'il existe) pour récupérer les IDs des étapes 1 à N-1.
2. Si `deploy-output.json` est absent ou ne contient pas les IDs requis, le script lève une erreur explicite listant les IDs manquants.
3. À partir de l'étape N, le comportement normal (idempotence + création) s'applique.

---

## Idempotence

Mode **standard** : si une ressource existe, elle est réutilisée et le script continue. Chaque skip est logué `[SKIP]`.

La **clé d'unicité** est le `displayName` pour toutes les ressources.

| Ressource | Endpoint de vérification | Clé |
|---|---|---|
| App Registration | `GET /v1.0/applications?$filter=displayName eq '{name}'` | displayName |
| Workspace Fabric | `GET /v1/workspaces` filtré client-side sur `displayName` | displayName |
| Lakehouse | `GET /v1/workspaces/{id}/lakehouses` filtré sur `displayName` | displayName |
| Notebook | `GET /v1/workspaces/{id}/notebooks` filtré sur `displayName` | displayName |
| Data Pipeline | `GET /v1/workspaces/{id}/dataPipelines` filtré sur `displayName` | displayName |
| Schedule | `GET /v1/workspaces/{id}/items/{pipelineId}/jobs/DefaultJob/schedules` — skip si schedule enabled existant | enabled=true |
| Job instance (step 7) | N/A — toujours déclenché, pas de vérification préalable | — |

---

## Gestion LRO (`lro.py`)

Les opérations Fabric qui retournent `202 Accepted` suivent un protocole **deux phases** :

```
POST → 202 Accepted + header Location (→ /v1/operations/{operationId})
  └→ Phase 1 : polling GET /v1/operations/{operationId} toutes {Retry-After}s
       ├→ status "Running"    → attendre
       ├→ status "Failed"     → lève FabricLROError(message)
       ├→ status "Succeeded"  → passer en Phase 2
       └→ timeout 5 min       → lève TimeoutError

  └→ Phase 2 : GET /v1/operations/{operationId}/result
       └→ retourne l'objet ressource avec son `id`
```

L'ID de la ressource créée n'est disponible qu'en phase 2 (`/result`). Ressources concernées : Lakehouse, Notebooks avec définition.

---

## Authentification & gestion des tokens (`entra.py` / `config.py`)

### Tokens requis

| Token | Scope | Usage | Acquis via |
|---|---|---|---|
| Graph admin token | `https://graph.microsoft.com/.default` | Étape 1 (App Reg) | Variable `ADMIN_TOKEN` (pré-acquis) |
| Graph pipeline token | `https://graph.microsoft.com/.default` | Appels Graph dans notebooks | MSAL client_credentials |
| Fabric token | `https://api.fabric.microsoft.com/.default` | Étapes 2–7 | MSAL client_credentials |

### Renouvellement des tokens

Les tokens MSAL (`client_credentials`) expirent après 3600s. Pour les runs longs (LRO polling inclus), le token Fabric est renouvelé avant chaque appel API en vérifiant l'expiry : si `expires_in < 300s`, un nouveau token est acquis via `acquire_token_for_client()`. MSAL gère le cache automatiquement.

### Permissions Fabric pour le Service Principal

Les permissions Fabric ne sont **pas** déclarées comme des scopes OAuth dans l'App Registration. L'autorisation Fabric est contrôlée par le **rôle workspace** assigné au Service Principal. L'étape 2 inclut un sous-appel :
```
POST /v1/workspaces/{workspaceId}/roleAssignments
Body: { "principal": { "id": "{spnObjectId}", "type": "ServicePrincipal" }, "role": "Contributor" }
```

### Étape 1 — consentement admin (appRoleAssignments)

L'accord de consentement admin pour les permissions **Application** (type Role) se fait via :
```
POST /v1.0/servicePrincipals/{spnId}/appRoleAssignments
Body: { "principalId": "{spnId}", "resourceId": "{graphSpId}", "appRoleId": "{permissionId}" }
```
(et non via `oauth2PermissionGrants` qui est pour les permissions Delegated).

Les `appRoleId` à assigner (GUIDs Microsoft Graph bien connus, confirmables à l'exécution via `GET /v1.0/servicePrincipals?$filter=displayName eq 'Microsoft Graph'` en inspectant `appRoles`) :
- `e4c9e354-4dc5-45b8-9e7c-e1393b0b1a20` — `CloudApp-Discovery.Read.All`
- `230c1aed-a721-4c5d-9cb4-a90514e508ef` — `Reports.Read.All`

L'implémentation doit résoudre ces GUIDs dynamiquement depuis le Service Principal Microsoft Graph plutôt que les coder en dur.

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
2026-03-25 10:30:06 | INFO  | [STEP 3] LRO phase 1 — polling /operations/xxx toutes 30s...
2026-03-25 10:31:10 | INFO  | [STEP 3] LRO phase 2 — récupération résultat /operations/xxx/result
2026-03-25 10:31:11 | INFO  | [STEP 3] Lakehouse créé — lakehouseId: lkh456
```

Le dossier `logs/` est listé dans `.gitignore`.

---

## Contenu des notebooks embarqués

### Structure commune aux deux notebooks

```
1. Authentification MSAL
   └→ secrets via mssparkutils.credentials.getSecret(KEY_VAULT_URL, KV_SECRET_NAME_CLIENT_ID)
          et mssparkutils.credentials.getSecret(KEY_VAULT_URL, KV_SECRET_NAME_CLIENT_SECRET)
   └→ token renouvelé si expiry < 300s avant chaque bloc d'appels

2. Appels API Graph avec retry/backoff exponentiel
   └→ pagination automatique sur @odata.nextLink

3. Conversion JSON → DataFrame Spark
   └→ schéma explicite (évite les inférences incorrectes)

4. Écriture Delta dans Lakehouse
   └→ mode "append" + déduplication sur clé naturelle (appId+reportDate / userPrincipalName+reportDate)

5. Logging structuré
   └→ timestamp, nb lignes ingérées, durée d'exécution
```

### `nb_cloud_discovery.py`

- Scope token : `https://graph.microsoft.com/.default`
- Récupère le `streamId` MDE via `GET /beta/security/dataDiscovery/cloudAppDiscovery/uploadedStreams`
- Appelle `aggregatedAppsDetails(period=duration'P90D')` avec filtre `category eq 'Generative AI' or category eq 'AI Model Provider'`
- Pagination sur `/users` pour chaque app
- Destinations Delta : `bronze.cloud_discovery_apps`, `bronze.cloud_discovery_users`
- Format durée : `duration'P90D'` (ISO 8601 — P7D, P30D, P90D uniquement)

### `nb_copilot_usage.py`

- Endpoints **`/v1.0/copilot/reports/`** (pas le chemin `/beta/reports/` déprécié)
- `getMicrosoft365CopilotUsageUserDetail(period='D30')`
- `getMicrosoft365CopilotUserCountTrend(period='D30')` avec explosion de `adoptionByDate`
- Format période : `'D7'`, `'D30'`, `'D90'`, `'D180'` (différent du format `duration'P...'` de Cloud Discovery)
- Destinations Delta : `bronze.copilot_usage_detail`, `bronze.copilot_usage_trend`

---

## Détail de l'étape 7 — Test run on-demand et vérification

### Déclenchement on-demand

```
POST /v1/workspaces/{workspaceId}/items/{pipelineId}/jobs/instances?jobType=Pipeline
→ 202 Accepted + header Location (→ /v1/workspaces/{id}/items/{pipelineId}/jobs/instances/{jobInstanceId})
```

### Polling du statut du job

```
GET /v1/workspaces/{workspaceId}/items/{pipelineId}/jobs/instances/{jobInstanceId}
  └→ polling toutes 30s
       ├→ status "InProgress"  → attendre
       ├→ status "Completed"   → succès
       ├→ status "Failed"      → lève PipelineJobError avec failureReason
       ├→ status "Cancelled"   → lève PipelineJobError
       └→ timeout 30 min       → lève TimeoutError
```

### Vérification des tables Delta

Après complétion du job, vérifier que les 4 tables Bronze ont été créées via l'API Lakehouse :

```
GET /v1/workspaces/{workspaceId}/lakehouses/{lakehouseId}/tables
→ vérifie la présence de : bronze.cloud_discovery_apps, bronze.cloud_discovery_users,
  bronze.copilot_usage_detail, bronze.copilot_usage_trend
→ logue le nombre de lignes si disponible dans la réponse
→ lève une TableVerificationError si une table est absente
```

---

## Fichier de sortie `deploy-output.json`

Ce fichier est l'état persisté du déploiement. Il est relu par `--from-step N` pour récupérer les IDs des étapes précédentes.

```json
{
  "deployedAt": "2026-03-25T10:30:00Z",
  "steps": {
    "1_entra": {
      "status": "created",
      "appId": "<guid>",
      "objectId": "<guid>",
      "spnObjectId": "<guid>",
      "portalUrl": "https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationMenuBlade/~/Overview/appId/<appId>"
    },
    "2_workspace": {
      "status": "existing_reused",
      "workspaceId": "<guid>",
      "portalUrl": "https://app.fabric.microsoft.com/groups/<workspaceId>"
    },
    "3_lakehouse": {
      "status": "created",
      "lakehouseId": "<guid>",
      "portalUrl": "https://app.fabric.microsoft.com/groups/<workspaceId>/lakehouses/<lakehouseId>"
    },
    "4_notebooks": {
      "status": "created",
      "ids": {
        "nb_cloud_discovery": "<guid>",
        "nb_copilot_usage": "<guid>"
      }
    },
    "5_pipeline": {
      "status": "created",
      "pipelineId": "<guid>",
      "portalUrl": "https://app.fabric.microsoft.com/groups/<workspaceId>/datapipelines/<pipelineId>"
    },
    "6_schedule": {
      "status": "created",
      "scheduleId": "<guid>",
      "nextRun": "2026-04-01T06:00:00+02:00"
    },
    "7_test_run": {
      "status": "completed",
      "jobInstanceId": "<guid>",
      "durationSeconds": 47,
      "tablesVerified": [
        "bronze.cloud_discovery_apps",
        "bronze.cloud_discovery_users",
        "bronze.copilot_usage_detail",
        "bronze.copilot_usage_trend"
      ]
    }
  }
}
```

Valeurs possibles pour `status` : `"created"`, `"existing_reused"`, `"failed"`.

---

## Contraintes techniques

| Point | Détail |
|---|---|
| API Graph Cloud Discovery | Beta — schémas susceptibles d'évoluer. Durées : `P7D`, `P30D`, `P90D` uniquement (format `duration'...'`). |
| Copilot API | Utiliser `/v1.0/copilot/reports/` — chemin `/beta/reports/` déprécié mars 2026. Format période : `'D7'`…`'D180'`. |
| Throttling | Géré par `retry.py` (script + notebooks). |
| Secrets | Jamais en clair. `.env` dans `.gitignore`. Key Vault via `mssparkutils.credentials.getSecret()` dans les notebooks. |
| LRO Fabric | Lakehouse et Notebooks retournent 202 → polling deux phases (`/operations/{id}` puis `/operations/{id}/result`). |
| Permissions Fabric SPN | Via rôle workspace (`roleAssignments`), pas via scopes OAuth. Deux paramètres admin tenant à activer. |
| Consentement Graph | Via `appRoleAssignments` (permissions Application/Role) — pas via `oauth2PermissionGrants`. |
| Schedule jobType | `DefaultJob` pour la création de schedule (`POST .../jobs/DefaultJob/schedules`). `Pipeline` est réservé aux runs on-demand. |
| Token expiry | Renouvellement automatique si `expires_in < 300s` avant chaque appel. |
| Viva Insights Dataflow Gen2 | **Hors périmètre** — API Fabric REST non disponible pour ce type d'item. Configuration manuelle documentée dans README. |

---

## Conventions Git

- Repo initialisé à la racine du projet avec `.gitignore` (logs/, .env, __pycache__/, *.pyc, deploy-output.json)
- Commits après chaque phase de développement :
  - Phase 0 : init repo + structure + `config.py` + `logger.py` + `lro.py` + `retry.py`
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
