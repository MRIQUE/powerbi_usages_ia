# Note technique — Pipeline Fabric : Données d'utilisation Copilot & Cloud Discovery

**Auteur** : DSI Groupe — ADENES  
**Date** : Mars 2026  
**Version** : 2.0  
**Classification** : Interne

---

## Sommaire

1. [Contexte et objectif](#1-contexte-et-objectif)
2. [Source 1 — Defender for Cloud Apps (Cloud Discovery)](#2-source-1--defender-for-cloud-apps-cloud-discovery)
3. [Source 2 — Viva Insights (Copilot Dashboard)](#3-source-2--viva-insights-copilot-dashboard)
4. [Source 3 — Microsoft Graph Reports API (Copilot Usage)](#4-source-3--microsoft-graph-reports-api-copilot-usage)
5. [Architecture cible du pipeline Fabric](#5-architecture-cible-du-pipeline-fabric)
6. [Prérequis transverses et enregistrement d'application](#6-prérequis-transverses-et-enregistrement-dapplication)
7. [Étapes de paramétrage détaillées](#7-étapes-de-paramétrage-détaillées)
8. [Modèle de données cible dans le Lakehouse](#8-modèle-de-données-cible-dans-le-lakehouse)
9. [Automatisation du déploiement via APIs Fabric, Graph et Entra](#9-automatisation-du-déploiement-via-apis-fabric-graph-et-entra)
10. [Limitations et points d'attention](#10-limitations-et-points-dattention)

---

## 1. Contexte et objectif

Le document de préparation CODIR IA s'appuie sur deux sources Microsoft pour rendre compte de l'utilisation de l'IA générative chez ADENES :

- **Microsoft Defender for Cloud Apps** (volet Cloud Discovery / Shadow IT) — portail `security.microsoft.com`
- **Microsoft Viva Insights** (Copilot Dashboard) — portail `analysis.insights.cloud.microsoft`

Une troisième source complémentaire, la **Microsoft Graph Reports API**, fournit des métriques d'usage Copilot au niveau utilisateur.

L'objectif de cette note est de recenser les APIs programmatiques permettant d'extraire ces données, puis de décrire les étapes de paramétrage nécessaires pour construire un pipeline d'ingestion automatisé vers un Lakehouse Microsoft Fabric, en vue de consolider l'ensemble dans un modèle analytique unifié. Une section spécifique couvre l'automatisation du déploiement de l'ensemble des composants du projet via les APIs REST Fabric, Graph et Entra ID.

---

## 2. Source 1 — Defender for Cloud Apps (Cloud Discovery)

### 2.1 Données disponibles

Le portail Defender for Cloud Apps expose les données de découverte cloud via le stream Defender-managed endpoints (intégration native MDE). Les données couvrent :

- Applications découvertes (nom, catégorie, score de risque, tags)
- Utilisateurs par application (identifiant, volume de transactions)
- Volumes de trafic réseau (upload/download en bytes)
- Nombre de transactions et d'adresses IP
- Appareils accédant aux applications

### 2.2 API disponible : Microsoft Graph Beta

L'API recommandée est la **Microsoft Graph API (endpoint beta)**, namespace `security/dataDiscovery/cloudAppDiscovery`. Cette API est en beta mais activement maintenue par Microsoft et documentée officiellement.

> **Vérification Context7** : les endpoints ci-dessous sont confirmés dans la documentation Microsoft Graph officielle. Les paramètres de durée supportés sont `P7D`, `P30D` et `P90D` (format ISO 8601 Duration).

#### Endpoints principaux

| Opération | Endpoint | Description |
|---|---|---|
| Lister les streams | `GET /beta/security/dataDiscovery/cloudAppDiscovery/uploadedStreams` | Récupère les flux de données (dont le streamId MDE) |
| Apps découvertes (agrégé) | `GET /beta/security/dataDiscovery/cloudAppDiscovery/uploadedStreams/{streamId}/microsoft.graph.security.aggregatedAppsDetails(period=duration'{duration}')` | Détails agrégés par app |
| Détail d'une app | `GET .../aggregatedAppsDetails(period=duration'{duration}')/{appId}` | Propriétés détaillées : `displayName`, `riskScore`, `uploadNetworkTrafficInBytes`, `downloadNetworkTrafficInBytes`, `transactionCount`, `userCount`, `deviceCount`, `lastSeenDateTime`, `category`, `domains` |
| Info risque d'une app | `GET .../aggregatedAppsDetails(period=duration'{duration}')/{appId}/appInfo` | Attributs de risque sécurité, légaux et conformité |
| Utilisateurs par app | `GET .../aggregatedAppsDetails(period=duration'{duration}')/{appId}/users` | Collection de `discoveredCloudAppUser` (propriété `userIdentifier`) |
| Adresses IP par app | `GET .../aggregatedAppsDetails(period=duration'{duration}')/{appId}/ipAddress` | Adresses IP accédant à l'app |

#### Permission requise

`CloudApp-Discovery.Read.All` (Application permission) avec consentement administrateur.

#### Filtrage OData

L'API supporte `$select`, `$filter`, `$top` et `$skip`. Exemple pour filtrer sur les catégories IA :

```
$filter=category eq 'Generative AI' or category eq 'AI Model Provider'
$select=displayName,userCount,uploadNetworkTrafficInBytes,downloadNetworkTrafficInBytes,transactionCount,riskScore
```

### 2.3 API alternative : REST API legacy

L'ancienne API REST Defender for Cloud Apps (`portal.cloudappsecurity.com/cas/api/v1/`) reste fonctionnelle mais est en voie de dépréciation au profit de l'API Graph. Elle utilise un token API généré dans **Settings > Cloud Apps > API tokens** et un header `Authorization: Token <your_token_key>`. Cette voie n'est **pas recommandée** pour un nouveau pipeline.

---

## 3. Source 2 — Viva Insights (Copilot Dashboard)

### 3.1 Données disponibles

Le Copilot Dashboard de Viva Insights fournit les métriques d'adoption, d'impact et de sentiment pour Microsoft 365 Copilot :

- Utilisateurs actifs (avec et sans licence M365 Copilot)
- Actions exécutées par application M365 (Word, Teams, Excel, PowerPoint, Outlook, etc.)
- Tendances d'utilisation des agents Copilot
- Statistiques Copilot Chat (avec et sans licence)
- Métriques d'intensité d'usage et de rétention

### 3.2 Méthodes d'extraction

> **Point important** : Microsoft a annoncé le retrait de l'export MGDC (Microsoft Graph Data Connect) pour Viva Insights fin 2025. Les méthodes supportées sont désormais les suivantes :

| Méthode | Description | Prérequis licence | Vers Fabric |
|---|---|---|---|
| **Export CSV Copilot Dashboard** | Export manuel de métriques dé-identifiées (6 mois, agrégation hebdomadaire par utilisateur hashé). GA mars 2026. Export au niveau jour prévu avril 2026. | 50+ licences Copilot | Manuel puis upload |
| **Power BI Connector to Fabric** | Connecteur natif Viva Insights dans Dataflow Gen2. Supporte row-level et aggregated data. **Méthode recommandée par Microsoft.** | Viva Insights (rôle Insights Analyst) | Natif (Dataflow Gen2) |
| **OData Feed (Advanced Analysis)** | Lien OData généré depuis Query Designer > Results. Exploitable en Power BI Desktop puis publishable vers Fabric. | Viva Insights (rôle Insights Analyst) | Via Power BI publish |
| **Custom Person Query** | Requête personnalisée avec 100+ métriques Copilot sélectionnables, filtres par attributs organisationnels. Export CSV ou OData. | Viva Insights (rôle Insights Analyst) | CSV ou OData |

### 3.3 Approche recommandée pour le pipeline

La voie privilégiée est le **Power BI Connector dans un Dataflow Gen2 Fabric** : on crée une requête Copilot dans Viva Insights (Advanced Analysis), on récupère le Partition ID et le Query ID, puis on configure un Dataflow Gen2 avec le connecteur Viva Insights comme source et un Lakehouse comme destination.

> **Note** : Dataflow Gen2 peut écrire vers de multiples destinations dont Lakehouse Delta table, Lakehouse file, Warehouse table, SQL Database table, et SharePoint file. Les destinations sont configurables par requête au sein d'un même Dataflow.

---

## 4. Source 3 — Microsoft Graph Reports API (Copilot Usage)

### 4.1 Données disponibles

L'API de rapports Microsoft Graph expose les métriques d'utilisation de Microsoft 365 Copilot au niveau utilisateur et tenant :

- Date de dernière activité par application (Teams, Word, Excel, PowerPoint, Outlook, OneNote, Loop, Copilot Chat)
- Nombre d'utilisateurs actifs et activés par application
- Tendances quotidiennes d'utilisation

### 4.2 Endpoints principaux

> **Vérification Context7** : les endpoints ci-dessous sont confirmés. La permission requise est `Reports.Read.All` (Application permission). Pour les permissions déléguées, l'utilisateur doit disposer d'un rôle admin M365 (Global Reader, Reports Reader, etc.).

| Endpoint | URL | Contenu |
|---|---|---|
| **Copilot Usage User Detail** | `GET /beta/copilot/reports/getMicrosoft365CopilotUsageUserDetail(period='D7')` | Détail par utilisateur licencié : dernière activité par app. Format CSV ou JSON. |
| **Copilot User Count Trend** | `GET /beta/copilot/reports/getMicrosoft365CopilotUserCountTrend(period='D7')` | Tendance quotidienne enabled/active users par app (anyApp, Teams, Word, Excel, PowerPoint, Outlook, OneNote, Loop, CopilotChat) |
| **Legacy path (déprécié)** | `GET /beta/reports/getMicrosoft365CopilotUsageUserDetail(period='D180')` | Ancien chemin — dépréciation prévue mars 2026, migration vers `/copilot/reports/` |

> **Transition beta → v1.0** : Microsoft a annoncé la disponibilité en v1.0 depuis décembre 2025. Les endpoints beta seront retirés à terme. Prévoir la migration du chemin `/beta/reports/` vers `/v1.0/copilot/reports/`.

#### Périodes supportées

`D7` (7 jours), `D30` (30 jours), `D90` (90 jours), `D180` (180 jours).

#### Format de sortie

Les endpoints supportent `$format=application/json` (par défaut) et `$format=text/csv`. Le format CSV est adapté à un chargement direct dans un Lakehouse via Notebook ou Pipeline.

#### Exemple de réponse JSON (User Detail)

```json
{
  "value": [
    {
      "reportRefreshDate": "2025-08-20",
      "userPrincipalName": "user@adenes.eu",
      "displayName": "Nom Utilisateur",
      "lastActivityDate": "2025-08-20",
      "copilotChatLastActivityDate": "2025-08-16",
      "microsoftTeamsCopilotLastActivityDate": "2025-08-20",
      "wordCopilotLastActivityDate": "2025-08-06",
      "excelCopilotLastActivityDate": "",
      "powerPointCopilotLastActivityDate": "2025-03-26",
      "outlookCopilotLastActivityDate": "",
      "oneNoteCopilotLastActivityDate": "",
      "loopCopilotLastActivityDate": "",
      "copilotActivityUserDetailsByPeriod": [{ "reportPeriod": 7 }]
    }
  ]
}
```

### 4.3 Limitation importante

**Cette API ne couvre que les utilisateurs licenciés Microsoft 365 Copilot.** L'usage de Copilot Chat sans licence (les 2456 utilisateurs identifiés dans le document CODIR) n'est pas disponible via Graph. Pour ces données, il faut passer par Viva Insights (source 2) ou l'Audit Log M365 (`Search-UnifiedAuditLog` en PowerShell, ou Office 365 Management Activity API).

---

## 5. Architecture cible du pipeline Fabric

L'architecture proposée s'organise en trois couches dans un workspace Fabric dédié :

| Couche | Composant Fabric | Source | Fréquence |
|---|---|---|---|
| **Ingestion** | Data Pipeline (Notebook Activity) | Graph API Cloud Discovery | Quotidien |
| **Ingestion** | Dataflow Gen2 (connecteur Viva Insights) | Viva Insights Custom Query | Hebdomadaire |
| **Ingestion** | Data Pipeline (Notebook Activity) | Graph Reports API Copilot Usage | Quotidien |
| **Stockage** | Lakehouse (tables Delta) | Bronze : données brutes JSON/CSV | — |
| **Transformation** | Notebook Spark / Dataflow Gen2 | Silver : données nettoyées, typées, enrichies | Post-ingestion |
| **Restitution** | Semantic Model + Power BI Report | Gold : modèle étoile pour tableaux de bord CODIR | Actualisé après ETL |

---

## 6. Prérequis transverses et enregistrement d'application

### 6.1 Enregistrement Entra ID (App Registration)

Une seule App Registration dans Microsoft Entra ID peut couvrir les deux sources basées sur Graph API (Cloud Discovery + Reports). Les étapes sont les suivantes :

1. Dans le portail Entra ID, accéder à **App registrations > New registration**.
2. Nommer l'application (ex : « Fabric-Pipeline-AI-Analytics »), type **Single tenant**.
3. Dans **API permissions**, ajouter les permissions suivantes :

| Permission | Type | API | Source |
|---|---|---|---|
| `CloudApp-Discovery.Read.All` | Application | Microsoft Graph | Cloud Discovery |
| `Reports.Read.All` | Application | Microsoft Graph | Copilot Usage |

4. Cliquer sur **Grant admin consent** pour le tenant ADENES.
5. Dans **Certificates & secrets**, créer un Client Secret (ou certificat X.509 pour la production) et noter la valeur.
6. Noter le **Application (client) ID** et le **Directory (tenant) ID**.

### 6.2 Prérequis Viva Insights

1. Disposer d'au moins 50 licences Microsoft 365 Copilot ou Viva Insights.
2. Assigner le rôle **Insights Analyst** à l'utilisateur technique qui exécutera les requêtes.
3. Vérifier que le **Viva Insights web app** est activé pour le tenant (VFAM dans M365 admin center).
4. Créer une ou plusieurs **Custom Person Queries** avec les métriques Copilot souhaitées et activer l'**Auto-refresh**.

### 6.3 Prérequis Fabric

1. Un workspace Fabric avec capacité (F64+ ou trial).
2. Un Lakehouse créé dans ce workspace (ex : `lkh_ai_analytics`).
3. Stocker le Client ID et le Client Secret dans un **Azure Key Vault** référencé par Fabric.

### 6.4 Prérequis pour l'automatisation du déploiement

Pour automatiser la création des composants Fabric via API (section 9), une App Registration complémentaire ou la même (étendue) nécessite les permissions supplémentaires suivantes :

| Permission / Scope | Type | API | Usage |
|---|---|---|---|
| `Workspace.ReadWrite.All` | Delegated | Fabric API | Créer/modifier des workspaces |
| `Lakehouse.ReadWrite.All` ou `Item.ReadWrite.All` | Delegated | Fabric API | Créer des Lakehouses |
| `Item.ReadWrite.All` | Delegated | Fabric API | Créer Notebooks et Pipelines |
| `Item.Execute.All` | Delegated | Fabric API | Exécuter des jobs (Pipeline, Notebook) |
| `Application.ReadWrite.All` | Application | Microsoft Graph | Créer/modifier des App Registrations (optionnel, si automatisation Entra) |

> **Note** : Les APIs Fabric REST supportent l'authentification par **Service Principal** et **Managed Identity** pour les opérations CRUD et le Job Scheduler sur les Notebooks et Pipelines.

---

## 7. Étapes de paramétrage détaillées

### 7.1 Pipeline Cloud Discovery (Graph API → Lakehouse)

1. Dans le workspace Fabric, créer un **Notebook Python**.
2. Obtenir un token OAuth2 via **MSAL** (client_credentials flow) avec les identifiants de l'App Registration.
3. Appeler `GET /beta/security/dataDiscovery/cloudAppDiscovery/uploadedStreams` pour récupérer le streamId du flux MDE.
4. Appeler `GET .../uploadedStreams/{streamId}/microsoft.graph.security.aggregatedAppsDetails(period=duration'P90D')` avec le filtre sur les catégories IA.
5. Pour chaque application, appeler l'endpoint `/users` pour récupérer le détail par utilisateur.
6. Transformer les réponses JSON en DataFrames Spark et écrire en tables Delta dans le Lakehouse (`bronze.cloud_discovery_apps`, `bronze.cloud_discovery_users`).
7. Encapsuler le Notebook dans un **Data Pipeline** avec déclenchement planifié quotidien.

#### Exemple de code Notebook (extrait)

```python
import msal
import requests
import json

# Configuration (en production, récupérer depuis Key Vault)
tenant_id = "<tenant_id>"
client_id = "<client_id>"
client_secret = "<client_secret>"  # En prod : mssparkutils.credentials.getSecret(...)

# Authentification MSAL
app = msal.ConfidentialClientApplication(
    client_id,
    authority=f"https://login.microsoftonline.com/{tenant_id}",
    client_credential=client_secret
)
token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
headers = {"Authorization": f"Bearer {token['access_token']}"}

# 1. Récupérer les streams
streams_url = "https://graph.microsoft.com/beta/security/dataDiscovery/cloudAppDiscovery/uploadedStreams"
streams = requests.get(streams_url, headers=headers).json()
stream_id = streams["value"][0]["id"]  # Stream MDE

# 2. Récupérer les apps découvertes (90 jours)
apps_url = (
    f"https://graph.microsoft.com/beta/security/dataDiscovery/cloudAppDiscovery/"
    f"uploadedStreams/{stream_id}/microsoft.graph.security.aggregatedAppsDetails"
    f"(period=duration'P90D')"
)
apps_response = requests.get(apps_url, headers=headers).json()

# 3. Conversion en DataFrame Spark et écriture Delta
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df_apps = spark.createDataFrame(apps_response["value"])
df_apps.write.mode("overwrite").format("delta").saveAsTable("bronze.cloud_discovery_apps")
```

### 7.2 Pipeline Viva Insights (Dataflow Gen2 → Lakehouse)

1. Dans Viva Insights > **Analyze > Query designer**, créer une **Custom Person Query** avec les métriques Copilot (adoption, actions, agents). Activer **Auto-refresh**.
2. Une fois la requête exécutée, aller dans **Results** et copier le **Partition ID** et le **Query ID**.
3. Dans Fabric, créer un **Dataflow Gen2**. Sélectionner **Get Data > Online Services > Viva Insights**.
4. Renseigner le Partition ID et Query ID. Choisir **Row-level data** (pour un chargement complet) ou **Aggregated data** (avec respect automatique du minimum group size).
5. S'authentifier avec un compte organisationnel disposant du rôle **Insights Analyst**.
6. Configurer la **destination** : sélectionner le Lakehouse cible et définir le nom de table (ex : `bronze.viva_copilot_metrics`). Choisir le mode **Append**.
7. Planifier le refresh du Dataflow Gen2 (hebdomadaire, aligné sur le cycle de rafraîchissement Viva Insights).

### 7.3 Pipeline Graph Copilot Usage (Graph API → Lakehouse)

Le même pattern que le pipeline Cloud Discovery est applicable. La différence réside dans les endpoints appelés et la permission utilisée (`Reports.Read.All`).

```python
# Copilot Usage User Detail (7 derniers jours, format JSON)
usage_url = (
    "https://graph.microsoft.com/beta/copilot/reports/"
    "getMicrosoft365CopilotUsageUserDetail(period='D7')"
    "?$format=application/json"
)
usage_data = requests.get(usage_url, headers=headers).json()
df_usage = spark.createDataFrame(usage_data["value"])
df_usage.write.mode("overwrite").format("delta").saveAsTable("bronze.copilot_usage_detail")

# Copilot User Count Trend (30 derniers jours)
trend_url = (
    "https://graph.microsoft.com/beta/copilot/reports/"
    "getMicrosoft365CopilotUserCountTrend(period='D30')"
    "?$format=application/json"
)
trend_data = requests.get(trend_url, headers=headers).json()
# Extraction du tableau imbriqué adoptionByDate
from pyspark.sql.functions import explode, col
df_trend_raw = spark.createDataFrame(trend_data["value"])
df_trend = df_trend_raw.select(explode("adoptionByDate").alias("day")).select("day.*")
df_trend.write.mode("overwrite").format("delta").saveAsTable("bronze.copilot_usage_trend")
```

**Alternative low-code** : une **Copy Activity** dans un Data Pipeline Fabric peut appeler directement une API REST comme source (type REST connector) avec authentification OAuth2 client_credentials, et cibler une table Lakehouse en destination.

---

## 8. Modèle de données cible dans le Lakehouse

### 8.1 Couche Bronze (données brutes)

| Table | Colonnes clés | Source |
|---|---|---|
| `bronze.cloud_discovery_apps` | appId, displayName, category, riskScore, userCount, transactionCount, uploadNetworkTrafficInBytes, downloadNetworkTrafficInBytes, lastSeenDateTime, deviceCount, domains | Graph Cloud Discovery |
| `bronze.cloud_discovery_users` | appId, userIdentifier, transactionCount, uploadBytes, downloadBytes | Graph Cloud Discovery |
| `bronze.viva_copilot_metrics` | personId (hashé), metricDate, copilotActionsInWord, copilotActionsInTeams, copilotChatActions, agentActions, organization, function | Viva Insights Dataflow Gen2 |
| `bronze.copilot_usage_detail` | userPrincipalName, displayName, lastActivityDate, *CopilotLastActivityDate (par app), reportPeriod | Graph Reports API |
| `bronze.copilot_usage_trend` | reportDate, *EnabledUsers, *ActiveUsers (par app), reportPeriod | Graph Reports API |

### 8.2 Couche Silver (nettoyage et enrichissement)

Les tables Silver réalisent la jointure avec les données organisationnelles Entra ID (département, filiale, site) et la normalisation des identifiants. Tables recommandées :

- `silver.fact_ai_app_usage` — union Cloud Discovery + Copilot usage
- `silver.fact_copilot_actions` — depuis Viva Insights
- `silver.dim_users` — enrichi depuis Entra ID
- `silver.dim_ai_applications` — référentiel des apps IA découvertes

### 8.3 Couche Gold (modèle sémantique)

Un Semantic Model Power BI construit sur le Lakehouse SQL endpoint, exposant les mesures DAX pour le rapport CODIR :

- Nombre d'utilisateurs actifs Copilot (licenciés + non-licenciés)
- Volume de données Shadow AI (upload vers ChatGPT, Gemini, etc.)
- Ratio Copilot vs ChatGPT
- Tendance d'adoption à 6 mois
- Top agents Copilot utilisés
- Ventilation par filiale ADENES (ELEX, VRS Vering, 3C Expertises, Roadia)

---

## 9. Automatisation du déploiement via APIs Fabric, Graph et Entra

Cette section décrit comment automatiser le provisionnement de l'ensemble de l'infrastructure du projet (workspace, lakehouse, notebooks, pipelines, planification) via les APIs REST, dans une logique d'Infrastructure as Code reproductible.

### 9.1 Vue d'ensemble des APIs de déploiement

| Composant | API | Endpoint | Méthode |
|---|---|---|---|
| App Registration Entra | Microsoft Graph v1.0 | `POST /v1.0/applications` | Application.ReadWrite.All |
| Service Principal | Microsoft Graph v1.0 | `POST /v1.0/servicePrincipals` | Application.ReadWrite.OwnedBy |
| Consentement admin | Microsoft Graph v1.0 | `POST /v1.0/oauth2PermissionGrants` | DelegatedPermissionGrant.ReadWrite.All |
| Workspace Fabric | Fabric REST API v1 | `POST https://api.fabric.microsoft.com/v1/workspaces` | Workspace.ReadWrite.All |
| Lakehouse | Fabric REST API v1 | `POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/lakehouses` | Lakehouse.ReadWrite.All |
| Notebook | Fabric REST API v1 | `POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/notebooks` | Item.ReadWrite.All |
| Data Pipeline | Fabric REST API v1 | `POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items` (type: DataPipeline) | Item.ReadWrite.All |
| Planification (Job Scheduler) | Fabric REST API v1 | `POST .../items/{itemId}/jobs/{jobType}/schedules` | Item.Execute.All |
| Exécution on-demand | Fabric REST API v1 | `POST .../items/{itemId}/jobs/instances?jobType=Pipeline` | Item.Execute.All |

### 9.2 Étape 1 : Création de l'App Registration via Graph API

```python
import requests

# Token admin obtenu via device code flow ou un autre Service Principal privilégié
admin_headers = {"Authorization": f"Bearer {admin_token}", "Content-Type": "application/json"}

# Créer l'application
app_payload = {
    "displayName": "Fabric-Pipeline-AI-Analytics",
    "signInAudience": "AzureADMyOrg",
    "requiredResourceAccess": [
        {
            "resourceAppId": "00000003-0000-0000-c000-000000000000",  # Microsoft Graph
            "resourceAccess": [
                {"id": "e4c9e354-4dc5-45b8-9e7c-e1393b0b1a20", "type": "Role"},  # CloudApp-Discovery.Read.All
                {"id": "230c1aed-a721-4c5d-9cb4-a90514e508ef", "type": "Role"}   # Reports.Read.All
            ]
        }
    ]
}
resp = requests.post("https://graph.microsoft.com/v1.0/applications", headers=admin_headers, json=app_payload)
app_id = resp.json()["appId"]
object_id = resp.json()["id"]

# Créer un secret
secret_payload = {"passwordCredential": {"displayName": "FabricPipeline", "endDateTime": "2027-03-25T00:00:00Z"}}
secret_resp = requests.post(
    f"https://graph.microsoft.com/v1.0/applications/{object_id}/addPassword",
    headers=admin_headers, json=secret_payload
)
client_secret = secret_resp.json()["secretText"]

# Créer le Service Principal
sp_payload = {"appId": app_id}
sp_resp = requests.post("https://graph.microsoft.com/v1.0/servicePrincipals", headers=admin_headers, json=sp_payload)
sp_id = sp_resp.json()["id"]
```

### 9.3 Étape 2 : Création du Workspace Fabric

```python
import requests

# Token Fabric (scope : https://api.fabric.microsoft.com/.default)
fabric_headers = {"Authorization": f"Bearer {fabric_token}", "Content-Type": "application/json"}

# Créer le workspace
ws_payload = {
    "displayName": "WS_AI_Analytics_ADENES",
    "description": "Workspace dédié au pipeline d'analytics IA et Copilot",
    "capacityId": "<capacity_id>"  # ID de la capacité Fabric
}
ws_resp = requests.post("https://api.fabric.microsoft.com/v1/workspaces", headers=fabric_headers, json=ws_payload)
workspace_id = ws_resp.json()["id"]
print(f"Workspace créé : {workspace_id}")
```

### 9.4 Étape 3 : Création du Lakehouse

```python
# Créer le Lakehouse avec schemas activés
lkh_payload = {
    "displayName": "lkh_ai_analytics",
    "description": "Lakehouse pour les données d'utilisation IA et Copilot",
    "creationPayload": {"enableSchemas": True}
}
lkh_resp = requests.post(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses",
    headers=fabric_headers, json=lkh_payload
)

# Gérer la réponse LRO (Long Running Operation) si 202
if lkh_resp.status_code == 202:
    operation_url = lkh_resp.headers["Location"]
    # Polling de l'opération jusqu'à complétion
    import time
    while True:
        op_status = requests.get(operation_url, headers=fabric_headers).json()
        if op_status["status"] in ("Succeeded", "Failed"):
            break
        time.sleep(int(lkh_resp.headers.get("Retry-After", 30)))
    lakehouse_id = op_status["id"]
else:
    lakehouse_id = lkh_resp.json()["id"]

print(f"Lakehouse créé : {lakehouse_id}")
```

### 9.5 Étape 4 : Création des Notebooks

Les Notebooks peuvent être créés via l'API avec leur contenu encodé en Base64. Deux formats sont supportés : `fabricGitSource` (fichier `.py`) et `ipynb` (format Jupyter standard).

```python
import base64

# Contenu du notebook d'ingestion Cloud Discovery
notebook_content = """# Notebook: Ingestion Cloud Discovery
# Ce notebook est exécuté quotidiennement par le pipeline

import msal, requests, json
from pyspark.sql import SparkSession

# ... (code d'ingestion complet)
"""

# Encoder en base64
content_b64 = base64.b64encode(notebook_content.encode()).decode()

# Créer le notebook via API
nb_payload = {
    "displayName": "nb_ingest_cloud_discovery",
    "description": "Notebook d'ingestion quotidienne Cloud Discovery via Graph API",
    "definition": {
        "format": "fabricGitSource",
        "parts": [
            {
                "path": "notebook-content.py",
                "payload": content_b64,
                "payloadType": "InlineBase64"
            },
            {
                "path": ".platform",
                "payload": base64.b64encode(json.dumps({
                    "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
                    "metadata": {"type": "Notebook", "displayName": "nb_ingest_cloud_discovery"},
                    "config": {"version": "2.0", "logicalId": "00000000-0000-0000-0000-000000000001"}
                }).encode()).decode(),
                "payloadType": "InlineBase64"
            }
        ]
    }
}
nb_resp = requests.post(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks",
    headers=fabric_headers, json=nb_payload
)
notebook_id = nb_resp.json()["id"] if nb_resp.status_code == 201 else None
```

### 9.6 Étape 5 : Création du Data Pipeline

```python
# Créer un pipeline vide
pip_payload = {
    "displayName": "pip_daily_ai_ingestion",
    "type": "DataPipeline",
    "description": "Pipeline quotidien d'ingestion des données IA (Cloud Discovery + Copilot Usage)"
}
pip_resp = requests.post(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items",
    headers=fabric_headers, json=pip_payload
)
pipeline_id = pip_resp.json()["id"]

# Mettre à jour le pipeline avec la définition (Notebook Activity)
# La définition du pipeline est un JSON encodé en base64 décrivant les activités
pipeline_definition = {
    "properties": {
        "activities": [
            {
                "name": "Ingest_Cloud_Discovery",
                "type": "TridentNotebook",
                "typeProperties": {
                    "notebookId": notebook_id,
                    "workspaceId": workspace_id
                }
            }
        ]
    }
}
pip_def_b64 = base64.b64encode(json.dumps(pipeline_definition).encode()).decode()
update_payload = {
    "definition": {
        "parts": [
            {"path": "pipeline-content.json", "payload": pip_def_b64, "payloadType": "InlineBase64"}
        ]
    }
}
requests.post(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/updateDefinition",
    headers=fabric_headers, json=update_payload
)
```

### 9.7 Étape 6 : Planification automatique (Job Scheduler)

```python
# Planifier le pipeline pour une exécution quotidienne à 6h00
schedule_payload = {
    "enabled": True,
    "configuration": {
        "startDateTime": "2026-04-01T06:00:00",
        "endDateTime": "2027-03-31T23:59:00",
        "localTimeZoneId": "Romance Standard Time",  # Fuseau horaire France
        "type": "Cron",
        "interval": 1440  # Minutes (= 24 heures)
    }
}
requests.post(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/Pipeline/schedules",
    headers=fabric_headers, json=schedule_payload
)
```

### 9.8 Étape 7 : Exécution on-demand et monitoring

```python
# Lancer le pipeline manuellement
run_resp = requests.post(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline",
    headers=fabric_headers
)
# Le job ID est dans le header Location
job_location = run_resp.headers["Location"]

# Polling du statut
import time
while True:
    status = requests.get(job_location, headers=fabric_headers).json()
    print(f"Statut : {status.get('status', 'Unknown')}")
    if status.get("status") in ("Completed", "Failed", "Cancelled"):
        break
    time.sleep(30)

# Vérification : lister les tables du Lakehouse
tables_resp = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables",
    headers=fabric_headers
)
print("Tables disponibles :", [t["name"] for t in tables_resp.json().get("data", [])])
```

### 9.9 Script d'orchestration complet

L'ensemble des étapes 1 à 7 peut être encapsulé dans un script Python unique (ou PowerShell) exécutable depuis un poste d'administration, un pipeline Azure DevOps, ou un GitHub Action. Le script doit :

1. S'authentifier via un Service Principal disposant des permissions listées en section 6.4
2. Vérifier l'existence des composants avant création (idempotence)
3. Gérer les Long Running Operations (polling des headers `Location` et `Retry-After`)
4. Stocker les secrets dans Azure Key Vault
5. Produire un fichier de configuration de sortie (JSON) listant tous les IDs créés pour la traçabilité

---

## 10. Limitations et points d'attention

| Sujet | Détail |
|---|---|
| **API Graph beta** | L'endpoint Cloud Discovery est en beta. Les schémas peuvent évoluer. Prévoir une couche d'abstraction dans le Notebook pour absorber les changements. Les durées supportées sont P7D, P30D et P90D uniquement. |
| **Transition Copilot API beta → v1.0** | La migration du chemin `/beta/reports/` vers `/v1.0/copilot/reports/` est attendue. L'ancien chemin sera retiré — prévoir la mise à jour des Notebooks. |
| **Périodes de données** | Cloud Discovery : 7, 30 ou 90 jours glissants. Graph Reports : 7, 30, 90 ou 180 jours. Viva Insights : jusqu'à 13 mois d'historique Copilot. Prévoir un stockage historisé dans le Lakehouse pour constituer un historique long. |
| **Données anonymisées** | L'export Copilot Dashboard fournit des données dé-identifiées (hashed ID). Pour des données nominatives, utiliser le Graph Reports API (nécessite désactiver l'obfuscation dans M365 admin center > Settings > Org settings > Reports). |
| **Copilot Chat sans licence** | Les 2456 utilisateurs Copilot Chat (sans licence) identifiés dans le document CODIR ne sont accessibles que via Viva Insights ou l'Audit Log M365, pas via la Graph Reports API. |
| **Throttling** | Les APIs Graph sont soumises au throttling standard (429 Too Many Requests). Implémenter un retry avec backoff exponentiel dans le Notebook. Le Dataflow Gen2 gère cela automatiquement. |
| **Sécurité des secrets** | Ne jamais stocker le Client Secret en clair dans le code du Notebook. Utiliser Azure Key Vault via `mssparkutils.credentials.getSecret()` dans les Notebooks Fabric. |
| **Bug métriques Copilot** | Microsoft a signalé une sous-estimation des métriques *Generate email draft* et *Summarize email thread* dans Viva Insights entre juin 2025 et février 2026. Les données à partir du 18 février 2026 sont corrigées ; les données historiques seront restaurées. |
| **Fabric REST API — Service Principal** | L'authentification par Service Principal est supportée pour les opérations CRUD et le Job Scheduler sur Notebooks et Pipelines. Il faut cependant que le tenant admin ait activé le paramètre « Service principals can create workspaces, connections, and deployment pipelines » dans le portail admin Fabric. |
| **LRO (Long Running Operations)** | La création de Lakehouse et de Notebooks avec définition retourne souvent un 202 Accepted. Le script d'automatisation doit implémenter le polling de l'URL fournie dans le header `Location` avec respect du `Retry-After`. |

---

## Références documentaires

- [Work with discovered apps via Graph API](https://learn.microsoft.com/en-us/defender-cloud-apps/discovered-apps-api-graph) — Microsoft Learn
- [Defender for Cloud Apps cloud discovery API](https://learn.microsoft.com/en-us/defender-cloud-apps/api-discovery) — Microsoft Learn
- [Copilot Analytics introduction (Viva Insights)](https://learn.microsoft.com/en-us/viva/insights/copilot-analytics-introduction) — Microsoft Learn
- [Export Copilot metrics from the Copilot Dashboard](https://learn.microsoft.com/en-us/viva/insights/org-team-insights/export-copilot-metrics) — Microsoft Learn
- [Export query data using the Power BI connector](https://learn.microsoft.com/en-us/viva/insights/advanced/analyst/power-bi-connector) — Microsoft Learn
- [copilotReportRoot: getMicrosoft365CopilotUsageUserDetail](https://learn.microsoft.com/en-us/microsoft-365-copilot/extensibility/api/admin-settings/reports/copilotreportroot-getmicrosoft365copilotusageuserdetail) — Microsoft Learn
- [Export Viva Insights data using MGDC (retrait annoncé)](https://learn.microsoft.com/en-us/viva/insights/advanced/admin/dynamic-metric-load) — Microsoft Learn
- [Fabric REST API — Create Workspace](https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/create-workspace) — Microsoft Learn
- [Fabric REST API — Create Lakehouse](https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/items/create-lakehouse) — Microsoft Learn
- [Fabric REST API — Create Notebook](https://learn.microsoft.com/en-us/rest/api/fabric/notebook/items/create-notebook) — Microsoft Learn
- [Fabric Data Factory pipeline REST API capabilities](https://learn.microsoft.com/en-us/fabric/data-factory/pipeline-rest-api-capabilities) — Microsoft Learn
- [Notebook public APIs (CRUD + Job Scheduler)](https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-public-api) — Microsoft Learn
- [Dataflow Gen2 data destinations](https://learn.microsoft.com/en-us/fabric/data-factory/connector-lakehouse) — Microsoft Learn
