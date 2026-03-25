# Fabric AI Analytics Deployer

Script Python d'orchestration pour provisionner l'infrastructure Microsoft Fabric
du pipeline d'analytics IA d'ADENES en une seule exécution.

## Prérequis

1. Python 3.10+
2. Activer dans le portail admin Fabric :
   - « Service principals can use Fabric APIs »
   - « Service principals can create workspaces, connections, and deployment pipelines »
3. Pour l'étape 1 (Entra), un compte avec `Application.ReadWrite.All` + `AppRoleAssignment.ReadWrite.All`

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Copier `.env.example` en `.env` et renseigner les valeurs :

```bash
cp .env.example .env
```

Pour l'étape 1, obtenir un token admin :
```bash
az account get-access-token --resource https://graph.microsoft.com --query accessToken -o tsv
```

## Utilisation

```bash
# Déploiement complet
python -m deploy

# Reprendre depuis l'étape 3 (après un échec)
python -m deploy --from-step 3

# Lister les étapes
python -m deploy --list-steps
```

## Étapes

| # | Description |
|---|---|
| 1 | App Registration Entra + Service Principal + permissions |
| 2 | Workspace Fabric + rôle SPN Contributor |
| 3 | Lakehouse `lkh_ai_analytics` |
| 4 | Notebooks d'ingestion (Cloud Discovery + Copilot Usage) |
| 5 | Data Pipeline `pip_daily_ai_ingestion` |
| 6 | Planification quotidienne 6h00 (Romance Standard Time) |
| 7 | Exécution test on-demand + vérification tables Bronze |

## Configuration Viva Insights (manuelle)

Le Dataflow Gen2 Viva Insights n'est pas provisionné par ce script
(API Fabric REST non disponible). Configuration manuelle :
1. Créer une Custom Person Query dans Viva Insights Advanced Analysis
2. Créer un Dataflow Gen2 dans Fabric avec le connecteur Viva Insights
3. Destination : Lakehouse `lkh_ai_analytics`, table `bronze.viva_copilot_metrics`
4. Planifier en refresh hebdomadaire

## Sortie

Le fichier `deploy-output.json` est généré avec les IDs et URLs de toutes les ressources.
