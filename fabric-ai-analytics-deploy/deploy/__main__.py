from __future__ import annotations
import argparse
import sys
import logging
import msal

from deploy.config import load_config, ConfigError
from deploy.logger import setup_logger
from deploy.output import read_output, write_output, get_id, build_portal_url, OutputError
from deploy.entra import provision_entra
from deploy.fabric_workspace import provision_workspace
from deploy.fabric_lakehouse import provision_lakehouse
from deploy.fabric_notebooks import provision_notebooks
from deploy.fabric_pipelines import provision_pipeline, provision_schedule, run_and_monitor

FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"

_STEPS = [
    (1, "App Registration Entra + Service Principal + appRoleAssignments"),
    (2, "Workspace Fabric + SPN Contributor role"),
    (3, "Lakehouse lkh_ai_analytics"),
    (4, "Notebooks d'ingestion (Cloud Discovery + Copilot Usage)"),
    (5, "Data Pipeline pip_daily_ai_ingestion"),
    (6, "Planification Job Scheduler (DefaultJob, quotidien 6h00)"),
    (7, "Exécution on-demand + monitoring + vérification tables Delta"),
]


def _get_fabric_token(cfg) -> str:
    app = msal.ConfidentialClientApplication(
        cfg.client_id,
        authority=f"https://login.microsoftonline.com/{cfg.tenant_id}",
        client_credential=cfg.client_secret,
    )
    result = app.acquire_token_for_client(scopes=[FABRIC_SCOPE])
    return result["access_token"]


def _make_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def main() -> None:
    parser = argparse.ArgumentParser(description="Fabric AI Analytics Deployer")
    parser.add_argument("--from-step", type=int, default=1, metavar="N",
                        help="Reprendre depuis l'étape N (1-7)")
    parser.add_argument("--list-steps", action="store_true",
                        help="Afficher les étapes et quitter")
    args = parser.parse_args()

    if args.list_steps:
        for num, desc in _STEPS:
            print(f"  Step {num}: {desc}")
        sys.exit(0)

    if not 1 <= args.from_step <= 7:
        print(f"Erreur : --from-step doit être entre 1 et 7 (reçu: {args.from_step})")
        sys.exit(1)

    logger = setup_logger()
    logger.info("=== Fabric AI Analytics Deployer ===")

    try:
        cfg = load_config()
    except ConfigError as e:
        logger.error(f"Configuration manquante : {e}")
        sys.exit(1)

    fabric_token = _get_fabric_token(cfg)
    fabric_headers = _make_headers(fabric_token)
    admin_headers = _make_headers(cfg.admin_token) if cfg.admin_token else {}

    output = read_output() if args.from_step > 1 else {}
    steps_result = dict(output)

    from_step = args.from_step

    try:
        # Step 1: Entra
        if from_step <= 1:
            logger.info("[STEP 1] Provisionnement App Registration Entra...")
            result = provision_entra("Fabric-Pipeline-AI-Analytics", admin_headers)
            steps_result["1_entra"] = {
                "status": result.status,
                "appId": result.app_id,
                "objectId": result.object_id,
                "spnObjectId": result.spn_object_id,
                "portalUrl": build_portal_url("entra_app", app_id=result.app_id),
            }
            write_output(steps_result)
            logger.info(f"[STEP 1] {result.status.upper()} — appId: {result.app_id}")

        # Step 2: Workspace
        if from_step <= 2:
            logger.info("[STEP 2] Provisionnement Workspace Fabric...")
            spn_id = get_id(steps_result, "1_entra", "spnObjectId")
            result = provision_workspace(
                "WS_AI_Analytics_ADENES", cfg.capacity_id, spn_id, fabric_headers
            )
            steps_result["2_workspace"] = {
                "status": result.status,
                "workspaceId": result.workspace_id,
                "portalUrl": build_portal_url("workspace", workspace_id=result.workspace_id),
            }
            write_output(steps_result)
            logger.info(f"[STEP 2] {result.status.upper()} — workspaceId: {result.workspace_id}")

        ws_id = get_id(steps_result, "2_workspace", "workspaceId")

        # Step 3: Lakehouse
        if from_step <= 3:
            logger.info("[STEP 3] Provisionnement Lakehouse...")
            result = provision_lakehouse("lkh_ai_analytics", ws_id, fabric_headers)
            steps_result["3_lakehouse"] = {
                "status": result.status,
                "lakehouseId": result.lakehouse_id,
                "portalUrl": build_portal_url("lakehouse", workspace_id=ws_id,
                                               lakehouse_id=result.lakehouse_id),
            }
            write_output(steps_result)
            logger.info(f"[STEP 3] {result.status.upper()} — lakehouseId: {result.lakehouse_id}")

        lkh_id = get_id(steps_result, "3_lakehouse", "lakehouseId")

        # Step 4: Notebooks
        if from_step <= 4:
            logger.info("[STEP 4] Provisionnement Notebooks...")
            result = provision_notebooks(ws_id, fabric_headers)
            steps_result["4_notebooks"] = {
                "status": result.status,
                "ids": result.ids,
            }
            write_output(steps_result)
            logger.info(f"[STEP 4] {result.status.upper()} — ids: {result.ids}")

        nb_ids = get_id(steps_result, "4_notebooks", "ids")

        # Step 5: Pipeline
        if from_step <= 5:
            logger.info("[STEP 5] Provisionnement Data Pipeline...")
            result = provision_pipeline(
                ws_id, nb_ids["nb_cloud_discovery"], nb_ids["nb_copilot_usage"], fabric_headers
            )
            steps_result["5_pipeline"] = {
                "status": result.status,
                "pipelineId": result.pipeline_id,
                "portalUrl": build_portal_url("pipeline", workspace_id=ws_id,
                                               pipeline_id=result.pipeline_id),
            }
            write_output(steps_result)
            logger.info(f"[STEP 5] {result.status.upper()} — pipelineId: {result.pipeline_id}")

        pip_id = get_id(steps_result, "5_pipeline", "pipelineId")

        # Step 6: Schedule
        if from_step <= 6:
            logger.info("[STEP 6] Provisionnement planification Job Scheduler...")
            result = provision_schedule(ws_id, pip_id, fabric_headers)
            steps_result["6_schedule"] = {
                "status": result.status,
                "scheduleId": result.schedule_id,
                "nextRun": result.next_run,
            }
            write_output(steps_result)
            logger.info(f"[STEP 6] {result.status.upper()} — scheduleId: {result.schedule_id}")

        # Step 7: Test run
        if from_step <= 7:
            logger.info("[STEP 7] Exécution on-demand et vérification tables...")
            result = run_and_monitor(ws_id, pip_id, lkh_id, fabric_headers)
            steps_result["7_test_run"] = {
                "status": result.status,
                "jobInstanceId": result.job_instance_id,
                "durationSeconds": result.duration_seconds,
                "tablesVerified": result.tables_verified,
            }
            write_output(steps_result)
            logger.info(
                f"[STEP 7] COMPLETED — durée: {result.duration_seconds}s, "
                f"tables: {result.tables_verified}"
            )

        logger.info("=== Déploiement terminé avec succès ===")
        logger.info(f"Résultat écrit dans deploy-output.json")

    except (OutputError, ConfigError) as e:
        logger.error(f"Erreur de configuration ou d'état : {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Erreur inattendue : {e}")
        write_output(steps_result)
        sys.exit(1)


if __name__ == "__main__":
    main()
