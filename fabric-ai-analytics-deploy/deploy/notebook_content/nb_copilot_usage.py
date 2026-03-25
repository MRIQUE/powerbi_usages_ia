# Notebook: Ingestion Copilot Usage via Microsoft Graph Reports API v1.0
# Exécuté quotidiennement par le pipeline pip_daily_ai_ingestion
# Prérequis : Key Vault référencé dans le Lakehouse, permission Reports.Read.All
# Note: utilise /v1.0/copilot/reports/ (chemin /beta/copilot/reports/ est déprécié)

import time
import requests
import msal
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, MapType
)

# ---------------------------------------------------------------------------
# 1. Configuration (secrets depuis Key Vault via mssparkutils)
# ---------------------------------------------------------------------------
TENANT_ID = mssparkutils.env.getTenantId()
KEY_VAULT_URL = mssparkutils.env.getJobTags().get("keyVaultUrl", "")
KV_SECRET_NAME_CLIENT_ID = "fabric-pipeline-client-id"
KV_SECRET_NAME_CLIENT_SECRET = "fabric-pipeline-client-secret"

CLIENT_ID = mssparkutils.credentials.getSecret(KEY_VAULT_URL, KV_SECRET_NAME_CLIENT_ID)
CLIENT_SECRET = mssparkutils.credentials.getSecret(KEY_VAULT_URL, KV_SECRET_NAME_CLIENT_SECRET)

GRAPH_BASE = "https://graph.microsoft.com"
COPILOT_BASE = f"{GRAPH_BASE}/v1.0/copilot/reports"
PERIOD = "D30"

# ---------------------------------------------------------------------------
# 2. Authentification MSAL avec renouvellement
# ---------------------------------------------------------------------------
_msal_app = msal.ConfidentialClientApplication(
    CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET,
)
_token_cache = {"token": None, "expires_at": 0}


def get_headers():
    if time.time() > _token_cache["expires_at"] - 300:
        result = _msal_app.acquire_token_for_client(scopes=[f"{GRAPH_BASE}/.default"])
        _token_cache["token"] = result["access_token"]
        _token_cache["expires_at"] = time.time() + result.get("expires_in", 3600)
    return {"Authorization": f"Bearer {_token_cache['token']}"}


# ---------------------------------------------------------------------------
# 3. Retry avec backoff exponentiel
# ---------------------------------------------------------------------------
_RETRYABLE = {429, 500, 502, 503, 504}


def get_with_retry(url, max_attempts=5, base_delay=30):
    delay = base_delay
    for attempt in range(1, max_attempts + 1):
        resp = requests.get(url, headers=get_headers())
        if resp.status_code not in _RETRYABLE:
            resp.raise_for_status()
            return resp
        if attempt == max_attempts:
            resp.raise_for_status()
        wait = int(resp.headers.get("Retry-After", delay))
        print(f"[RETRY] {resp.status_code} — attente {wait}s (tentative {attempt}/{max_attempts})")
        time.sleep(wait)
        delay = min(delay * 2, 300)


t_start = time.time()
spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# 4. Copilot Usage User Detail
# ---------------------------------------------------------------------------
user_detail_url = (
    f"{COPILOT_BASE}/getMicrosoft365CopilotUsageUserDetail(period='{PERIOD}')"
    "?$format=application/json"
)
user_detail_data = get_with_retry(user_detail_url).json()
user_rows = user_detail_data.get("value", [])
print(f"[INFO] Copilot Usage User Detail : {len(user_rows)} enregistrements")

user_detail_schema = StructType([
    StructField("reportRefreshDate", StringType()),
    StructField("userPrincipalName", StringType()),
    StructField("displayName", StringType()),
    StructField("lastActivityDate", StringType()),
    StructField("copilotChatLastActivityDate", StringType()),
    StructField("microsoftTeamsCopilotLastActivityDate", StringType()),
    StructField("wordCopilotLastActivityDate", StringType()),
    StructField("excelCopilotLastActivityDate", StringType()),
    StructField("powerPointCopilotLastActivityDate", StringType()),
    StructField("outlookCopilotLastActivityDate", StringType()),
    StructField("oneNoteCopilotLastActivityDate", StringType()),
    StructField("loopCopilotLastActivityDate", StringType()),
])

df_detail = spark.createDataFrame(
    [{k: str(r.get(k, "") or "") for k in user_detail_schema.fieldNames()} for r in user_rows],
    schema=user_detail_schema,
)
df_detail.dropDuplicates(["userPrincipalName", "reportRefreshDate"]) \
         .write.mode("append").format("delta").saveAsTable("bronze.copilot_usage_detail")
print(f"[INFO] bronze.copilot_usage_detail : {df_detail.count()} lignes écrites")

# ---------------------------------------------------------------------------
# 5. Copilot User Count Trend (avec explosion de adoptionByDate)
# ---------------------------------------------------------------------------
trend_url = (
    f"{COPILOT_BASE}/getMicrosoft365CopilotUserCountTrend(period='{PERIOD}')"
    "?$format=application/json"
)
trend_data = get_with_retry(trend_url).json()
trend_rows = trend_data.get("value", [])
print(f"[INFO] Copilot User Count Trend : {len(trend_rows)} apps")

df_trend_raw = spark.createDataFrame(trend_rows)
df_trend = (
    df_trend_raw
    .select(explode("adoptionByDate").alias("day"))
    .select("day.*")
)
df_trend.dropDuplicates(["reportDate"]) \
        .write.mode("append").format("delta").saveAsTable("bronze.copilot_usage_trend")
print(f"[INFO] bronze.copilot_usage_trend : {df_trend.count()} lignes écrites")

print(f"[INFO] Durée totale : {time.time() - t_start:.1f}s")
