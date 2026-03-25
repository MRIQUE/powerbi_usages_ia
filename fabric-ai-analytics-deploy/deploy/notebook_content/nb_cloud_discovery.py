# Notebook: Ingestion Cloud Discovery via Microsoft Graph Beta API
# Exécuté quotidiennement par le pipeline pip_daily_ai_ingestion
# Prérequis : Key Vault référencé dans le Lakehouse, permission CloudApp-Discovery.Read.All

import time
import requests
import msal
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, FloatType
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
# Endpoint: /beta/security/dataDiscovery/cloudAppDiscovery/uploadedStreams
DISCOVERY_BASE = f"{GRAPH_BASE}/beta/security/dataDiscovery/cloudAppDiscovery"
AI_CATEGORIES_FILTER = "category eq 'Generative AI' or category eq 'AI Model Provider'"
PERIOD = "P90D"

# ---------------------------------------------------------------------------
# 2. Authentification MSAL avec renouvellement automatique
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


# ---------------------------------------------------------------------------
# 4. Pagination automatique
# ---------------------------------------------------------------------------
def paginate(url):
    results = []
    while url:
        resp = get_with_retry(url).json()
        results.extend(resp.get("value", []))
        url = resp.get("@odata.nextLink")
    return results


# ---------------------------------------------------------------------------
# 5. Récupération du streamId MDE
# ---------------------------------------------------------------------------
t_start = time.time()
streams = get_with_retry(f"{DISCOVERY_BASE}/uploadedStreams").json()
stream_id = next(s["id"] for s in streams["value"])
print(f"[INFO] Stream MDE : {stream_id}")

# ---------------------------------------------------------------------------
# 6. Apps découvertes (filtrage sur catégories IA)
# ---------------------------------------------------------------------------
apps_url = (
    f"{DISCOVERY_BASE}/uploadedStreams/{stream_id}/"
    f"microsoft.graph.security.aggregatedAppsDetails(period=duration'{PERIOD}')"
    f"?$filter={AI_CATEGORIES_FILTER}"
    f"&$select=displayName,category,riskScore,userCount,transactionCount,"
    f"uploadNetworkTrafficInBytes,downloadNetworkTrafficInBytes,lastSeenDateTime,deviceCount"
)
apps = paginate(apps_url)
print(f"[INFO] Apps découvertes : {len(apps)}")

# ---------------------------------------------------------------------------
# 7. Utilisateurs par app (pagination)
# ---------------------------------------------------------------------------
users_rows = []
for app in apps:
    app_id = app.get("id", app.get("appId", ""))
    users_url = (
        f"{DISCOVERY_BASE}/uploadedStreams/{stream_id}/"
        f"microsoft.graph.security.aggregatedAppsDetails(period=duration'{PERIOD}')/{app_id}/users"
    )
    try:
        users = paginate(users_url)
        for u in users:
            users_rows.append({
                "appId": app_id,
                "userIdentifier": u.get("userIdentifier", ""),
                "transactionCount": u.get("transactionCount", 0),
                "uploadBytes": u.get("uploadNetworkTrafficInBytes", 0),
                "downloadBytes": u.get("downloadNetworkTrafficInBytes", 0),
                "reportDate": app.get("lastSeenDateTime", "")[:10],
            })
    except Exception as e:
        print(f"[WARN] Erreur users pour app {app_id}: {e}")

# ---------------------------------------------------------------------------
# 8. Écriture Delta — bronze.cloud_discovery_apps
# ---------------------------------------------------------------------------
spark = SparkSession.builder.getOrCreate()

apps_schema = StructType([
    StructField("appId", StringType()),
    StructField("displayName", StringType()),
    StructField("category", StringType()),
    StructField("riskScore", FloatType()),
    StructField("userCount", IntegerType()),
    StructField("transactionCount", LongType()),
    StructField("uploadNetworkTrafficInBytes", LongType()),
    StructField("downloadNetworkTrafficInBytes", LongType()),
    StructField("lastSeenDateTime", StringType()),
    StructField("deviceCount", IntegerType()),
])

apps_rows = [{
    "appId": a.get("id", a.get("appId", "")),
    "displayName": a.get("displayName", ""),
    "category": a.get("category", ""),
    "riskScore": float(a.get("riskScore", 0) or 0),
    "userCount": int(a.get("userCount", 0) or 0),
    "transactionCount": int(a.get("transactionCount", 0) or 0),
    "uploadNetworkTrafficInBytes": int(a.get("uploadNetworkTrafficInBytes", 0) or 0),
    "downloadNetworkTrafficInBytes": int(a.get("downloadNetworkTrafficInBytes", 0) or 0),
    "lastSeenDateTime": str(a.get("lastSeenDateTime", "") or ""),
    "deviceCount": int(a.get("deviceCount", 0) or 0),
} for a in apps]

df_apps = spark.createDataFrame(apps_rows, schema=apps_schema)
df_apps.dropDuplicates(["appId", "lastSeenDateTime"]) \
       .write.mode("append").format("delta").saveAsTable("bronze.cloud_discovery_apps")
print(f"[INFO] bronze.cloud_discovery_apps : {df_apps.count()} lignes écrites")

# ---------------------------------------------------------------------------
# 9. Écriture Delta — bronze.cloud_discovery_users
# ---------------------------------------------------------------------------
users_schema = StructType([
    StructField("appId", StringType()),
    StructField("userIdentifier", StringType()),
    StructField("transactionCount", LongType()),
    StructField("uploadBytes", LongType()),
    StructField("downloadBytes", LongType()),
    StructField("reportDate", StringType()),
])

df_users = spark.createDataFrame(users_rows, schema=users_schema)
df_users.dropDuplicates(["appId", "userIdentifier", "reportDate"]) \
        .write.mode("append").format("delta").saveAsTable("bronze.cloud_discovery_users")
print(f"[INFO] bronze.cloud_discovery_users : {df_users.count()} lignes écrites")

print(f"[INFO] Durée totale : {time.time() - t_start:.1f}s")
