"""Central configuration. Every value can be overridden with an environment variable,
so the same code runs inside Docker (service names) or on the Windows host (localhost)."""
import os
import socket


def _env(name, default, cast=str):
    v = os.environ.get(name)
    if v is None or v == "":
        return default
    if cast is bool:
        return v.strip().lower() in ("1", "true", "yes", "on")
    return cast(v)


# --- infrastructure -------------------------------------------------------
KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP", "localhost:9092")
PG_DSN = _env("PG_DSN", "host=localhost port=5433 dbname=tessera user=tessera password=tessera")
INSTANCE = _env("INSTANCE_NAME", socket.gethostname())

# --- topics (Kafka is the backbone: every hop between components is a topic) ---
T_RAW = "gps.raw"                    # data source -> fast plane (3 partitions per city; key = coarse H3 tile)
T_LATE = "gps.late"                  # side output: events later than allowed lateness
T_DLQ = "gps.dlq"                    # dead letters: malformed records rejected at the edge
T_AGG = "tile.aggregates"            # fast plane -> gate + sink (partition preserved from gps.raw)
T_SURPRISE = "surprise.events"       # gate -> slow plane (evidence bundles)
T_DECISIONS = "agent.decisions"      # slow plane / cache -> gate (cache + variogram) + sink
T_PLAN = "plan.patches"              # slow plane -> fast plane (compacted, broadcast)
T_ALERTS = "alerts"                  # any plane -> sink + dashboard
T_METRICS = "ops.metrics"            # telemetry from every component
T_TRUTH = "scenario.truth"           # ground truth of injected incidents (evaluation)
T_GOV = "governance.control"         # dashboard -> slow plane (human approvals, demo controls)

TOPICS = {
    # name: (partitions, config)
    T_RAW: (15, {"retention.ms": str(6 * 3600 * 1000)}),
    T_LATE: (3, {}),
    T_DLQ: (1, {}),
    T_AGG: (15, {}),
    T_SURPRISE: (3, {}),
    T_DECISIONS: (3, {}),
    T_PLAN: (1, {"cleanup.policy": "compact"}),
    T_ALERTS: (3, {}),
    T_METRICS: (3, {"retention.ms": str(6 * 3600 * 1000)}),
    T_TRUTH: (1, {}),
    T_GOV: (1, {}),
}

# --- consumer groups ------------------------------------------------------
G_FAST = "tessera-fastplane"         # scale out: docker compose up --scale fastplane=3
G_GATE = "tessera-gate"
G_SLOW = "tessera-slowplane"
G_SINK = "tessera-sink"

# --- spatial ----------------------------------------------------------------
TILE_RES = _env("TILE_RES", 8, int)          # analysis tessellation (edge ~0.53 km)
PARTITION_RES = _env("PARTITION_RES", 6, int)  # partition key resolution (spatial locality)
# --- scenario ----------------------------------------------------------------
# SOURCE_MODE = synthetic (default): simulated mixed traffic around the selected CITY
#               india              : five-city mode - Bengaluru traffic, optional Delhi OTD live feed,
#                                    Mobility Database schedule overlays and labelled simulated bus GPS for the other cities
#               delhi              : REAL live bus positions from Delhi Open Transit Data only
#               tdrive             : replay of the Microsoft T-Drive taxi traces (Beijing) - benchmark
SOURCE_MODE = _env("SOURCE_MODE", "synthetic").lower()
if SOURCE_MODE == "both":
    SOURCE_MODE = "india"
CITY_PROFILES = {
    # centre lat, centre lon, radius km, display name, time zone
    "bengaluru": (12.955, 77.610, 14.0, "Bengaluru", "Asia/Kolkata"),
    "delhi": (28.6139, 77.2090, 22.0, "Delhi", "Asia/Kolkata"),
    "chennai": (13.0827, 80.2707, 18.0, "Chennai", "Asia/Kolkata"),
    "hyderabad": (17.3850, 78.4867, 18.0, "Hyderabad", "Asia/Kolkata"),
    "hubli_dharwad": (15.3647, 75.1240, 12.0, "Hubballi–Dharwad", "Asia/Kolkata"),
    "beijing": (39.9042, 116.4074, 9.0, "Beijing (T-Drive)", "Asia/Shanghai"),
}
ACTIVE_CITIES = (["bengaluru", "delhi", "chennai", "hyderabad", "hubli_dharwad"]
                 if SOURCE_MODE == "india" else
                 {"delhi": ["delhi"], "tdrive": ["beijing"]}.get(
                     SOURCE_MODE, [_env("CITY", "bengaluru").lower()]))
CITY = ACTIVE_CITIES[0] if SOURCE_MODE == "india" else _env("CITY", ACTIVE_CITIES[0]).lower()
if CITY not in CITY_PROFILES or CITY not in ACTIVE_CITIES:
    CITY = ACTIVE_CITIES[0]
_c = CITY_PROFILES.get(CITY, CITY_PROFILES["bengaluru"])
CITY_LAT = _env("CITY_LAT", _c[0], float)
CITY_LON = _env("CITY_LON", _c[1], float)
CITY_RADIUS_KM = _env("CITY_RADIUS_KM", _c[2], float)
CITY_NAME = _c[3]
CITY_TZ = _c[4]


# Three Kafka partitions per city preserve city-level event-time isolation while allowing
# multiple workers to scale. Existing 6-partition topics are expanded by topic_init at startup.
CITY_PARTITIONS = ({city: list(range(i * 3, i * 3 + 3))
                    for i, city in enumerate(ACTIVE_CITIES)} if SOURCE_MODE == "india" else {})
# Real GPS fixes reach the open-data feed tens of seconds late and unevenly, so the live Delhi feed gets a
# wider out-of-orderness bound (watermark) and allowed lateness than the simulated feed (4 s / 3 s).
LIVE_WATERMARK_MS = _env("LIVE_WATERMARK_MS", 60000, int)
LIVE_LATENESS_MS = _env("LIVE_LATENESS_MS", 15000, int)
# Real BMTC buses (unofficial Namma BMTC API, the same API behind bmtckiosk.karnataka.gov.in) are added to the
# simulated Bengaluru traffic in india mode. Their fixes are ~30 s old like Delhi's, so the Bengaluru partitions
# then also use the live time policy.
BMTC_LIVE = _env("BMTC_LIVE", 1, int) == 1 and SOURCE_MODE == "india"
LIVE_PARTITIONS = (set(CITY_PARTITIONS.get("delhi", [])) |
                   (set(CITY_PARTITIONS.get("bengaluru", [])) if BMTC_LIVE else set())) \
    if SOURCE_MODE == "india" else (set(range(15)) if SOURCE_MODE == "delhi" else set())

def city_of(lat, lon):
    """Key of the active city whose service area contains the point (or the nearest one)."""
    import math  # noqa: PLC0415
    best = None
    for key in ACTIVE_CITIES:
        clat, clon, r, *_ = CITY_PROFILES[key]
        d = math.hypot(lat - clat, (lon - clon) * math.cos(math.radians(clat))) * 111.32
        if best is None or d / r < best[0]:
            best = (d / r, key)
    return best[1]


# --- producer ---------------------------------------------------------------
OTD_API_KEY = _env("OTD_API_KEY", "")         # Delhi Open Transit Data key (https://otd.delhi.gov.in/data/realtime/)
OTD_URL = _env("OTD_URL", "https://otd.delhi.gov.in/api/realtime/VehiclePositions.pb")
OTD_POLL_S = _env("OTD_POLL_S", 10.0, float)  # the feed refreshes about every 10 s
OTD_SPEED_UNIT = _env("OTD_SPEED_UNIT", "ms")
# Mobility Database catalog access. Put the long-lived refresh token in .env; never commit it.
MOBILITY_DATABASE_REFRESH_TOKEN = _env("MOBILITY_DATABASE_REFRESH_TOKEN", "")
MOBILITY_DATABASE_API_BASE = _env("MOBILITY_DATABASE_API_BASE", "https://api.mobilitydatabase.org/v1")
MOBILITY_FEED_IDS = {
    "chennai": _env("MDB_CHENNAI_FEED_ID", "mdb-3360"),
    "hyderabad": _env("MDB_HYDERABAD_FEED_ID", "mdb-3361"),
    "hubli_dharwad": _env("MDB_HUBLI_DHARWAD_FEED_ID", "mdb-3141"),
}
# Producer URLs published by Mobility Database; used as a resilient fallback if catalog auth is not configured.
MOBILITY_FEED_URLS = {
    "chennai": _env("MDB_CHENNAI_GTFS_URL", "https://raw.githubusercontent.com/ungalsoththu/ChennaiGTFS/main/data/chennai-unified-gtfs.zip"),
    "hyderabad": _env("MDB_HYDERABAD_GTFS_URL", "https://data.opencity.in/dataset/88e2d145-7ec6-4666-88dd-6cf18b18312e/resource/1b0d18bb-b2fb-4a79-8ed0-1e071da5790c/download/telangana_opendata_gtfs_tgsrtc_08_february_2026.zip"),
    "hubli_dharwad": _env("MDB_HUBLI_DHARWAD_GTFS_URL", "https://github.com/croyla/hdbrts-gtfs/raw/refs/heads/main/gtfs_compat.zip"),
}
MOBILITY_CACHE_DIR = _env("MOBILITY_CACHE_DIR", "/data/mobility")
MOBILITY_REFRESH_S = _env("MOBILITY_REFRESH_S", 86400, int)
NUM_CITY_SYNTHETIC_VEHICLES = _env("NUM_CITY_SYNTHETIC_VEHICLES", 90, int)

BLR_FEED_URL = _env("BLR_FEED_URL", "http://blr-feed:8090/VehiclePositions.pb")   # simulated BMTC GTFS-RT
BLR_FEED_PORT = _env("BLR_FEED_PORT", 8090, int)
BLR_VEHICLES = _env("BLR_VEHICLES", 4500, int)        # simulated Bengaluru fleet in india mode
BMTC_API = _env("BMTC_API", "https://bmtcmobileapi.karnataka.gov.in/WebAPI/")
BMTC_REQ_INTERVAL_S = _env("BMTC_REQ_INTERVAL_S", 1.2, float)   # one small request at a time: gentle on BMTC
BMTC_MAX_FIX_AGE_S = _env("BMTC_MAX_FIX_AGE_S", 150, int)      # older fixes would only be late events
# busy routes through Silk Board, ORR, Hosur Rd, Majestic, Hebbal, Marathahalli, KR Puram ... (unknown ones are skipped)
BMTC_ROUTES = [r.strip() for r in _env("BMTC_ROUTES", "500-D,500-DA,500-K,500-C,500-CA,500-A,501-D,KIA-9,KIA-5,"
                                       "KIA-8,KIA-6,KIA-4,335-E,201,G-4,356-CE,600-F,401-K,365,210-N,215-H,226,"
                                       "290-E,252,13,171,378,410-FA,25-A,45-G,V-500D,V-335E").split(",") if r.strip()]
BLR_REPORT_S = _env("BLR_REPORT_S", 6.0, float)       # each vehicle reports every 6 s (AIS-140 allows 5 s-10 min)   # GTFS-Realtime speed is metres/second; set kmh if the feed differs
TDRIVE_DIR = _env("TDRIVE_DIR", "/data/tdrive")
NUM_VEHICLES = _env("NUM_VEHICLES", 500, int)   # 500 vehicles x 1 report / 2 s = 250 events/s
REPORT_INTERVAL_S = _env("REPORT_INTERVAL_S", 2.0, float)
SPEEDUP = _env("SPEEDUP", 1.0, float)
INCIDENT_EVERY_S = _env("INCIDENT_EVERY_S", 150, float)
INCIDENT_DURATION_S = _env("INCIDENT_DURATION_S", 240, float)
OUT_OF_ORDER_FRAC = _env("OUT_OF_ORDER_FRAC", 0.05, float)
VERY_LATE_FRAC = _env("VERY_LATE_FRAC", 0.004, float)
MALFORMED_FRAC = _env("MALFORMED_FRAC", 0.002, float)

# --- fast plane ---------------------------------------------------------------
CHECKPOINT_INTERVAL_S = _env("CHECKPOINT_INTERVAL_S", 10.0, float)
IDLE_TIMEOUT_S = _env("IDLE_TIMEOUT_S", 15.0, float)

# --- gate -------------------------------------------------------------------------
FDR_Q = _env("FDR_Q", 0.01, float)
GATE_MIN_SLOW = _env("GATE_MIN_SLOW", 4.0, float)
# a hotspot needs this many distinct vehicles (real feeds: one stopped bus is not a traffic incident)
GATE_MIN_VEHICLES = _env("GATE_MIN_VEHICLES", 3 if SOURCE_MODE in ("delhi", "india") else 1, int)
TOKENS_PER_MIN = _env("TOKENS_PER_MIN", 6.0, float)
TOKEN_BURST = _env("TOKEN_BURST", 3, int)
CACHE_TTL_S = _env("CACHE_TTL_S", 120.0, float)
CACHE_COS = _env("CACHE_COS", 0.92, float)
VARIOGRAM_EPS = _env("VARIOGRAM_EPS", 0.02, float)

# --- slow plane -----------------------------------------------------------------
AGENT_LATENCY_S = _env("AGENT_LATENCY_S", 2.0, float)       # simulated reasoning latency
FAULT_INJECTION_RATE = _env("FAULT_INJECTION_RATE", 0.2, float)  # demo: agent sometimes emits a broken patch
SHADOW_SOAK_S = _env("SHADOW_SOAK_S", 20.0, float)
OLLAMA_URL = _env("OLLAMA_URL", "")       # optional free local LLM, e.g. http://host.docker.internal:11434
OLLAMA_MODEL = _env("OLLAMA_MODEL", "llama3.2")
DISTILL_MIN_EPISODES = _env("DISTILL_MIN_EPISODES", 8, int)
