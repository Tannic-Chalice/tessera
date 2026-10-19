"""GS-IR: the Geo-Stream Intermediate Representation.

The only artefact that crosses the mutation boundary from the slow (agent) plane to the fast
(data) plane. A closed-vocabulary, typed, JSON-serialisable description of the continuous
spatial query, plus patches (diffs) that agents propose against it."""
import copy
import json
import math

from . import config, geo

# ---- closed vocabulary (an agent cannot invent near_ish()) -------------------
OPERATOR_KINDS = {"aggregate", "geofence", "distilled_filter"}
PREDICATES = {"dwithin", "within", "intersects", "disjoint", "contains", "overlaps", "touches"}
METRICS = {"geodesic", "planar"}
WINDOW_TYPES = {"sliding", "tumbling"}
INDEX_FAMILIES = {"H3"}
AGG_FNS = {"count", "distinct_vehicles", "mean_speed", "std_speed", "slow_frac"}
LATE_POLICIES = {"side_output", "drop"}
AXES = {"lat,lon", "lon,lat"}
UNITS = {"metre"}
PATCH_OPS = {"add_operator", "remove_operator", "set"}
DISTILL_FEATURES = {"slow_frac", "mean_speed_norm", "log_count", "vehicles_norm"}


def city_bbox(key=None):
    """Approved geographic scope of one city (the verifier rejects geofences outside every active city)."""
    if key is None:
        lat0, lon0, r = config.CITY_LAT, config.CITY_LON, config.CITY_RADIUS_KM
    else:
        lat0, lon0, r = config.CITY_PROFILES[key][:3]
    d_lat = r * 1.6 / 111.0
    d_lon = d_lat / max(0.2, abs(math.cos(math.radians(lat0))))
    return [round(lat0 - d_lat, 5), round(lon0 - d_lon, 5), round(lat0 + d_lat, 5), round(lon0 + d_lon, 5)]


def city_bboxes():
    return [city_bbox(k) for k in config.ACTIVE_CITIES]


def default_plan():
    """Version-1 plan: human-authored baseline (the 'intent' an operator approved)."""
    return {
        "name": "urban_mobility_monitor",
        "version": 1,
        "budget": {"p99_ms": 250, "max_tokens_per_min": 400, "on_violation": "degrade"},
        "source": {"topic": config.T_RAW, "format": "JSON", "crs": "EPSG:4326", "axis": "lat,lon",
                   "positional_accuracy_m": 15},
        "time": {"field": "ts", "semantics": "event", "watermark_ms": 4000,
                 "allowed_lateness_ms": 3000, "late_policy": "side_output"},
        "normalise": {"reproject": "EPSG:4326", "units": "metre"},
        "tile": {"index": "H3", "res": config.TILE_RES},
        "window": {"type": "sliding", "size_ms": 60000, "slide_ms": 10000,
                   "median_interarrival_ms": int(config.REPORT_INTERVAL_S * 1000)},
        "operators": [
            {"id": "tile_stats", "kind": "aggregate", "fns": sorted(AGG_FNS), "slow_kmh": 10.0,
             "provenance_id": "human:baseline"},
        ],
        "gate": {"statistic": "getis_ord_gstar", "neighbourhood": "k_ring(1)",
                 "control": "benjamini_hochberg", "fdr": config.FDR_Q},
        "emit": {"topic": config.T_ALERTS, "dedupe_cooldown_s": 90},
        "policy": {"bbox": city_bbox(), "bboxes": city_bboxes(), "max_geofences": 12, "max_geofence_radius_m": 3000,
                   "max_ttl_s": 1800, "actuation": False},
    }


def apply_patch(plan, patch):
    """Pure function: plan + patch -> new plan (version + 1). Raises ValueError on a malformed patch."""
    new = copy.deepcopy(plan)
    for op in patch.get("ops", []):
        kind = op.get("op")
        if kind not in PATCH_OPS:
            raise ValueError(f"unknown patch op {kind!r}")
        if kind == "add_operator":
            oper = copy.deepcopy(op["operator"])
            new["operators"] = [o for o in new["operators"] if o.get("id") != oper.get("id")] + [oper]
        elif kind == "remove_operator":
            new["operators"] = [o for o in new["operators"] if o.get("id") != op["id"]]
        elif kind == "set":
            node = new
            parts = op["path"].split(".")
            for p in parts[:-1]:
                node = node.setdefault(p, {})
            node[parts[-1]] = op["value"]
    new["version"] = int(plan.get("version", 1)) + 1
    return new


def operators(plan, kind):
    return [o for o in plan.get("operators", []) if o.get("kind") == kind]


def active_geofences(plan, now_ms):
    return [g for g in operators(plan, "geofence") if g.get("expires_ms", 1 << 62) > now_ms]


def render(plan):
    """Pretty-print a plan in the GS-IR surface syntax (for humans / the dashboard)."""
    L = []
    b, s, t, w = plan["budget"], plan["source"], plan["time"], plan["window"]
    L.append(f'plan "{plan["name"]}" v{plan["version"]}')
    L.append(f'budget {{ p99_ms: {b["p99_ms"]}, max_tokens_per_min: {b["max_tokens_per_min"]}, '
             f'on_violation: {b["on_violation"]} }}')
    L.append("")
    L.append(f'source {s["topic"]}')
    L.append(f'  format     {s["format"]}')
    L.append(f'  crs        {s["crs"]}  axis: {s["axis"]}      # declared, not assumed')
    L.append(f'  time       field={t["field"]} semantics={t["semantics"]} watermark=bounded({t["watermark_ms"]/1000:g}s)')
    L.append(f'  late       allowed={t["allowed_lateness_ms"]/1000:g}s policy={t["late_policy"]}')
    L.append(f'  tile       {plan["tile"]["index"]} res={plan["tile"]["res"]}')
    L.append(f'  assert     units={plan["normalise"]["units"]}')
    L.append("")
    L.append(f'window {w["type"]} size={w["size_ms"]/1000:g}s slide={w["slide_ms"]/1000:g}s')
    L.append(f'  guard size >= 3 * median_interarrival ({w["median_interarrival_ms"]/1000:g}s)')
    for o in plan["operators"]:
        L.append("")
        if o["kind"] == "aggregate":
            L.append(f'operator aggregate "{o["id"]}"')
            L.append(f'  fns        [{", ".join(o["fns"])}]   slow_kmh={o.get("slow_kmh")}')
        elif o["kind"] == "geofence":
            sh = o.get("shape", {})
            preds = o.get("predicate")
            preds = preds if isinstance(preds, str) else " AND ".join(preds or [])
            L.append(f'operator geofence "{o["id"]}"')
            L.append(f'  shape      circle({sh.get("lat")}, {sh.get("lon")}) r={sh.get("radius")} {sh.get("units")}'
                     f' crs={sh.get("crs")} axis={sh.get("axis")}')
            L.append(f'  predicate  {preds}({o.get("metric")})   refine {o.get("refine")}')
            L.append(f'  ttl        expires_ms={o.get("expires_ms")}  cooldown={o.get("cooldown_s")}s')
            L.append(f'  provenance {o.get("provenance_id")}')
        elif o["kind"] == "distilled_filter":
            m = o.get("model", {})
            L.append(f'operator distilled_filter "{o["id"]}"')
            L.append(f'  features   [{", ".join(o.get("features", []))}]')
            L.append(f'  model      {m.get("type")} w={[round(x, 3) for x in m.get("weights", [])]} b={round(m.get("bias", 0), 3)}')
            L.append(f'  threshold  {o.get("threshold")}   fallback escalate_to_gate in {o.get("uncertain_band")}')
            L.append(f'  provenance {o.get("provenance_id")}')
        else:
            L.append(f'operator {o.get("kind")} "{o.get("id")}"   # ??? not in vocabulary')
    g = plan["gate"]
    L.append("")
    L.append(f'gate surprise')
    L.append(f'  statistic  {g["statistic"]} neighbourhood={g["neighbourhood"]}')
    L.append(f'  control    {g["control"]} fdr={g["fdr"]}')
    L.append("")
    L.append(f'emit {plan["emit"]["topic"]}  dedupe cooldown={plan["emit"]["dedupe_cooldown_s"]}s')
    return "\n".join(L)


def to_json(plan):
    return json.dumps(plan, indent=2, sort_keys=False)


def geofence_operator(op_id, lat, lon, radius_m, expires_ms, provenance_id, res=None):
    """Well-formed geofence operator as an agent should emit it."""
    return {
        "id": op_id, "kind": "geofence",
        "shape": {"type": "circle", "lat": round(lat, 6), "lon": round(lon, 6), "radius": round(radius_m, 1),
                  "units": "metre", "crs": "EPSG:4326", "axis": "lat,lon"},
        "predicate": "dwithin", "metric": "geodesic", "refine": "exact_geometry",
        "candidate_k": geo.candidate_ring_k(radius_m, res or config.TILE_RES, lat, lon),
        "expires_ms": int(expires_ms), "cooldown_s": 120, "provenance_id": provenance_id,
    }
