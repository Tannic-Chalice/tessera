"""Geo-Verifier, gate 1: static verification of a candidate GS-IR plan.

Every check is decidable from the plan alone (plus a small context of observed facts), so a
mechanical geo/temporal error is rejected *before* a single tuple flows through it."""
import copy
import random

from . import geo, gsir

CANONICAL_CRS = "EPSG:4326"
UNSAT_PAIRS = [  # DE-9IM: these predicates can never hold together for non-empty geometries
    ("disjoint", "within"), ("disjoint", "intersects"), ("disjoint", "contains"),
    ("disjoint", "overlaps"), ("disjoint", "touches"), ("disjoint", "dwithin"),
    ("within", "touches"), ("contains", "touches"),
]


def _err(errors, code, msg):
    errors.append({"code": code, "msg": msg})


def _in_bbox(lat, lon, bbox):
    return bbox[0] <= lat <= bbox[2] and bbox[1] <= lon <= bbox[3]


def verify(plan, ctx=None):
    """Return {"passed": bool, "errors": [...], "checks": n}. ctx: observed facts such as
    {"observed_ooo_ms": 3500, "observed_p99_ms": 12.0}."""
    ctx = ctx or {}
    errors = []
    checks = 0

    # 1. grammar / closed vocabulary --------------------------------------------------
    checks += 1
    for key in ("name", "version", "budget", "source", "time", "tile", "window", "operators", "gate", "emit", "policy"):
        if key not in plan:
            _err(errors, "GRAMMAR", f"missing required section '{key}'")
    if errors:
        return {"passed": False, "errors": errors, "checks": checks}
    ids = set()
    for o in plan["operators"]:
        if o.get("kind") not in gsir.OPERATOR_KINDS:
            _err(errors, "GRAMMAR", f"operator kind {o.get('kind')!r} is not in the GS-IR vocabulary")
        if not o.get("id"):
            _err(errors, "GRAMMAR", "operator without id")
        if o.get("id") in ids:
            _err(errors, "GRAMMAR", f"duplicate operator id {o.get('id')}")
        ids.add(o.get("id"))
    if plan["tile"].get("index") not in gsir.INDEX_FAMILIES:
        _err(errors, "GRAMMAR", f"index family {plan['tile'].get('index')!r} unknown")
    if plan["window"].get("type") not in gsir.WINDOW_TYPES:
        _err(errors, "GRAMMAR", f"window type {plan['window'].get('type')!r} unknown")

    src = plan["source"]
    res = int(plan["tile"].get("res", 8))
    bboxes = plan["policy"].get("bboxes") or [plan["policy"]["bbox"]]     # one scope per active city
    accuracy = float(src.get("positional_accuracy_m", 10))

    # 2. CRS / axis order of the source (mandatory, no defaults) -------------------------
    checks += 1
    if not src.get("crs"):
        _err(errors, "CRS", "source CRS not declared")
    elif src["crs"] not in geo.SUPPORTED_CRS:
        _err(errors, "CRS", f"source CRS {src['crs']} has no transformation to canonical {CANONICAL_CRS}")
    if src.get("axis") not in gsir.AXES:
        _err(errors, "AXIS", f"source axis order {src.get('axis')!r} not declared")
    if plan.get("normalise", {}).get("units") not in gsir.UNITS:
        _err(errors, "UNITS", "normalise.units must be 'metre'")

    # 3. window / watermark / lateness well-formedness ----------------------------------
    checks += 1
    w, t = plan["window"], plan["time"]
    size, slide = int(w.get("size_ms", 0)), int(w.get("slide_ms", 0))
    if slide <= 0 or size <= 0:
        _err(errors, "WINDOW", "window size and slide must be positive")
    elif slide > size:
        _err(errors, "WINDOW", f"slide ({slide} ms) exceeds window size ({size} ms)")
    elif size % slide != 0:
        _err(errors, "WINDOW", "window size must be a multiple of the slide")
    if size < 3 * int(w.get("median_interarrival_ms", 0)):
        _err(errors, "WINDOW", "window shorter than 3x median inter-arrival (statistically inadequate)")
    wm, lat_ms = int(t.get("watermark_ms", -1)), int(t.get("allowed_lateness_ms", -1))
    if wm < 0 or lat_ms < 0:
        _err(errors, "WATERMARK", "watermark bound and allowed lateness must be >= 0")
    if t.get("late_policy") not in gsir.LATE_POLICIES:
        _err(errors, "WATERMARK", f"late policy {t.get('late_policy')!r} unknown")
    if wm + max(lat_ms, 0) < 0.5 * int(w.get("median_interarrival_ms", 0)):
        _err(errors, "WATERMARK", "watermark+lateness shorter than half the sensor reporting interval")
    obs = ctx.get("observed_ooo_ms")
    if obs is not None and wm + max(lat_ms, 0) < 0.5 * obs:
        _err(errors, "WATERMARK", f"watermark+lateness ({wm + lat_ms} ms) far below observed out-of-orderness ({obs} ms)")

    # 4. per-operator geometric checks -------------------------------------------------------
    geofences = [o for o in plan["operators"] if o.get("kind") == "geofence"]
    distilled = [o for o in plan["operators"] if o.get("kind") == "distilled_filter"]
    for g in geofences:
        checks += 5
        sh = g.get("shape") or {}
        gid = g.get("id")
        if not sh.get("crs"):
            _err(errors, "CRS", f"{gid}: geometry CRS missing (no default is assumed)")
        elif sh["crs"] != CANONICAL_CRS:
            _err(errors, "CRS", f"{gid}: geometry in {sh['crs']} but no datum/transform to {CANONICAL_CRS} declared")
        if sh.get("axis") not in gsir.AXES:
            _err(errors, "AXIS", f"{gid}: axis order not declared")
        if sh.get("units") not in gsir.UNITS:
            _err(errors, "UNITS", f"{gid}: radius in {sh.get('units')!r}; distance thresholds must be metric")
        try:
            lat, lon, r = float(sh["lat"]), float(sh["lon"]), float(sh["radius"])
        except (KeyError, TypeError, ValueError):
            _err(errors, "GRAMMAR", f"{gid}: circle needs numeric lat, lon, radius")
            continue
        if sh.get("axis") == "lon,lat":
            lat, lon = lon, lat
        if not any(_in_bbox(lat, lon, b) for b in bboxes):
            if any(_in_bbox(lon, lat, b) for b in bboxes):
                _err(errors, "AXIS", f"{gid}: centre outside scope but inside if axes swapped - lat/lon order error")
            else:
                _err(errors, "POLICY", f"{gid}: centre outside the approved geographic scope")
        if r < accuracy:
            _err(errors, "PRECISION", f"{gid}: radius {r} m finer than source positional accuracy {accuracy} m"
                 + (" (looks like angular units)" if r < 1 else ""))
        if r > plan["policy"].get("max_geofence_radius_m", 1e9):
            _err(errors, "POLICY", f"{gid}: radius {r} m exceeds policy maximum")
        if -90 <= lat <= 90 and -180 <= lon <= 180:     # index checks only make sense for a valid centre
            k = geo.candidate_ring_k(r, res, lat, lon)
            if k > 12:
                _err(errors, "INDEX", f"{gid}: radius {r} m needs k={k} rings at res {res} (enumeration dominates; choose coarser res)")
            if r < 0.25 * geo.max_circumradius_m(res, lat, lon):
                _err(errors, "INDEX", f"{gid}: res {res} too coarse for radius {r} m (filter degenerates)")
            if g.get("candidate_k") is not None and int(g["candidate_k"]) < k:
                _err(errors, "INDEX", f"{gid}: declared candidate ring k={g['candidate_k']} < required {k} (false negatives)")
        preds = g.get("predicate")
        preds = [preds] if isinstance(preds, str) else list(preds or [])
        for p in preds:
            if p not in gsir.PREDICATES:
                _err(errors, "GRAMMAR", f"{gid}: predicate {p!r} not in vocabulary")
        ps = set(preds)
        for a, b in UNSAT_PAIRS:
            if a in ps and b in ps:
                _err(errors, "TOPOLOGY", f"{gid}: '{a}' AND '{b}' is unsatisfiable under DE-9IM")
        if g.get("metric") not in gsir.METRICS:
            _err(errors, "UNITS", f"{gid}: distance metric must be declared (geodesic|planar)")
        if "expires_ms" not in g:
            _err(errors, "POLICY", f"{gid}: temporary geofence without expiry")

    for d in distilled:
        checks += 2
        did = d.get("id")
        feats = d.get("features", [])
        bad = [f for f in feats if f not in gsir.DISTILL_FEATURES]
        if bad:
            _err(errors, "GRAMMAR", f"{did}: unknown features {bad}")
        if len(d.get("model", {}).get("weights", [])) != len(feats):
            _err(errors, "GRAMMAR", f"{did}: weight vector length does not match features")
        thr = d.get("threshold")
        if not isinstance(thr, (int, float)) or not 0 < thr < 1:
            _err(errors, "GRAMMAR", f"{did}: threshold must be in (0,1)")
        if not d.get("provenance_id"):
            _err(errors, "PROVENANCE", f"{did}: distilled operator without provenance")

    # 5. policy envelope and budget feasibility ----------------------------------------------
    checks += 2
    if len(geofences) > plan["policy"].get("max_geofences", 1e9):
        _err(errors, "POLICY", f"{len(geofences)} geofences exceed the policy maximum")
    est = float(ctx.get("observed_p99_ms", 5.0)) + 0.05 * len(geofences) + 0.02 * len(distilled)
    if est > float(plan["budget"].get("p99_ms", 0)):
        _err(errors, "BUDGET", f"estimated p99 {est:.1f} ms exceeds declared budget {plan['budget'].get('p99_ms')} ms")
    if plan["policy"].get("actuation"):
        _err(errors, "POLICY", "actuation-enabling plans cannot be auto-verified")

    return {"passed": not errors, "errors": errors, "checks": checks}


# ---------------------------------------------------------------------------------------
# Fault-injection suite (Section 8.5): deliberately malformed patches the verifier must catch.
# Each takes a well-formed geofence operator + plan and returns a broken (plan, label).

def _gf(plan):
    for o in reversed(plan["operators"]):      # the most recently added geofence
        if o.get("kind") == "geofence":
            return o
    raise ValueError("no geofence to corrupt")


def f_swap_axis(p):
    g = _gf(p)["shape"]
    g["lat"], g["lon"] = g["lon"], g["lat"]


def f_degree_units(p):
    g = _gf(p)["shape"]
    g["radius"] = round(g["radius"] / 111320.0, 5)
    g["units"] = "degree"


def f_missing_crs(p):
    _gf(p)["shape"].pop("crs", None)


def f_foreign_crs(p):
    _gf(p)["shape"]["crs"] = "EPSG:32650"


def f_index_radius(p):
    _gf(p)["shape"]["radius"] = 2900.0
    p["tile"]["res"] = 11


def f_slide_gt_size(p):
    p["window"]["slide_ms"] = p["window"]["size_ms"] * 2


def f_watermark(p):
    p["time"]["watermark_ms"] = 50
    p["time"]["allowed_lateness_ms"] = 0


def f_unsat_topology(p):
    _gf(p)["predicate"] = ["disjoint", "within"]


def f_precision(p):
    _gf(p)["shape"]["radius"] = 3.0


def f_budget(p):
    p["budget"]["p99_ms"] = 1


def f_invented_operator(p):
    p["operators"].append({"id": "x_near", "kind": "near_ish", "provenance_id": "agent"})


FAULTS = {
    "swapped_axis_order": f_swap_axis,
    "distance_in_degrees": f_degree_units,
    "missing_crs": f_missing_crs,
    "missing_datum_transform": f_foreign_crs,
    "index_radius_incompatible": f_index_radius,
    "slide_greater_than_size": f_slide_gt_size,
    "watermark_below_out_of_orderness": f_watermark,
    "de9im_unsatisfiable": f_unsat_topology,
    "precision_exceeds_accuracy": f_precision,
    "latency_budget_exceeded": f_budget,
    "invented_operator": f_invented_operator,
}


def inject_fault(plan, name=None, rng=random):
    name = name or rng.choice(sorted(FAULTS))
    broken = copy.deepcopy(plan)
    FAULTS[name](broken)
    return broken, name


def run_fault_suite(plan, trials_per_fault=20, ctx=None, seed=7):
    """Benchmark H5: catch rate per fault class and false-rejection rate on valid plans."""
    rng = random.Random(seed)
    ctx = ctx or {"observed_ooo_ms": 3500, "observed_p99_ms": 10.0}
    from . import config  # noqa: PLC0415
    report = {}
    for name in sorted(FAULTS):
        caught = 0
        for _ in range(trials_per_fault):
            base = _random_valid_plan(plan, rng, config)
            broken, _ = inject_fault(base, name, rng)
            if not verify(broken, ctx)["passed"]:
                caught += 1
        report[name] = caught / trials_per_fault
    valid_rejected = 0
    n_valid = trials_per_fault * 5
    for _ in range(n_valid):
        if not verify(_random_valid_plan(plan, rng, config), ctx)["passed"]:
            valid_rejected += 1
    return {"catch_rate": report,
            "overall_catch_rate": sum(report.values()) / len(report),
            "false_rejection_rate": valid_rejected / n_valid}


def _random_valid_plan(plan, rng, config):
    b = plan["policy"]["bbox"]
    lat = rng.uniform(b[0] + 0.02, b[2] - 0.02)
    lon = rng.uniform(b[1] + 0.02, b[3] - 0.02)
    r = rng.uniform(300, 2500)
    op = gsir.geofence_operator(f"gf_{rng.randint(0, 1 << 30):x}", lat, lon, r, 1 << 50, "test:episode")
    return gsir.apply_patch(plan, {"ops": [{"op": "add_operator", "operator": op}]})


def ring_count(radius_m, res):
    k = geo.candidate_ring_k(radius_m, res)
    return 3 * k * (k + 1) + 1, k

