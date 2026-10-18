"""Spatial primitives: hierarchical tessellation (H3), geodesic distance, edge normalisation."""
import math

import h3

EARTH_R_M = 6371008.8
SUPPORTED_CRS = {"EPSG:4326"}           # canonical CRS of the fast plane
METRIC_CRS = {"EPSG:3395", "EPSG:3857", "EPSG:32650", "EPSG:32643"}
_EDGE_CACHE = {}


def haversine_m(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_R_M * math.asin(min(1.0, math.sqrt(a)))


def tile(lat, lon, res):
    return h3.latlng_to_cell(lat, lon, res)


def parent(cell, res):
    return h3.cell_to_parent(cell, res)


def center(cell):
    return h3.cell_to_latlng(cell)


def ring(cell, k):
    return h3.grid_disk(cell, k)


def grid_distance(a, b):
    try:
        return h3.grid_distance(a, b)
    except Exception:  # noqa: BLE001  (cells too far apart / pentagon distortion)
        return 10 ** 6


def boundary(cell):
    return [[lat, lon] for lat, lon in h3.cell_to_boundary(cell)]


def cell_geometry_m(cell):
    """Measured local geometry of a cell: (circumradius, min centre distance to a neighbour).
    H3 cells are distorted (gnomonic projection), so bounds are computed from the real cell,
    not from the global average edge length."""
    if cell not in _EDGE_CACHE:
        lat, lon = h3.cell_to_latlng(cell)
        circ = max(haversine_m(lat, lon, a, b) for a, b in h3.cell_to_boundary(cell))
        dmin = min(haversine_m(lat, lon, *h3.cell_to_latlng(n)) for n in h3.grid_disk(cell, 1) if n != cell)
        _EDGE_CACHE[cell] = (circ, dmin)
    return _EDGE_CACHE[cell]


def max_circumradius_m(res, lat=None, lon=None):
    from . import config  # noqa: PLC0415
    cell = h3.latlng_to_cell(lat if lat is not None else config.CITY_LAT,
                             lon if lon is not None else config.CITY_LON, res)
    return cell_geometry_m(cell)[0] * 1.1


def candidate_ring_k(radius_m, res, lat=None, lon=None):
    """Smallest k such that grid_disk(cell, k) contains every cell holding a point within radius_m
    of any point of the origin cell (filter step with no false negatives, property P3).

    A target cell's centre is at most r + 2c from the origin centre (c = circumradius). On a hex
    lattice a cell at grid distance g has its centre at least g * d * sqrt(3)/2 away (d = minimum
    neighbour spacing), so k = ceil((r + 2c) / (d * sqrt(3)/2)) is sufficient. c and d are measured
    on the actual (distorted) H3 cells with safety margins."""
    from . import config  # noqa: PLC0415
    cell = h3.latlng_to_cell(lat if lat is not None else config.CITY_LAT,
                             lon if lon is not None else config.CITY_LON, res)
    circ, dmin = cell_geometry_m(cell)
    c, d = circ * 1.1, dmin * 0.9
    return max(1, math.ceil((radius_m + 2 * c) / (d * math.sqrt(3) / 2)))


def normalise(ev):
    """Edge validation + canonicalisation (L0). Returns (event, None) or (None, reason)."""
    try:
        crs = ev.get("crs", "EPSG:4326")
        axis = ev.get("axis", "lat,lon")
        lat, lon = float(ev["lat"]), float(ev["lon"])
        if crs not in SUPPORTED_CRS:
            return None, f"unsupported CRS {crs}"
        if axis == "lon,lat":            # declared axis order wins over assumption
            lat, lon = lon, lat
        if not (-90 <= lat <= 90 and -180 <= lon <= 180) or (lat == 0 and lon == 0):
            return None, "coordinates out of range"
        ts = int(ev["ts"])
        speed = float(ev.get("speed", 0.0))
        if speed < 0 or speed > 250 or math.isnan(speed):
            return None, "implausible speed"
        vid = str(ev["vid"])
    except (KeyError, TypeError, ValueError) as e:
        return None, f"malformed: {e}"
    return {"vid": vid, "ts": ts, "lat": lat, "lon": lon, "speed": speed,
            "heading": float(ev.get("heading", 0.0)), "ingest_ms": ev.get("ingest_ms")}, None
