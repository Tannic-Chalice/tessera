"""Spatio-Semantic Cache (SSC) and Geo-Episodic Memory (GEM)."""
import math
import time
import uuid
from collections import deque

from . import geo
from .db import jsonb


def cosine(a, b):
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return sum(x * y for x, y in zip(a, b)) / (na * nb)


def regime(ts_s):
    """Coarse context bucket that blocks reuse across structural breaks (time-of-day band)."""
    hour = time.localtime(ts_s).tm_hour
    return ["night", "morning", "afternoon", "evening"][hour // 6]


class SpatioSemanticCache:
    """Reuse rule (Eq. 3):  h(i,j) <= k*  AND  |t-s| <= tau  AND  cos(e_i,e_j) >= sigma  AND  regime equal.
    k* comes from the decision-field variogram, so the cache widens where the world is smooth
    and tightens to the same tile where it is rough."""

    def __init__(self, tau_s=180, sigma=0.92, maxlen=2000):
        self.tau, self.sigma = tau_s, sigma
        self.entries = deque(maxlen=maxlen)
        self.hits = self.misses = 0

    def insert(self, tile, t, vec, reg, decision):
        self.entries.append((tile, t, list(vec), reg, decision))

    def lookup(self, tile, t, vec, reg, k_star):
        for (tj, s, ej, rj, dec) in reversed(self.entries):
            if abs(t - s) > self.tau or rj != reg:
                continue
            if geo.grid_distance(tile, tj) > k_star:
                continue
            if cosine(vec, ej) < self.sigma:
                continue
            self.hits += 1
            return {"tile": tj, "t": s, "decision": dec, "similarity": round(cosine(vec, ej), 4)}
        self.misses += 1
        return None

    def stats(self):
        tot = self.hits + self.misses
        return {"hits": self.hits, "misses": self.misses, "hit_rate": round(self.hits / tot, 4) if tot else 0.0,
                "entries": len(self.entries)}


# Fact classes have declared half-lives (seconds): a congestion episode decays in minutes,
# a recurring hotspot regime in hours.
HALF_LIFE = {"congestion_incident": 300.0, "recurring_hotspot": 6 * 3600.0, "clear": 120.0}
CONF_FLOOR = 0.2


class GeoEpisodicMemory:
    def __init__(self, db):
        self.db = db

    def write(self, tiles, lat, lon, claim, fact_class, confidence, valid_s, provenance, plan_version):
        now = time.time()
        fid = "gem_" + uuid.uuid4().hex[:12]
        self.db.execute(
            """INSERT INTO gem_facts (fact_id, tiles, lat, lon, valid_from, valid_to, half_life_s, confidence0,
                                      fact_class, claim, provenance, plan_version)
               VALUES (%s,%s,%s,%s,to_timestamp(%s),to_timestamp(%s),%s,%s,%s,%s,%s,%s)""",
            (fid, list(tiles), lat, lon, now, now + valid_s, HALF_LIFE.get(fact_class, 600.0), confidence,
             fact_class, claim, jsonb(provenance), plan_version))
        return fid

    def retrieve(self, tiles, k=2, delta_s=3600, now=None):
        """Spatial + temporal filter FIRST (k-ring, validity interval), then read-time decay,
        then ranking. Facts below the confidence floor never reach the agent's context."""
        now = now or time.time()
        area = set()
        for t in tiles:
            area.update(geo.ring(t, k))
        rows = self.db.query(
            """SELECT fact_id, tiles, lat, lon, extract(epoch from valid_from) AS vf,
                      extract(epoch from valid_to) AS vt, half_life_s, confidence0, fact_class, claim, plan_version
               FROM gem_facts
               WHERE tiles && %s AND valid_from >= to_timestamp(%s) AND valid_from <= to_timestamp(%s)
               ORDER BY valid_from DESC LIMIT 50""",
            (list(area), now - delta_s, now))
        out, excluded = [], 0
        for r in rows:
            age = now - float(r["vf"])
            conf = float(r["confidence0"]) * 2 ** (-age / float(r["half_life_s"]))
            if now > float(r["vt"]) or conf < CONF_FLOOR:
                excluded += 1
                continue
            r["confidence"] = round(conf, 3)
            out.append(r)
        out.sort(key=lambda r: -r["confidence"])
        classes = {r["fact_class"] for r in out}
        conflict = "clear" in classes and "congestion_incident" in classes
        return out, excluded, conflict
