"""Statistics for the surprise gate, the cache and the distiller.

* Getis-Ord local G*_i (Eq. 1) over the tessellation's k-ring spatial weights
* Benjamini-Hochberg false-discovery-rate control across the active tile population (Eq. 2)
* Empirical semivariogram of the agent's decision field and reuse radius k* (Eqs. 4-5)
* A tiny logistic-regression distiller (judgement -> cheap deterministic operator)"""
import math
import time
from collections import deque

import numpy as np

from . import geo


class EWMA:
    """Exponentially weighted mean/variance so the gate's baseline adapts to drift."""

    def __init__(self, alpha=0.2):
        self.alpha, self.mean, self.var, self.n = alpha, None, None, 0

    def update(self, mean, var):
        if self.mean is None:
            self.mean, self.var = mean, var
        else:
            a = self.alpha
            self.mean = (1 - a) * self.mean + a * mean
            self.var = (1 - a) * self.var + a * var
        self.n += 1

    @property
    def sd(self):
        return math.sqrt(self.var) if self.var else 0.0


def gstar(values, active, baseline=None, k=1):
    """values: tile -> x (only tiles with data); active: set of tiles (data tiles + neighbours).
    Returns tile -> z for every tile that has data. Weights are binary over k_ring(i, k), i included."""
    n = len(active)
    if n < 3:
        return {}
    xs = [values.get(t, 0.0) for t in active]
    mean_now = sum(xs) / n
    var_now = sum((x - mean_now) ** 2 for x in xs) / n
    if baseline is not None and baseline.n >= 3:
        # blend: robust to a single window where the whole city shifts at once
        xbar = 0.5 * baseline.mean + 0.5 * mean_now
        s = math.sqrt(0.5 * baseline.var + 0.5 * var_now)
    else:
        xbar, s = mean_now, math.sqrt(var_now)
    if baseline is not None:
        baseline.update(mean_now, var_now)
    if s <= 1e-9:
        return {t: 0.0 for t in values}
    z = {}
    for i in values:
        neigh = [j for j in geo.ring(i, k) if j in active]
        w = len(neigh)
        sx = sum(values.get(j, 0.0) for j in neigh)
        den = s * math.sqrt(max(1e-12, (n * w - w * w) / (n - 1)))
        z[i] = (sx - xbar * w) / den
    return z


def p_two_sided(z):
    return math.erfc(abs(z) / math.sqrt(2))


def benjamini_hochberg(pvals, q):
    """pvals: dict key -> p. Returns the set of keys rejected at FDR level q."""
    items = sorted(pvals.items(), key=lambda kv: kv[1])
    m = len(items)
    k_max = 0
    for rank, (_, p) in enumerate(items, start=1):
        if p <= rank * q / m:
            k_max = rank
    return {key for key, _ in items[:k_max]}


def clusters(tiles):
    """Connected components of a tile set under grid adjacency (one evidence bundle per hotspot)."""
    tiles = set(tiles)
    seen, out = set(), []
    for t in tiles:
        if t in seen:
            continue
        comp, stack = [], [t]
        seen.add(t)
        while stack:
            c = stack.pop()
            comp.append(c)
            for nb in geo.ring(c, 1):
                if nb in tiles and nb not in seen:
                    seen.add(nb)
                    stack.append(nb)
        out.append(comp)
    return out


class DecisionVariogram:
    """Online empirical semivariogram of the agent's own decision field:
        gamma(h) = 1/(2|N(h)|) * sum_{(i,j) in N(h)} (d_i - d_j)^2
    over decision pairs at grid distance h recorded within tau seconds of each other.
    The reuse radius is k* = max{h : gamma(h') <= eps for all h' <= h}."""

    def __init__(self, max_lag=6, tau_s=600, eps=0.02, min_pairs=3, maxlen=400):
        self.max_lag, self.tau, self.eps, self.min_pairs = max_lag, tau_s, eps, min_pairs
        self.decisions = deque(maxlen=maxlen)   # (tile, t, d)
        self.gamma, self.pairs, self.k_star = {}, {}, 0

    def add(self, tile, t, d):
        self.decisions.append((tile, t, float(d)))
        self.recompute()

    def recompute(self):
        sums = {h: 0.0 for h in range(1, self.max_lag + 1)}
        cnt = {h: 0 for h in range(1, self.max_lag + 1)}
        dec = list(self.decisions)
        for a in range(len(dec)):
            ti, si, di = dec[a]
            for b in range(a + 1, len(dec)):
                tj, sj, dj = dec[b]
                if abs(si - sj) > self.tau or ti == tj:
                    continue
                h = geo.grid_distance(ti, tj)
                if 1 <= h <= self.max_lag:
                    sums[h] += (di - dj) ** 2
                    cnt[h] += 1
        self.gamma = {h: (sums[h] / (2 * cnt[h]) if cnt[h] else None) for h in sums}
        self.pairs = cnt
        k = 0
        for h in range(1, self.max_lag + 1):
            g = self.gamma[h]
            if g is None or cnt[h] < self.min_pairs:
                break
            if g <= self.eps:
                k = h
            else:
                break
        self.k_star = k
        return k

    def state(self):
        return {"k_star": self.k_star, "eps": self.eps,
                "gamma": {str(h): (round(g, 5) if g is not None else None) for h, g in self.gamma.items()},
                "pairs": {str(h): c for h, c in self.pairs.items()}}


class TokenBucket:
    def __init__(self, per_min, burst):
        self.rate, self.cap = per_min / 60.0, float(burst)
        self.tokens, self.t = float(burst), time.time()

    def try_acquire(self):
        now = time.time()
        self.tokens = min(self.cap, self.tokens + (now - self.t) * self.rate)
        self.t = now
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        return False


# ---- distiller ---------------------------------------------------------------------------

def features(agg):
    """Named, auditable features (never raw embeddings)."""
    return [float(agg.get("slow_frac", 0.0)),
            float(agg.get("mean_speed", 0.0)) / 60.0,
            math.log1p(float(agg.get("n_obs", 0))) / 5.0,
            float(agg.get("vehicles", 0)) / 30.0]


FEATURE_NAMES = ["slow_frac", "mean_speed_norm", "log_count", "vehicles_norm"]


def sigmoid(x):
    return 1.0 / (1.0 + np.exp(-x))


def fit_logistic(X, y, l2=0.01, iters=2000, lr=0.5):
    X, y = np.asarray(X, float), np.asarray(y, float)
    w, b = np.zeros(X.shape[1]), 0.0
    for _ in range(iters):
        p = sigmoid(X @ w + b)
        g = p - y
        w -= lr * (X.T @ g / len(y) + l2 * w)
        b -= lr * g.mean()
    return w.tolist(), float(b)


def calibrate_threshold(X, y, w, b):
    s = sigmoid(np.asarray(X, float) @ np.asarray(w) + b)
    best, best_f1 = 0.5, -1
    for thr in np.linspace(0.5, 0.95, 19):
        pred = s >= thr
        tp = float(np.sum(pred & (np.asarray(y) == 1)))
        fp = float(np.sum(pred & (np.asarray(y) == 0)))
        fn = float(np.sum(~pred & (np.asarray(y) == 1)))
        f1 = 2 * tp / (2 * tp + fp + fn) if tp else 0.0
        if f1 > best_f1:
            best, best_f1 = float(thr), f1
    return round(best, 3), best_f1


def score(model, feats):
    return float(sigmoid(np.dot(model["weights"], feats) + model["bias"]))
