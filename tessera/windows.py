"""Event-time sliding windows with bounded-out-of-orderness watermarks and allowed lateness.

One WindowState per Kafka partition: because the partition key is a (coarse) tile, every event
of a given analysis tile lands in the same partition, so per-tile window state is never split
across instances - partition locality == spatial locality."""
import math
import time


class WindowState:
    __slots__ = ("size", "slide", "ooo", "lateness", "windows", "max_ts", "watermark",
                 "fired_upto", "last_event_wall", "last_idle_check", "late_total")

    def __init__(self, size_ms, slide_ms, ooo_ms, lateness_ms):
        self.size, self.slide = int(size_ms), int(slide_ms)
        self.ooo, self.lateness = int(ooo_ms), int(lateness_ms)
        self.windows = {}          # window_end -> tile -> accumulator
        self.max_ts = None
        self.watermark = None
        self.fired_upto = -1       # every window with end <= fired_upto has been emitted
        self.last_event_wall = time.time()
        self.last_idle_check = time.time()
        self.late_total = 0

    def reconfigure(self, size_ms, slide_ms, ooo_ms, lateness_ms):
        """Plan hot-swap: new windows use the new geometry; open windows finish unchanged."""
        self.size, self.slide = int(size_ms), int(slide_ms)
        self.ooo, self.lateness = int(ooo_ms), int(lateness_ms)

    def window_ends(self, ts):
        first = (ts // self.slide + 1) * self.slide
        return [first + i * self.slide for i in range(self.size // self.slide)]

    def add(self, ts, tile, speed, vid, slow_kmh):
        """Assign an event to every window that contains it. Returns False if it is too late for
        all of them (=> side output)."""
        added = False
        for end in self.window_ends(ts):
            if end <= self.fired_upto:
                continue
            tiles = self.windows.setdefault(end, {})
            acc = tiles.get(tile)
            if acc is None:
                acc = tiles[tile] = [0, 0.0, 0.0, 0, set()]
            acc[0] += 1
            acc[1] += speed
            acc[2] += speed * speed
            if speed < slow_kmh:
                acc[3] += 1
            acc[4].add(vid)
            added = True
        if self.max_ts is None or ts > self.max_ts:
            self.max_ts = ts
            wm = ts - self.ooo
            if self.watermark is None or wm > self.watermark:
                self.watermark = wm
        self.last_event_wall = time.time()
        if not added:
            self.late_total += 1
        return added

    def advance_idle(self, idle_timeout_s, now=None):
        """Idle-source handling: if a partition receives nothing, advance its watermark with
        processing time so its windows still close (otherwise one quiet partition stalls output)."""
        now = now or time.time()
        elapsed = now - self.last_idle_check
        self.last_idle_check = now
        if self.watermark is not None and now - self.last_event_wall > idle_timeout_s:
            self.watermark += int(elapsed * 1000)

    def fire(self):
        """Emit every window whose end + allowed lateness is behind the watermark."""
        if self.watermark is None:
            return []
        out = []
        for end in sorted(e for e in self.windows if e + self.lateness <= self.watermark):
            for tile, acc in self.windows.pop(end).items():
                out.append((end, tile, summarize(acc)))
            self.fired_upto = max(self.fired_upto, end)
        if self.watermark - self.lateness > self.fired_upto:
            # also mark empty windows as fired so stragglers for them are classified late
            self.fired_upto = max(self.fired_upto,
                                  ((self.watermark - self.lateness) // self.slide) * self.slide)
        return out

    # ---- checkpointing -----------------------------------------------------------
    def snapshot(self):
        return {
            "size": self.size, "slide": self.slide, "ooo": self.ooo, "lateness": self.lateness,
            "max_ts": self.max_ts, "watermark": self.watermark, "fired_upto": self.fired_upto,
            "late_total": self.late_total,
            "windows": {str(end): {t: [a[0], a[1], a[2], a[3], sorted(a[4])] for t, a in tiles.items()}
                        for end, tiles in self.windows.items()},
        }

    @classmethod
    def restore(cls, snap):
        ws = cls(snap["size"], snap["slide"], snap["ooo"], snap["lateness"])
        ws.max_ts, ws.watermark = snap["max_ts"], snap["watermark"]
        ws.fired_upto, ws.late_total = snap["fired_upto"], snap.get("late_total", 0)
        ws.windows = {int(end): {t: [a[0], a[1], a[2], a[3], set(a[4])] for t, a in tiles.items()}
                      for end, tiles in snap["windows"].items()}
        return ws


def summarize(acc):
    """Aggregation: count, distinct vehicles, mean and std of speed (moment sums), slow fraction."""
    n, s, ss, slow, vids = acc
    mean = s / n if n else 0.0
    var = max(0.0, ss / n - mean * mean) if n else 0.0
    return {"n_obs": n, "vehicles": len(vids), "mean_speed": round(mean, 3),
            "std_speed": round(math.sqrt(var), 3), "slow_frac": round(slow / n, 4) if n else 0.0}
