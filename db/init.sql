-- TESSERA storage schema. Idempotent: safe to run many times.
-- Runs on TimescaleDB (hypertables + retention); falls back gracefully to plain PostgreSQL.

DO $$ BEGIN
  CREATE EXTENSION IF NOT EXISTS timescaledb;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'timescaledb extension not available - using plain PostgreSQL tables';
END $$;

-- Windowed per-tile aggregates produced by the fast plane (one row per tile per window).
CREATE TABLE IF NOT EXISTS tile_aggregates (
  window_end    TIMESTAMPTZ NOT NULL,
  window_start  TIMESTAMPTZ NOT NULL,
  tile          TEXT        NOT NULL,
  lat           DOUBLE PRECISION,
  lon           DOUBLE PRECISION,
  n_obs         INTEGER,
  vehicles      INTEGER,
  mean_speed    DOUBLE PRECISION,
  std_speed     DOUBLE PRECISION,
  slow_frac     DOUBLE PRECISION,
  late_dropped  INTEGER DEFAULT 0,
  plan_version  INTEGER,
  degradation   INTEGER DEFAULT 0,
  instance      TEXT,
  PRIMARY KEY (tile, window_end)
);

CREATE TABLE IF NOT EXISTS alerts (
  ts            TIMESTAMPTZ NOT NULL,
  alert_id      TEXT NOT NULL,
  type          TEXT,
  severity      TEXT,
  tile          TEXT,
  lat           DOUBLE PRECISION,
  lon           DOUBLE PRECISION,
  message       TEXT,
  payload       JSONB,
  plan_version  INTEGER,
  provenance_id TEXT,
  PRIMARY KEY (alert_id, ts)
);

CREATE TABLE IF NOT EXISTS surprise_events (
  ts            TIMESTAMPTZ NOT NULL,
  event_id      TEXT NOT NULL,
  window_end    TIMESTAMPTZ,
  tiles         TEXT[],
  max_z         DOUBLE PRECISION,
  lat           DOUBLE PRECISION,
  lon           DOUBLE PRECISION,
  payload       JSONB,
  PRIMARY KEY (event_id, ts)
);

CREATE TABLE IF NOT EXISTS agent_decisions (
  ts            TIMESTAMPTZ NOT NULL,
  decision_id   TEXT NOT NULL,
  tile          TEXT,
  lat           DOUBLE PRECISION,
  lon           DOUBLE PRECISION,
  label         TEXT,
  risk          DOUBLE PRECISION,
  source        TEXT,
  features      JSONB,
  episode_id    TEXT,
  PRIMARY KEY (decision_id, ts)
);

CREATE TABLE IF NOT EXISTS ops_metrics (
  ts            TIMESTAMPTZ NOT NULL,
  component     TEXT,
  instance      TEXT,
  name          TEXT,
  value         DOUBLE PRECISION,
  labels        JSONB
);
CREATE INDEX IF NOT EXISTS ops_metrics_name_ts ON ops_metrics (name, ts DESC);

-- Geo-Episodic Memory: georeferenced, time-bounded, confidence-decaying facts.
CREATE TABLE IF NOT EXISTS gem_facts (
  fact_id       TEXT PRIMARY KEY,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  tiles         TEXT[] NOT NULL,
  lat           DOUBLE PRECISION,
  lon           DOUBLE PRECISION,
  valid_from    TIMESTAMPTZ NOT NULL,
  valid_to      TIMESTAMPTZ NOT NULL,
  half_life_s   DOUBLE PRECISION NOT NULL,
  confidence0   DOUBLE PRECISION NOT NULL,
  fact_class    TEXT,
  claim         TEXT,
  provenance    JSONB,
  plan_version  INTEGER
);
CREATE INDEX IF NOT EXISTS gem_facts_tiles ON gem_facts USING GIN (tiles);

CREATE TABLE IF NOT EXISTS plan_versions (
  version       INTEGER NOT NULL,
  stage         TEXT NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  disposition   TEXT,
  plan          JSONB,
  diff          JSONB,
  verification  JSONB,
  provenance    JSONB,
  PRIMARY KEY (version, stage)
);

CREATE TABLE IF NOT EXISTS verifier_reports (
  report_id     TEXT PRIMARY KEY,
  ts            TIMESTAMPTZ NOT NULL DEFAULT now(),
  plan_version  INTEGER,
  passed        BOOLEAN,
  injected_fault TEXT,
  errors        JSONB
);

CREATE TABLE IF NOT EXISTS scenario_truth (
  incident_id   TEXT PRIMARY KEY,
  start_ts      TIMESTAMPTZ,
  end_ts        TIMESTAMPTZ,
  lat           DOUBLE PRECISION,
  lon           DOUBLE PRECISION,
  radius_m      DOUBLE PRECISION,
  place         TEXT,                 -- Bengaluru scenario: bottleneck where the incident was injected
  kind          TEXT                  -- e.g. waterlogging, breakdown, accident
);

-- Fault tolerance: operator state snapshots aligned with Kafka offsets (Flink-style checkpoints).
CREATE TABLE IF NOT EXISTS checkpoints (
  group_id      TEXT NOT NULL,
  topic         TEXT NOT NULL,
  partition     INTEGER NOT NULL,
  next_offset   BIGINT NOT NULL,
  state         JSONB,
  instance      TEXT,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (group_id, topic, partition)
);

-- Time-series features when TimescaleDB is present.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
    PERFORM create_hypertable('tile_aggregates', 'window_end', if_not_exists => TRUE, migrate_data => TRUE);
    PERFORM create_hypertable('alerts', 'ts', if_not_exists => TRUE, migrate_data => TRUE);
    PERFORM create_hypertable('surprise_events', 'ts', if_not_exists => TRUE, migrate_data => TRUE);
    PERFORM create_hypertable('agent_decisions', 'ts', if_not_exists => TRUE, migrate_data => TRUE);
    PERFORM create_hypertable('ops_metrics', 'ts', if_not_exists => TRUE, migrate_data => TRUE);
    BEGIN
      PERFORM add_retention_policy('ops_metrics', INTERVAL '3 days', if_not_exists => TRUE);
      PERFORM add_retention_policy('tile_aggregates', INTERVAL '14 days', if_not_exists => TRUE);
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'retention policy not added: %', SQLERRM;
    END;
  END IF;
END $$;

-- City-wide per-minute summary (works on both TimescaleDB and plain PostgreSQL).
CREATE OR REPLACE VIEW city_minute AS
SELECT date_trunc('minute', window_end) AS bucket,
       count(*)                          AS active_tiles,
       sum(n_obs)                        AS observations,
       avg(mean_speed)                   AS avg_speed,
       avg(slow_frac)                    AS avg_slow_frac
FROM tile_aggregates
GROUP BY 1;
