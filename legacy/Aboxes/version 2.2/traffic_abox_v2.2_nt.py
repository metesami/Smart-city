# pip install duckdb

import duckdb
import json
import gzip
import math
import urllib.parse
import datetime as dt

# -----------------------------
# Config
# -----------------------------
IN_PARQUET = "/run/determined/workdir/traffic/intersection_imputed_layered_10min_analytics.parquet"
OUT_NT_GZ  = "/run/determined/workdir/traffic/A142_traffic_10min_abox.nt.gz"

SENSOR_MAP_JSON  = "/run/determined/workdir/traffic/sensor_uri_map.json"
SENSOR2LANE_JSON = "/run/determined/workdir/traffic/sensor_to_lane_map.json"

BATCH = 100_000   # tune based on memory/CPU
WINDOW_SEC = 600

# -----------------------------
# Namespaces / URIs
# -----------------------------
NS_EX      = "http://example.org/traffic/"
NS_SC      = "http://example.org/smartcity/core#"
NS_TRAFFIC = "http://example.org/smartcity/traffic#"
NS_SOSA    = "http://www.w3.org/ns/sosa/"
NS_TIME    = "http://www.w3.org/2006/time#"
NS_SCTIME  = "http://example.org/smartcity/time/"
NS_XSD     = "http://www.w3.org/2001/XMLSchema#"

URI_RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"

URI_SOSA_OBSERVATION          = f"<{NS_SOSA}Observation>"
URI_SOSA_MADE_BY_SENSOR       = f"<{NS_SOSA}madeBySensor>"
URI_SOSA_OBSERVED_PROPERTY    = f"<{NS_SOSA}observedProperty>"
URI_SOSA_HAS_SIMPLE_RESULT    = f"<{NS_SOSA}hasSimpleResult>"
URI_SOSA_PHENOMENON_TIME      = f"<{NS_SOSA}phenomenonTime>"
URI_SOSA_HAS_FEATURE_INTEREST = f"<{NS_SOSA}hasFeatureOfInterest>"

URI_TIME_INSTANT              = f"<{NS_TIME}Instant>"
URI_TIME_IN_XSD_DATETIME      = f"<{NS_TIME}inXSDDateTime>"

URI_TRAFFIC_OBSERVATION       = f"<{NS_TRAFFIC}TrafficObservation>"
URI_TRAFFIC_VEHICLE_COUNT     = f"<{NS_TRAFFIC}VehicleCount>"
URI_TRAFFIC_OCCUPANCY_TIME    = f"<{NS_TRAFFIC}OccupancyTime>"
URI_TRAFFIC_CONF_MIN          = f"<{NS_TRAFFIC}confidenceMin>"
URI_TRAFFIC_CONF_MEAN         = f"<{NS_TRAFFIC}confidenceMean>"
URI_TRAFFIC_COVERAGE_COUNT    = f"<{NS_TRAFFIC}coverageCount>"
URI_TRAFFIC_COVERAGE_OCC      = f"<{NS_TRAFFIC}coverageOccupancy>"
URI_TRAFFIC_IMPUTED_RATE      = f"<{NS_TRAFFIC}imputedRate>"
URI_TRAFFIC_CLEAN_OBS_RATE    = f"<{NS_TRAFFIC}cleanObservedRate>"

URI_SC_OBSERVED_AT_TIMEINDEX  = f"<{NS_SC}observedAtTimeIndex>"
URI_SC_AGG_WINDOW_SECONDS     = f"<{NS_SC}aggregationWindowSeconds>"

# -----------------------------
# Helpers
# -----------------------------
def u(uri: str) -> str:
    return f"<{uri}>"

def lit_double(x) -> str:
    return f"\"{float(x)}\"^^<{NS_XSD}double>"

def lit_long(x) -> str:
    return f"\"{int(x)}\"^^<{NS_XSD}long>"

def lit_int(x) -> str:
    return f"\"{int(x)}\"^^<{NS_XSD}integer>"

def lit_datetime(x: str) -> str:
    return f"\"{x}\"^^<{NS_XSD}dateTime>"

def triple(s: str, p: str, o: str) -> str:
    return f"{s} {p} {o} .\n"

def safe_local(text: str) -> str:
    return urllib.parse.quote(str(text).strip(), safe="")

def normalize_ts(ts):
    """
    Convert DuckDB timestamp result to timezone-aware UTC datetime.
    """
    if ts is None:
        return None

    if isinstance(ts, dt.datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=dt.timezone.utc)
        return ts.astimezone(dt.timezone.utc)

    # fallback for string-like timestamps
    ts2 = dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    if ts2.tzinfo is None:
        ts2 = ts2.replace(tzinfo=dt.timezone.utc)
    else:
        ts2 = ts2.astimezone(dt.timezone.utc)
    return ts2

# -----------------------------
# Load maps
# -----------------------------
with open(SENSOR_MAP_JSON, "r", encoding="utf-8") as f:
    sensor_uri_map = json.load(f)

try:
    with open(SENSOR2LANE_JSON, "r", encoding="utf-8") as f:
        sensor_to_lane_map = json.load(f)
except Exception:
    sensor_to_lane_map = {}

print("Loaded maps:")
print("  sensors     :", len(sensor_uri_map))
print("  sensor→lane :", len(sensor_to_lane_map))

# -----------------------------
# DuckDB
# -----------------------------
con = duckdb.connect(database=":memory:")
con.execute("PRAGMA threads=8;")
con.execute("PRAGMA enable_progress_bar;")

# -----------------------------
# Write RDF stream directly
# -----------------------------
total_obs = 0
total_time_instants = 0

with gzip.open(OUT_NT_GZ, "wt", encoding="utf-8") as fout:
    # -----------------------------------------
    # Part 1: write unique time instants once
    # -----------------------------------------
    q_time = f"""
    SELECT DISTINCT timestamp
    FROM '{IN_PARQUET}'
    WHERE timestamp IS NOT NULL
    ORDER BY timestamp
    """
    cur_time = con.execute(q_time)

    while True:
        rows = cur_time.fetchmany(BATCH)
        if not rows:
            break

        buf = []

        for (ts,) in rows:
            ts = normalize_ts(ts)
            if ts is None:
                continue

            t_idx = int(ts.timestamp())
            iso_t = ts.isoformat()

            t_inst = u(f"{NS_SCTIME}t_{t_idx}")

            buf.append(triple(t_inst, URI_RDF_TYPE, URI_TIME_INSTANT))
            buf.append(triple(t_inst, URI_TIME_IN_XSD_DATETIME, lit_datetime(iso_t)))
            total_time_instants += 1

        fout.write("".join(buf))
        print(f"[time] written instants so far: {total_time_instants}")

    # -----------------------------------------
    # Part 2: write observations
    # -----------------------------------------
    q_obs = f"""
    SELECT
      sensor_id,
      timestamp,
      count_10min,
      occupancy_time_10min,
      confidence_min,
      confidence_mean,
      coverage_count,
      coverage_dwell,
      imputed_rate,
      is_clean_observed_rate
    FROM '{IN_PARQUET}'
    WHERE sensor_id IS NOT NULL
      AND timestamp IS NOT NULL
      AND (count_10min IS NOT NULL OR occupancy_time_10min IS NOT NULL)
    ORDER BY timestamp, sensor_id
    """

    cur_obs = con.execute(q_obs)

    while True:
        rows = cur_obs.fetchmany(BATCH)
        if not rows:
            break

        buf = []

        for (
            sid,
            ts,
            count_10min,
            occ_time,
            conf_min,
            conf_mean,
            cov_c,
            cov_o,
            imputed_rate,
            obs_rate
        ) in rows:

            sid = str(sid).strip()
            sensor_uri_str = sensor_uri_map.get(sid)
            if not sensor_uri_str:
                continue

            sensor_uri = u(sensor_uri_str)

            lane_uri_str = sensor_to_lane_map.get(sid)
            lane_uri = u(lane_uri_str) if lane_uri_str else None

            ts = normalize_ts(ts)
            if ts is None:
                continue

            t_idx = int(ts.timestamp())
            t_inst = u(f"{NS_SCTIME}t_{t_idx}")
            sid_safe = safe_local(sid)

            # ---------------------------
            # VehicleCount observation
            # ---------------------------
            if count_10min is not None and not (isinstance(count_10min, float) and math.isnan(count_10min)):
                obs_count = u(f"{NS_EX}obsCount_{sid_safe}_{t_idx}")

                buf.append(triple(obs_count, URI_RDF_TYPE, URI_SOSA_OBSERVATION))
                buf.append(triple(obs_count, URI_RDF_TYPE, URI_TRAFFIC_OBSERVATION))
                buf.append(triple(obs_count, URI_SOSA_MADE_BY_SENSOR, sensor_uri))
                buf.append(triple(obs_count, URI_SOSA_OBSERVED_PROPERTY, URI_TRAFFIC_VEHICLE_COUNT))
                buf.append(triple(obs_count, URI_SOSA_HAS_SIMPLE_RESULT, lit_double(count_10min)))
                buf.append(triple(obs_count, URI_SOSA_PHENOMENON_TIME, t_inst))
                buf.append(triple(obs_count, URI_SC_OBSERVED_AT_TIMEINDEX, lit_long(t_idx)))
                buf.append(triple(obs_count, URI_SC_AGG_WINDOW_SECONDS, lit_int(WINDOW_SEC)))

                if lane_uri:
                    buf.append(triple(obs_count, URI_SOSA_HAS_FEATURE_INTEREST, lane_uri))
                if conf_min is not None and not (isinstance(conf_min, float) and math.isnan(conf_min)):
                    buf.append(triple(obs_count, URI_TRAFFIC_CONF_MIN, lit_double(conf_min)))
                if conf_mean is not None and not (isinstance(conf_mean, float) and math.isnan(conf_mean)):
                    buf.append(triple(obs_count, URI_TRAFFIC_CONF_MEAN, lit_double(conf_mean)))
                if cov_c is not None and not (isinstance(cov_c, float) and math.isnan(cov_c)):
                    buf.append(triple(obs_count, URI_TRAFFIC_COVERAGE_COUNT, lit_double(cov_c)))
                if imputed_rate is not None and not (isinstance(imputed_rate, float) and math.isnan(imputed_rate)):
                    buf.append(triple(obs_count, URI_TRAFFIC_IMPUTED_RATE, lit_double(imputed_rate)))
                if obs_rate is not None and not (isinstance(obs_rate, float) and math.isnan(obs_rate)):
                    buf.append(triple(obs_count, URI_TRAFFIC_CLEAN_OBS_RATE, lit_double(obs_rate)))

                total_obs += 1

            # ---------------------------
            # OccupancyTime observation
            # ---------------------------
            if occ_time is not None and not (isinstance(occ_time, float) and math.isnan(occ_time)):
                obs_occ = u(f"{NS_EX}obsOcc_{sid_safe}_{t_idx}")

                buf.append(triple(obs_occ, URI_RDF_TYPE, URI_SOSA_OBSERVATION))
                buf.append(triple(obs_occ, URI_RDF_TYPE, URI_TRAFFIC_OBSERVATION))
                buf.append(triple(obs_occ, URI_SOSA_MADE_BY_SENSOR, sensor_uri))
                buf.append(triple(obs_occ, URI_SOSA_OBSERVED_PROPERTY, URI_TRAFFIC_OCCUPANCY_TIME))
                buf.append(triple(obs_occ, URI_SOSA_HAS_SIMPLE_RESULT, lit_double(occ_time)))
                buf.append(triple(obs_occ, URI_SOSA_PHENOMENON_TIME, t_inst))
                buf.append(triple(obs_occ, URI_SC_OBSERVED_AT_TIMEINDEX, lit_long(t_idx)))
                buf.append(triple(obs_occ, URI_SC_AGG_WINDOW_SECONDS, lit_int(WINDOW_SEC)))

                if lane_uri:
                    buf.append(triple(obs_occ, URI_SOSA_HAS_FEATURE_INTEREST, lane_uri))
                if conf_min is not None and not (isinstance(conf_min, float) and math.isnan(conf_min)):
                    buf.append(triple(obs_occ, URI_TRAFFIC_CONF_MIN, lit_double(conf_min)))
                if conf_mean is not None and not (isinstance(conf_mean, float) and math.isnan(conf_mean)):
                    buf.append(triple(obs_occ, URI_TRAFFIC_CONF_MEAN, lit_double(conf_mean)))
                if cov_o is not None and not (isinstance(cov_o, float) and math.isnan(cov_o)):
                    buf.append(triple(obs_occ, URI_TRAFFIC_COVERAGE_OCC, lit_double(cov_o)))
                if imputed_rate is not None and not (isinstance(imputed_rate, float) and math.isnan(imputed_rate)):
                    buf.append(triple(obs_occ, URI_TRAFFIC_IMPUTED_RATE, lit_double(imputed_rate)))
                if obs_rate is not None and not (isinstance(obs_rate, float) and math.isnan(obs_rate)):
                    buf.append(triple(obs_occ, URI_TRAFFIC_CLEAN_OBS_RATE, lit_double(obs_rate)))

                total_obs += 1

        fout.write("".join(buf))
        print(f"[obs] written observations so far: {total_obs}")

print("✅ RDF stream saved:", OUT_NT_GZ)
print("   total time instants :", total_time_instants)
print("   total observations  :", total_obs)